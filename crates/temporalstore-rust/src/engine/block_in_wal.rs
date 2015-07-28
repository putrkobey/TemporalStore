// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Serving a written page back out of the log record that carries it.
//!
//! # The hole this closes
//!
//! Under `async_storage` a written value is never handed to the block store. Its only durable
//! copy is its WAL record, and it is served from the memory cache at a synthetic address that
//! names no file. So when the cache drops that entry before a dump materializes it, the read
//! finds nothing: an acked write reads back as MISSING until a full reload replays the WAL.
//! [`super::hot_block_spill`] works around that by copying evicted values to a real slab, which
//! helps only if the spill happened and succeeded.
//!
//! The value was in the WAL the whole time. What was missing was a way to say *where*: the
//! synthetic address is a counter, not a position, so nothing could find the record again.
//!
//! # Staging
//!
//! A page is often derived state rather than the command's own bytes -- a serialized counter
//! series cannot be rebuilt from the command that bumped it -- so the page itself has to travel
//! with the write. As a write produces pages it puts them aside here; the append attaches
//! whatever was staged to the record it writes and reports the log id that record landed at;
//! and a read resolves the log id and takes its page straight out of the record.
//!
//! The buffer is per thread and cleared at the start of every execute, so a command that stages
//! a page and then fails to append cannot leak it into the next command's record.
//!
//! # Addressing
//!
//! A log id survives reclaim -- the record moves when the log is compacted, but the id keeps
//! naming it -- which is what makes it usable as an address at all. Registrations are keyed on
//! the object id the write derived, which the stored address already carries, so a read finds
//! its record by identity rather than by when the write happened.
//!
//! # Lifetime
//!
//! Registrations are live-path state, like the spill redirects: on reload the WAL is replayed
//! and every page is re-derived, so they are never persisted, and a shard's entries are dropped
//! when the shard unloads.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::types::ShardId;
use crate::wal::{decode_wal_line, LocalWriteAheadLogStore, StagedBlock};

thread_local! {
    /// Pages produced by the write currently executing on this thread.
    static STAGED: RefCell<Vec<StagedBlock>> = const { RefCell::new(Vec::new()) };
}

/// Start a write with nothing staged.
///
/// Called before every execute. Without it a command that staged a page and then did not append
/// -- a rejected write, a read-only command -- would leave the page for the next command to
/// attach to an unrelated record.
pub(super) fn begin_write() {
    STAGED.with(|staged| staged.borrow_mut().clear());
    OUTCOMES.with(|outcomes| outcomes.borrow_mut().clear());
}

/// Put a page aside for the record this write is about to append.
///
/// Unconditional. `TS_BLOCK_IN_WAL` used to gate it and is gone -- said the way
/// `hot_block_spill` says the same thing about `TS_HOT_PAGE_SPILL`, because a retired flag that
/// leaves no note behind gets cited later as though it still decided something. It was, in the
/// durability analysis at the top of `tests/wal_single_barrier_recovery.rs`, which is the file
/// somebody reads to decide what an ack promises.
pub(super) fn stage(object_id: u64, component: Option<&str>, bytes: &[u8]) {
    // Charged here rather than at the two call sites inside `append_value`: this copy is what
    // carrying a page in its record COSTS, and a third caller staging a page would otherwise add
    // that cost to the store while adding nothing to the count.
    //
    // The component's copy is charged to the same class for the same reason. It is a second,
    // smaller cost of carrying the page -- a hash field name is two or three bytes, a zset member
    // is `16 + 2n` characters -- and it belongs beside the page copy it travels with rather than
    // against whichever caller happened to render the name.
    crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::CarriedBlock, || {
        STAGED.with(|staged| {
            staged.borrow_mut().push(StagedBlock {
                object_id,
                component: component.map(std::sync::Arc::from),
                bytes: bytes.to_vec(),
            })
        });
    });
}

thread_local! {
    /// Index outcomes produced by the write currently executing on this thread.
    static OUTCOMES: RefCell<Vec<crate::wal::WalOutcomeItem>> =
        const { RefCell::new(Vec::new()) };
}

/// Put aside what this write DID: which object, and where its page now lives.
///
/// Staged for the same reason pages are -- the record does not exist yet, so there is nowhere
/// to put it until the append.
/// PROOF THAT AN OUTCOME HAS BEEN STAGED FOR THE RECORD.
///
/// Private field, so only this module constructs it. A recorded container whose record IS a staged
/// outcome -- a context node, which is never filed in the bucket index -- requires one of these, so
/// deleting the staging call stops the install from compiling.
#[must_use = "a staged outcome whose witness is dropped recorded something nothing will apply"]
pub(super) struct OutcomeStaged(());

impl OutcomeStaged {
    /// A STAGED OUTCOME IS A FILED RECORD, so it satisfies a recorded container's witness.
    ///
    /// A context node's record IS the staged outcome and nothing else -- it is deliberately never
    /// filed in the bucket index, because recording it as a `hash` there would have a rebuild add
    /// an entry the write never made. So this is the one conversion between the two witnesses, and
    /// it goes one way only: a filed block does NOT imply a staged outcome, because
    /// `upsert_bucket_index_block_with(.., stage: false)` exists precisely to file without staging.
    pub(super) fn into_block_filed(self) -> super::storage_bucket_internals::BlockFiled {
        super::storage_bucket_internals::BlockFiled::from_staged_outcome(self)
    }
}

/// [`stage_outcome`], returning the witness.
pub(super) fn stage_outcome_attested(item: crate::wal::WalOutcomeItem) -> OutcomeStaged {
    stage_outcome(item);
    OutcomeStaged(())
}

pub(super) fn stage_outcome(item: crate::wal::WalOutcomeItem) {
    crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::StagedOutcome, || {
        stage_outcome_inner(item)
    })
}

/// The staging itself, so the class above covers the opt-out return as well as the push.
///
/// This charge sees the outcome BUFFER. Each item's own strings are built by the caller, before it
/// gets here, and are charged to whatever class that caller is in -- for a page upsert the
/// bucket-index primitive re-enters this class around the item it builds, so that one path is
/// whole; a future caller that does not will have its item counted as its own class's cost.
fn stage_outcome_inner(item: crate::wal::WalOutcomeItem) {
    // Asked here rather than by each caller. Six of them asked it immediately before calling,
    // which is six chances to write a seventh that does not -- and the answer belongs with the
    // table that holds the result, not with the code that produced it.
    //
    // The cost of asking late is that a caller now builds the item before learning it is not
    // wanted. That lands only on the opted-out path: recording results is the default, and the
    // variable exists to write records an older build can still replay.
    if !crate::wal::wal_outcome_items_enabled() {
        return;
    }
    OUTCOMES.with(|outcomes| outcomes.borrow_mut().push(item));
}

/// How many outcomes this write has staged so far, without consuming them.
pub(super) fn staged_outcome_count() -> usize {
    OUTCOMES.with(|outcomes| outcomes.borrow().len())
}

/// Take what this write recorded doing, leaving nothing behind.
pub(super) fn take_outcomes() -> Vec<crate::wal::WalOutcomeItem> {
    OUTCOMES.with(|outcomes| std::mem::take(&mut *outcomes.borrow_mut()))
}

/// Take what this write staged, leaving nothing behind.
pub(super) fn take_staged() -> Vec<StagedBlock> {
    STAGED.with(|staged| std::mem::take(&mut *staged.borrow_mut()))
}

/// (shard, object id) -> the log holding that page, where in it, and the WAL sequence of the
/// record carrying it.
///
/// The log handle is stored per entry rather than once per process: two engines in one process
/// have separate logs, and a single shared handle would resolve one engine's addresses against
/// the other's log -- reading the wrong bytes, silently.
///
/// The sequence is what lets WAL reclaim coexist with these registrations: a registered page's
/// only durable copy is its record, so reclaim must never truncate below the lowest registered
/// sequence (see [`min_registered_sequence`]).
type Registration = (LocalWriteAheadLogStore, u64, u64);

/// Which page this table is about: the object, and which element of it.
///
/// The second term is the whole point of this type existing rather than a bare `u64`. An object id
/// resolves a RECORD, and one record carries many pages; without the element beside it the last
/// page of a key to be written owns that key's entry and every earlier one becomes unreachable.
pub(super) type BlockKey = (u64, Option<std::sync::Arc<str>>);

/// Build a key without owning the component until the map needs it.
fn block_key(object_id: u64, component: Option<&str>) -> BlockKey {
    (object_id, component.map(std::sync::Arc::from))
}

/// The same page identity, folded into ONE `u64`, for the persisted map that survives a reload.
