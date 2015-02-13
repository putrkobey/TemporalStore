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

