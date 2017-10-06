// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Where slab bytes actually live.
//!
//! The block store is the one log in this tree that writes straight to files. The write-ahead
//! log, the index log and the raft log all go through `log_framing`, and the replicator goes
//! through the `ObjectStore` backends -- so the two halves of a storage layer exist here, and
//! the block store uses neither.
//!
//! This names the operations the block store performs on a slab, once, instead of spelling
//! each one inline at its call site. It is deliberately NOT the object-store trait: that one is
//! key-and-bytes with ranges, while a slab is appended to and read back by offset, and mapping
//! one onto the other at every call site is what this exists to avoid.
//!
//! WHAT THIS TRAIT IS NOT. It does not let a slab live on an object backend without the block
//! store knowing which. `LocalSlabBackend` is its only implementation, nothing anywhere takes a
//! `dyn SlabBackend` or is generic over `B: SlabBackend`, and both production call sites name
//! the concrete type. A slab that lives remotely is handled by `SharedSlabSource` instead: a
//! separate one-method trait, held as `Arc<dyn SharedSlabSource>`, with three implementations in
//! `shared_store.rs`. Each fetches a whole slab and hands it to `BlockStore::install_slab`,
//! which writes it to local disk -- so from the first read onward every operation on that slab
//! is a local file operation. That is why a remote implementation of THIS trait has never been
//! needed, and why adding methods here for the sake of one would be building for a caller that
//! does not exist.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use super::BlockStoreStats;

// ---------------------------------------------------------------------------------------------
// WHAT THIS STORE READS OFF DISK, COUNTED WHERE THE READING HAPPENS.
//
// `BlockStoreStats::bytes_read` counts the PHYSICAL bytes of a record read back through one of
// the store's four read entry points. That is a useful number and it is not this one. The store
// also reads whole slab FILES on paths with no record read in them at all -- above all the slab
// inspection every open runs -- and those fed no counter whatsoever.
//
// MEASURED, before this existed. A cold open of a store built by appending, 20 records of 128
// bytes to a slab, `TS_REVERIFY_ALL_SLABS` unset:
//
//     slabs                                        50            200     ratio
//     slab bytes on disk (stat)               140,000        560,000     4.000
//     slab manifest on disk (stat)             12,760         51,360     4.025
//     what the STORE said it read                   0              0        --
//     what the KERNEL charged this process    153,087        611,711     3.996
//
// A READER WITH NO COUNTER CONTRIBUTES ZERO TO EVERY REPORT, WHICH IS INDISTINGUISHABLE FROM A
// READER DOING NO WORK. This is the seventh counter in this campaign found blind.
//
// PROCESS-WIDE, AND THERE IS DELIBERATELY NO THREAD-LOCAL BESIDE IT. `wal.rs` and `index_log.rs`
// both offer a `..._on_this_thread()` counter because a restore runs on one thread while the
// suite does not. THAT INSTRUMENT CANNOT MEASURE THIS READER: the slab inspection reads and
// hashes on threads it SPAWNS, so the bytes are charged to threads that did not exist before the
// open and are gone after it. Measured across the same two cold opens above,
// `/proc/thread-self/io` on the CALLING thread saw 12,871 and 51,478 bytes -- the manifest and
// nothing else, 8.4% of what the process was charged, both times. A thread-local counter here
// would report that same near-zero and would look perfectly reasonable. The tally publishes into
// these atomics from whichever thread did the reading, which is what puts a worker's bytes in the
// total at all.
// ---------------------------------------------------------------------------------------------

static BLOCK_STORE_FILE_BYTES_READ: AtomicU64 = AtomicU64::new(0);
static BLOCK_STORE_FILE_READS: AtomicU64 = AtomicU64::new(0);

/// Bytes and read calls this PROCESS has made against block-store files since it started.
///
/// Slab files and the slab manifest both: every byte the block store pulls off its own disk goes
/// through [`read_block_store_file`], and nothing else in these modules opens one of its files to
/// read it.
///
/// Process-global and shared by every store in the process, so a caller measuring one span must
/// take a DELTA across it, never an absolute.
pub fn block_store_file_read_counts() -> (u64, u64) {
    (
        BLOCK_STORE_FILE_BYTES_READ.load(Ordering::Relaxed),
        BLOCK_STORE_FILE_READS.load(Ordering::Relaxed),
    )
}

/// What one span of reading against block-store files cost, published when it goes out of scope.
///
/// Borrowed mutably by the [`CountedSlabRead`] it is passed to, so the read cannot outlive it and
/// A NEW READ OF A BLOCK-STORE FILE CANNOT BE WRITTEN WITHOUT A TALLY TO HAND OVER.
pub(crate) struct BlockStoreReadTally {
    bytes: u64,
    reads: u64,
}

impl BlockStoreReadTally {
    pub(crate) fn start() -> Self {
        Self { bytes: 0, reads: 0 }
    }

    /// What this tally has taken so far, for a caller that wants its own span's figure rather
    /// than the process total.
    ///
    /// THE OPEN PATH NEEDS THIS AND THE PROCESS COUNTER CANNOT GIVE IT. `block_store_file_read_counts`
    /// is shared by every store in the process, so a suite running stores in parallel reads one
    /// store's open as another's. A store's own open figure is taken from the tallies that open
    /// used, which no other thread can touch.
    pub(crate) fn taken(&self) -> (u64, u64) {
        (self.bytes, self.reads)
    }
}

impl Drop for BlockStoreReadTally {
    fn drop(&mut self) {
        if self.reads == 0 {
            return;
        }
        BLOCK_STORE_FILE_BYTES_READ.fetch_add(self.bytes, Ordering::Relaxed);
        BLOCK_STORE_FILE_READS.fetch_add(self.reads, Ordering::Relaxed);
    }
}

/// Read one whole block-store file, and charge every byte of it to `tally`.
///
/// THE ONE DOOR. `std::fs`'s reading primitives are not in the namespace of the modules that read
/// this store's files any more, so a read written the old way does not compile. Slab files and
/// the slab manifest both come through here; they are the only two kinds of file this store
/// reads.
pub(crate) fn read_block_store_file<'a>(
    path: &Path,
    tally: &'a mut BlockStoreReadTally,
) -> io::Result<CountedSlabRead<'a>> {
    let bytes = std::fs::read(path)?;
    Ok(CountedSlabRead::of(bytes, tally))
}

/// Bytes read off a block-store file, and the charge for having read them.
///
/// The bytes are PRIVATE and [`CountedSlabRead::charge`] is the only way to them, so a read path
/// that forgets to charge the STORE does not compile; and building one at all requires a
/// [`BlockStoreReadTally`], so a read path that is invisible to the PROCESS counter does not
/// compile either. That is not hypothetical tidiness in either direction: of the block store's
/// four read entry points, `read_slab` charged nothing at all -- it read whole slabs off disk for
/// the dump manifest, the cluster snapshot and two shared-storage paths, and
/// `BlockStoreStats::reads` and `bytes_read` never moved. Those fields are `pub` on a crate other
/// people build on, so the number was wrong for them too, not only for our own guards. And every
/// open of a populated store read every slab in it without touching either counter.
///
/// Charging the STORE is deliberately a SEPARATE step from reading rather than something the
/// backend does itself: `read_slab` clones the root and reads outside the store lock on purpose,
/// and a backend that took `&mut BlockStoreStats` would have forced that read back under the
/// lock. It is also a step some readers have no way to take -- the open path has no store to
/// charge yet, and the slab inspection runs on a worker thread that holds none -- which is what
/// the tally is for: it takes the reading of every one of them.
#[must_use = "a slab read has to be charged to the store's stats"]
pub(crate) struct CountedSlabRead<'a> {
    bytes: Vec<u8>,
    physical_bytes: u64,
    /// Held only so a read cannot be constructed without one; the bytes are charged to it on the
    /// way in, not on the way out.
    _tally: &'a mut BlockStoreReadTally,
}

impl<'a> CountedSlabRead<'a> {
    fn of(bytes: Vec<u8>, tally: &'a mut BlockStoreReadTally) -> Self {
        let physical_bytes = bytes.len() as u64;
        tally.bytes = tally.bytes.saturating_add(physical_bytes);
        tally.reads = tally.reads.saturating_add(1);
        Self {
            bytes,
            physical_bytes,
            _tally: tally,
