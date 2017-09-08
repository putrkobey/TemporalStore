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
        }
    }

    /// The bytes, once the read is on the store's books.
    ///
    /// `bytes_read` is charged the PHYSICAL bytes this read pulled off disk, which is what it
    /// means beside `logical_bytes_read`.
    pub(crate) fn charge(self, stats: &mut BlockStoreStats) -> Vec<u8> {
        stats.reads = stats.reads.saturating_add(1);
        stats.bytes_read = stats.bytes_read.saturating_add(self.physical_bytes);
        self.bytes
    }

    /// The bytes, for a reader that has no store to charge.
    ///
    /// The open path and the slab inspection both read before any `BlockStore` exists, and the
    /// inspection reads on a worker thread that could not reach one anyway. Their bytes are
    /// already on the PROCESS counter -- that happened when this was built -- so this is not an
    /// uncounted escape hatch; it is the absence of a second, store-scoped charge that would have
    /// nowhere to land. Named so that it reads as a decision at each call site.
    pub(crate) fn into_bytes_charged_to_the_process_only(self) -> Vec<u8> {
        self.bytes
    }
}

/// One slab's worth of storage, addressed by the id the block store already uses.
///
/// WHICH OF THESE RUN. Only `read_range`, `read_range_at_most` and `read_all` have a production
/// caller, all three in `read.rs`, and between them they are every read the block store makes.
/// The other seven are reached only by this file's own tests. They are not dead code and not a
/// contract held open for a remote backend (see the module note above): the block store still
/// performs every one of those operations, inline, on its own handles. This trait was extracted
/// and then only `read.rs` was moved onto it.
///
/// The work left is to move the remaining call sites here, so each of the seven is a target,
/// not a leftover. Anyone doing that must carry the call site's behaviour across rather than
/// assume these bodies already match it -- today three of them do not:
///
///   * `append` opens the slab file on every call, while the batched append in `append.rs`
///     holds one handle open across many records; and it reports the offset from
///     `metadata().len()`, while both production paths track `write_offset` themselves.
///   * `sync` calls `sync_all`, while the append and slab-roll paths call `sync_data`.
///   * `remove` deletes `slab_path(root, id)`, while the production delete in the
///     delayed-destroy sweep removes an already-quarantined file by its own path.
///
/// `len`, `slab_ids` and `exists` do match what production does inline today
/// (`metadata().len()`, `slab_ids_at(root)` and `slab_path(root, id).exists()` respectively).
pub(crate) trait SlabBackend: Send + Sync {
    /// Append bytes to a slab, answering the offset they landed at.
    ///
    /// The offset is the backend's to report rather than the caller's to assume: an object
    /// backend appends into an object whose length it knows, and a caller tracking its own
    /// offset would be guessing at it.
    fn append(&self, slab_id: u64, bytes: &[u8]) -> io::Result<u64>;

    /// Read one record's bytes back, given where the address says they are.
    ///
    /// EXACT: a slab too short for the range is an error, because an address that points past
    /// the end of its own slab is a broken address and not a short answer.
    fn read_range<'a>(
        &self,
        slab_id: u64,
        offset: u64,
        length: u64,
        tally: &'a mut BlockStoreReadTally,
    ) -> io::Result<CountedSlabRead<'a>>;

    /// The same range, TOLERATING a short slab: what is there is returned, and nothing is an
    /// error.
    ///
    /// The block store's streaming and slab-report reads have always behaved this way, and the
    /// difference is not cosmetic -- routing them through `read_range` above would turn a
    /// truncated tail from an empty answer into a failed read. Named separately so the choice is
    /// made by whoever knows which one they want.
    fn read_range_at_most<'a>(
        &self,
        slab_id: u64,
        offset: u64,
        length: u64,
        tally: &'a mut BlockStoreReadTally,
    ) -> io::Result<CountedSlabRead<'a>>;

    /// The whole slab, for the walks that summarise or inspect one.
    fn read_all<'a>(
        &self,
        slab_id: u64,
        tally: &'a mut BlockStoreReadTally,
    ) -> io::Result<CountedSlabRead<'a>>;

    /// How long the slab is, without reading it.
    fn len(&self, slab_id: u64) -> io::Result<u64>;

    /// Cut a slab back to a length.
    ///
    /// NOT the torn-tail fence, despite what this said before. That fence is in
    /// `BlockStore::open` and calls `set_len` on a handle it opens itself, because it must also
    /// `sync_all` the slab, `sync_all` the parent directory and record a durability barrier --
    /// none of which happens here. Pointing that fence at this method as it stands would
    /// quietly drop two fsyncs from a crash-recovery path, so whoever routes it through here
    /// has to bring the durability with it.
    fn truncate(&self, slab_id: u64, length: u64) -> io::Result<()>;

    /// Forget a slab entirely.
    fn remove(&self, slab_id: u64) -> io::Result<()>;

    /// Which slabs exist.
    fn slab_ids(&self) -> io::Result<Vec<u64>>;

    /// Make everything written to a slab durable.
    fn sync(&self, slab_id: u64) -> io::Result<()>;

    /// Whether a slab exists at all.
    fn exists(&self, slab_id: u64) -> bool;
}

/// Slabs as files in a directory, which is what this has always done.
///
/// Every method is the code the block store ran inline before, moved behind the name of the
/// operation it performs.
pub(crate) struct LocalSlabBackend<'a> {
    root: &'a Path,
}

impl<'a> LocalSlabBackend<'a> {
    /// Borrows the root rather than owning it, so a caller on a read path can make one without
    /// allocating: the block store already holds the root it would have cloned.
    pub(crate) fn new(root: &'a Path) -> Self {
        Self { root }
    }

    pub(crate) fn root(&self) -> &Path {
        self.root
    }

    fn path(&self, slab_id: u64) -> PathBuf {
        super::slab_path(self.root, slab_id)
    }
}

impl SlabBackend for LocalSlabBackend<'_> {
    fn append(&self, slab_id: u64, bytes: &[u8]) -> io::Result<u64> {
        use std::io::Write as _;
        std::fs::create_dir_all(self.root)?;
        let path = self.path(slab_id);
        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        // The offset is read from the file rather than tracked by the caller, so a slab that
        // grew underneath us is not silently written over.
        let offset = file.metadata()?.len();
        file.write_all(bytes)?;
        file.flush()?;
        Ok(offset)
    }

    fn read_range<'a>(
        &self,
        slab_id: u64,
        offset: u64,
        length: u64,
        tally: &'a mut BlockStoreReadTally,
    ) -> io::Result<CountedSlabRead<'a>> {
        use std::io::{Read as _, Seek as _, SeekFrom};
        let mut file = std::fs::File::open(self.path(slab_id))?;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0_u8; length as usize];
        file.read_exact(&mut bytes)?;
        Ok(CountedSlabRead::of(bytes, tally))
