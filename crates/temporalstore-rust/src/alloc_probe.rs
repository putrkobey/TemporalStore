// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Counting allocator, tests only.
//!
//! Process RSS conflates three different things: memory allocated and still held, memory freed but
//! retained by the allocator, and memory that never came from the heap at all. Measured on the
//! proxy, 71% of RSS was allocator retention rather than live data -- so an RSS delta cannot say
//! whether a change reduced what a request holds, and a change that removes real allocations can
//! show up as nothing at all.
//!
//! This counts the calls and the bytes directly, which is what "this path allocates N times per
//! candidate" actually means. Counts do not move with machine load either, which matters on a box
//! that sits between load 5 and 30 for hours.
//!
//! Wired under `cfg(test)` only: a global allocator wrapper adds two atomic increments to every
//! allocation in the process, which is not something to put on a serving path.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, Ordering};

pub static ALLOC_CALLS: AtomicU64 = AtomicU64::new(0);
pub static ALLOC_BYTES: AtomicU64 = AtomicU64::new(0);
/// What the allocator actually SET ASIDE for those requests, header and rounding included.
///
/// `ALLOC_BYTES` charges `layout.size()`, which is what the caller ASKED FOR. An allocator
/// serves a request out of a chunk that is at least as large and usually larger: glibc adds a
/// header word and rounds to sixteen, with a floor of thirty-two, so a 112-byte request is a
/// 128-byte chunk and a 1-byte request is a 32-byte one.
///
/// THE DIFFERENCE IS NOT A ROUNDING DETAIL WHEN THE CHANGE UNDER TEST MOVES BYTES OUT OF LINE.
/// A field held inline in a structure costs its own width and nothing else. The same field
/// behind a pointer costs the pointer, the chunk header, and the rounding -- and the request
/// column cannot see any of the three. Comparing an inline shape against an out-of-line one on
/// `ALLOC_BYTES` therefore UNDER-CHARGES the out-of-line side, systematically and in the
/// direction that flatters it.
///
/// THIS IS READ FROM THE ALLOCATOR, NOT MODELLED. On glibc it is `malloc_usable_size` on the
/// pointer that was just handed back, plus the header word. `the_chunk_counter_reads_more_than
/// _the_request_counter_for_a_planted_odd_size` plants a size whose chunk is known and recovers
/// it, and `the_chunk_counter_agrees_with_the_documented_glibc_chunk_rule` checks the reading
/// against the rule over a sweep of sizes, so a platform whose allocator rounds differently is
/// reported rather than assumed away.
pub static ALLOC_CHUNK_BYTES: AtomicU64 = AtomicU64::new(0);
pub static FREE_CALLS: AtomicU64 = AtomicU64::new(0);
pub static FREE_BYTES: AtomicU64 = AtomicU64::new(0);

/// The header word glibc puts in front of every chunk. `malloc_usable_size` reports the space
/// the CALLER may use, which is the chunk minus this.
const CHUNK_HEADER: usize = std::mem::size_of::<usize>();

extern "C" {
    /// glibc. Reports how many bytes are usable at `ptr`, which is the chunk it came from minus
    /// the header word. Declared here rather than pulled in as a dependency: this module is
    /// test-only and one symbol does not justify a crate.
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    fn malloc_usable_size(ptr: *mut std::ffi::c_void) -> usize;
}

/// What the allocator set aside for a request that was served at `ptr`.
///
/// On glibc this is READ BACK from the allocator. Everywhere else there is no portable way to ask,
/// so the request is charged unchanged and the chunk column degrades to the request column -- which
/// under-states the out-of-line side rather than inventing a number for it. The two planted-marker
/// tests below state which of the two is running.
#[inline]
fn chunk_of(ptr: *mut u8, requested: usize) -> usize {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        if ptr.is_null() {
            return 0;
        }
        // SAFETY: `ptr` was just returned by `System.alloc`/`System.realloc`, which on this target
        // is glibc `malloc`, so it is a live pointer into a chunk this allocator owns.
        return unsafe { malloc_usable_size(ptr as *mut std::ffi::c_void) } + CHUNK_HEADER;
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    {
        let _ = ptr;
        requested
    }
}

/// The chunk glibc is documented to serve a request of `request` bytes from: a header word added,
/// rounded up to sixteen, with a floor of thirty-two.
///
/// This is the RULE, not the reading. It exists so the reading can be checked against it rather
/// than trusted, and so a target where the two disagree says so out loud.
pub fn documented_glibc_chunk(request: usize) -> usize {
    let with_header = request + CHUNK_HEADER;
    let rounded = with_header.div_ceil(16) * 16;
    rounded.max(32)
}

/// The chunk the allocator actually set aside for a boxed value, read back from the allocator.
///
/// The safe way to ask the decisive question an out-of-line shape raises: a field moved behind a
/// pointer costs the chunk, not its own width, and the two differ. Taking a live box rather than a
/// raw pointer keeps the reading safe -- the pointer is guaranteed live and to have come from this
/// allocator, which is exactly what the usable-size reading requires.
pub fn chunk_behind<T>(boxed: &Box<T>) -> usize {
    chunk_of(&**boxed as *const T as *mut u8, std::mem::size_of::<T>())
}

pub struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        ALLOC_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        charge_class_alloc(layout.size());
        let ptr = System.alloc(layout);
        ALLOC_CHUNK_BYTES.fetch_add(chunk_of(ptr, layout.size()) as u64, Ordering::Relaxed);
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        FREE_CALLS.fetch_add(1, Ordering::Relaxed);
        FREE_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        charge_class_free(layout.size());
        System.dealloc(ptr, layout)
    }

    // realloc and alloc_zeroed have default implementations in terms of alloc/dealloc, but the
    // default realloc copies; forwarding to System::realloc keeps growth cheap and still counts the
    // net change, which is what a Vec push storm actually costs.
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOC_CALLS.fetch_add(1, Ordering::Relaxed);
        if new_size > layout.size() {
            ALLOC_BYTES.fetch_add((new_size - layout.size()) as u64, Ordering::Relaxed);
        }
        // Charged the same way the whole-process counter above is, so the class rows and the span
        // total treat a grow identically and a `Vec` push storm cannot land in one and not the
        // other.
        charge_class_alloc(new_size.saturating_sub(layout.size()));
        // The chunk column charges the GROWTH in set-aside bytes, the same net-change rule the
        // request column above uses, so a `Vec` push storm lands identically in both.
        let before = chunk_of(ptr, layout.size());
        let grown = System.realloc(ptr, layout, new_size);
        let after = chunk_of(grown, new_size);
        ALLOC_CHUNK_BYTES.fetch_add(after.saturating_sub(before) as u64, Ordering::Relaxed);
        grown
    }
}

// ---------------------------------------------------------------------------------------------
// WHAT THE MEMORY WAS SPENT ON
// ---------------------------------------------------------------------------------------------
//
// One undifferentiated total answers "how much" and never "on what". At four thousand records
// "6.0 allocations per record" is a summary; at a large corpus the same number describes a store
// whose index is flat per record and one whose index is outgrowing the store, and those two want
// opposite fixes. The counters below split the same allocations by the sink they land in.
//
// THE CLASSES WERE MEASURED, NOT CHOSEN. They are the nine sinks a value write was measured to
// land in on this engine, each with a primitive that owns it; the run and the full decomposition
// are in `engine::tests::alloc_class_scale`. Candidates that did not survive the measurement are
// named there too, with the reason, so the set is a finding rather than a taxonomy.
//
// WHERE THE CHARGE SITS. Inside the primitive that owns the sink, never at its call sites. Six
// counters in this crate have been declared and then seen a fraction of their call sites or none
// of them; the two most recent corrections (#1882, #1899) both moved the charge into the callee
// for exactly this reason, and the exploratory pass behind this module hit the same wall from the
// other side -- a scope placed at the single-command call site of `execute_on_shard` read ZERO on
// a batch ingest, because the batch path is a second live caller of the same function.
//
// What placing the charge in the callee does NOT cover is a NEW primitive that allocates into a
// sink without going through the existing one. `every_alloc_class_has_a_production_scope` fails on
// a class nothing enters; nothing can fail on a sink nobody has written yet. The pull request that
// introduced this says so rather than implying the cover is total.

/// What a span of allocation was spent on, in this engine's own terms.
///
/// A PAGE is what a write stores, a SLAB is the file it lands in, a BUCKET is the set of keys a
/// routing bucket owns, and an OUTCOME is what a write puts aside for its log record -- the words
/// this tree already uses for its own parts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AllocClass {
    /// The stored value's own bytes: the block-record payload encode, and the copy the in-memory
    /// page holds on the asynchronous arm. The largest class by bytes after the log record.
    BlockBytes,
    /// The copy of those same bytes that the write-ahead record CARRIES, so an ack promises the
    /// page and not only the command that produced it. Separate from `BlockBytes` because a
    /// durability decision put it there and reclaim is what takes it away again.
    CarriedBlock,
    /// Appending that page to a slab, minus the payload itself: the record header, the framing,
    /// the address, and what the block store keeps about where it went.
    SlabAppend,
    /// The per-routing-bucket page index: the bucket node, its page entries, the object-id set,
    /// and the object/component lookup that resolves a key to its pages. This is the row a store
    /// whose index is outgrowing its data shows it in.
    BucketIndex,
    /// The dirty-object index: which keys a later storage round has to sweep, under which bucket.
    /// Index BOOKKEEPING rather than index, and it is reported apart from `BucketIndex` because a
    /// sweep that falls behind grows this one and not that one.
    DirtyObjects,
    /// The index outcome a write stages for its log record -- which object, under which kind, and
    /// where its page landed -- and the buffer those outcomes accumulate in.
    StagedOutcome,
    /// Encoding the write-ahead record itself. Few allocations and the most bytes of any class:
    /// the record is built as whole buffers, so counting calls alone reports it as nearly free.
    LogRecord,
    /// The index-log delta a write appends: the items describing the pages it changed, and the
    /// record they are written in.
    IndexLogDelta,
    /// Invalidating the serving cache's entry for a written key. The largest class by allocation
    /// COUNT and a middling one by bytes, which is the pair of facts either column alone hides.
    CacheInvalidation,
    /// ASKING THE SERVING CACHE FOR AN ANSWER: the lookup itself, inside the three production
    /// primitives that make one. The only class on the READ side, and it was added because the
    /// other nine are all charged inside WRITE primitives, so a serving read reconciled to a
    /// classified share of exactly zero -- every allocation it made landed in the residual and
    /// nothing could say what any of them were for.
    ///
    /// Charged around the cache call and nothing else. The key is built by the caller and the
    /// answer is decoded by it, which is the same boundary `CacheInvalidation` draws around
    /// `invalidate_cache_key`, so the two cache-side classes are measured the same way and can be
    /// read against each other.
    CacheRead,
    /// STAMPING PER-BUCKET LRU RECENCY for the keys a command touched, at the two sites that
    /// do it. The second class on the READ side, and it exists because that stamp was TWO of
    /// the ten allocations #1952 measured on a warm serving read -- a `Vec<String>` built and
    /// cloned into so that a routing bucket could be derived from a key the command already
    /// owned. It now visits the keys instead of collecting them, and this row is what says so:
    /// zero on a read command, and still non-zero on a write, whose keys are synthesised and
    /// have to be built.
    RecencyStamp,
}

const CLASS_COUNT: usize = 11;

impl AllocClass {
    /// Every class, in slot order. `alloc_class_slots_are_dense_and_in_order` holds that.
    pub const ALL: [AllocClass; CLASS_COUNT] = [
        AllocClass::BlockBytes,
        AllocClass::CarriedBlock,
        AllocClass::SlabAppend,
        AllocClass::BucketIndex,
        AllocClass::DirtyObjects,
        AllocClass::StagedOutcome,
        AllocClass::LogRecord,
        AllocClass::IndexLogDelta,
        AllocClass::CacheInvalidation,
        AllocClass::CacheRead,
        AllocClass::RecencyStamp,
    ];

    /// Which counter row this class owns.
    ///
    /// THIS MATCH IS THE COMPILE ERROR. A class added to the enum without a row here does not
    /// build, and neither does one without a `label`. That is the half of "declared but never
    /// incremented" a compiler can catch; the other half -- declared, given a row, and then never
    /// entered by any production code -- is caught by `every_alloc_class_has_a_production_scope`,
    /// which fails by name on a class no production file enters.
    pub(crate) const fn slot(self) -> usize {
        match self {
            AllocClass::BlockBytes => 0,
            AllocClass::CarriedBlock => 1,
            AllocClass::SlabAppend => 2,
            AllocClass::BucketIndex => 3,
            AllocClass::DirtyObjects => 4,
            AllocClass::StagedOutcome => 5,
            AllocClass::LogRecord => 6,
            AllocClass::IndexLogDelta => 7,
            AllocClass::CacheInvalidation => 8,
            AllocClass::CacheRead => 9,
            AllocClass::RecencyStamp => 10,
        }
    }

    /// The name a report prints.
    pub const fn label(self) -> &'static str {
        match self {
            AllocClass::BlockBytes => "block_bytes",
            AllocClass::CarriedBlock => "carried_block",
            AllocClass::SlabAppend => "slab_append",
            AllocClass::BucketIndex => "bucket_index",
            AllocClass::DirtyObjects => "dirty_objects",
            AllocClass::StagedOutcome => "staged_outcome",
            AllocClass::LogRecord => "log_record",
            AllocClass::IndexLogDelta => "index_log_delta",
            AllocClass::CacheInvalidation => "cache_invalidation",
            AllocClass::CacheRead => "cache_read",
            AllocClass::RecencyStamp => "recency_stamp",
        }
    }
}

// A class whose slot is past the end of the table would index out of bounds on its first use.
// Held here so the mismatch is a build failure rather than a panic in whichever test ran first.
const _: () = assert!(AllocClass::ALL.len() == CLASS_COUNT);

struct ClassSlot {
    allocs: AtomicU64,
    alloc_bytes: AtomicU64,
    frees: AtomicU64,
    free_bytes: AtomicU64,
}

impl ClassSlot {
    const fn new() -> Self {
        ClassSlot {
            allocs: AtomicU64::new(0),
            alloc_bytes: AtomicU64::new(0),
            frees: AtomicU64::new(0),
            free_bytes: AtomicU64::new(0),
        }
    }
}

static CLASS_SLOTS: [ClassSlot; CLASS_COUNT] = [
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
    ClassSlot::new(),
];

/// No class in scope.
///
/// Deliberately NOT a variant of `AllocClass`: unclassified is a residual that the reconciliation
/// measures against an independent total, never a bucket anything can be charged to on purpose.
const UNCLASSIFIED: u8 = u8::MAX;

thread_local! {
    /// The class the running code is inside, or `UNCLASSIFIED`.
    ///
    /// `const`-initialised, and a `Cell<u8>` has no destructor: the allocator reads this on every
    /// allocation, and a thread-local that allocated to initialise itself would recurse through
    /// the allocator that is reading it.
    static CURRENT_CLASS: std::cell::Cell<u8> = const { std::cell::Cell::new(UNCLASSIFIED) };
}

#[inline]
fn charge_class_alloc(size: usize) {
    let _ = CURRENT_CLASS.try_with(|cell| {
        let slot = cell.get() as usize;
        if slot < CLASS_COUNT {
            CLASS_SLOTS[slot].allocs.fetch_add(1, Ordering::Relaxed);
            CLASS_SLOTS[slot]
                .alloc_bytes
                .fetch_add(size as u64, Ordering::Relaxed);
        }
    });
}

#[inline]
fn charge_class_free(size: usize) {
    let _ = CURRENT_CLASS.try_with(|cell| {
        let slot = cell.get() as usize;
