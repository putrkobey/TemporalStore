// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::num::NonZeroU64;

use serde::{Deserialize, Serialize};

use crate::block_store::ElementEntry;
use crate::types::{CommandResponse, ControlStateSelectionType, FeaturePoint, ShardId};

use super::control_rollup::RollupEntry;
use super::hll::Hll;
use super::hash_field_map::HashFieldMap;

/// WHICH FLAVOUR OF DIRTINESS A MARK IS, as a field rather than as a key prefix.
///
/// One map holds both, and it used to tell them apart by testing whether the key began
/// `ctx:embdirty:`. That worked and was documented as load-bearing, which is the tell: a
/// boolean spelled as a string prefix has to be parsed back out, and the parse is what every
/// reader then depends on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) enum DirtyFlavour {
    /// A node whose SUMMARY is stale.
    Summary,
    /// A node whose EMBEDDING is stale.
    Embedding,
}

/// THE KEY OF ONE DIRTY MARK: the two integers it is about, and which flavour it is.
///
/// This replaced `String`. The string was `ctx:dirty:{tenant}:{node}` or
/// `ctx:embdirty:{tenant}:{node}` -- a heap allocation per mark spelling two integers that
/// `ContextDirtyEntry` already carries as fields, plus a prefix standing in for the flavour.
/// Measured at 400 context events: 800 keys holding 14,000 characters, 52% of every key
/// character in the context family, in the one map of that family that could not share an
/// allocation with anything because it was the only String-keyed one.
///
/// ORDERED, because a bounded drain sorts its candidates for a stable pass across calls. The
/// derived order is (tenant, node, flavour), which is not the lexicographic order of the old
/// keys -- a drain's ORDER is an implementation detail it only needs to be STABLE in, and the
/// test over it asserts stability rather than a particular sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) struct DirtyMark {
    pub(super) tenant_hash: u64,
    pub(super) node_hash: u64,
    pub(super) flavour: DirtyFlavour,
}

impl DirtyMark {
    pub(super) fn summary(tenant_hash: u64, node_hash: u64) -> Self {
        Self { tenant_hash, node_hash, flavour: DirtyFlavour::Summary }
    }

    pub(super) fn embedding(tenant_hash: u64, node_hash: u64) -> Self {
        Self { tenant_hash, node_hash, flavour: DirtyFlavour::Embedding }
    }
}

/// Twenty-four bytes and no heap at all, pinned as an equality so a field added without
/// thought shows up here rather than in a footprint report.
const _: () = assert!(std::mem::size_of::<DirtyMark>() == 24);

/// In-memory, coalesced summary-dirty entry.
///
/// One entry per `DirtyMark` -- a tenant, a node and a flavour. Repeated
/// `ContextMarkSummaryDirty` commands for the same node update this single entry
/// in place instead of appending a new persisted marker, so the number of dirty
/// records is bounded by the number of distinct dirty nodes rather than the number
/// of events. `propagate_depth` keeps the deepest parent-propagation requested and
/// `event_time_ms` bounds track the earliest/latest events that made the node dirty.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct ContextDirtyEntry {
    // `tenant_hash` is populated for embedding-dirty entries so the all-pending
    // drain scan (which spans every tenant on the shard) can recover each node's
    // tenant. It is left 0 for summary-dirty entries, whose queries are always
    // per-node and already carry the tenant.
    pub(super) tenant_hash: u64,
    pub(super) node_hash: u64,
    pub(super) first_event_time_ms: u64,
    pub(super) last_event_time_ms: u64,
    pub(super) reason: u32,
    pub(super) propagate_depth: u32,
    pub(super) mark_count: u64,
}

/// Where a WAL-resident block's bytes are: the log id of the record carrying it, and that
/// record's sequence (which is what log reclaim reasons about).
#[derive(Debug, Default, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct WalResidentBlock {
    pub(super) log_id: u64,
    pub(super) sequence: u64,
}

/// Two log coordinates. One per block whose only durable copy is a log record.
const _: () = assert!(std::mem::size_of::<WalResidentBlock>() == 16);

/// The key type the three bare-address resident model maps are keyed by.
///
/// `Box<str>` rather than `String`. A model-map key is written once when the key is first seen and
/// is never appended to, so the capacity word a `String` carries is eight bytes per entry that
/// nothing can ever use -- and the table holds a slot for every bucket, not for every live key, so
/// those eight bytes are charged at the table's occupancy rather than at the key count. Measured on
/// the counting allocator at the shipped routing range: 1.434 slots a live key at 40,000 keys, so
/// 13.1 B/key off each map's chunk column (112.4 -> 99.2 B/key on the string map).
///
/// The three are `strings`, `control_state_blocks` and `context_nodes` -- every resident map
/// whose value is a bare [`BlockAddress`] rather than a nested container. `address_footprint`
/// iterates the three together in one array, so they have to agree on their key type; that
/// array is the reason the class is these three and not just the one that was measured.
///
/// THE CLASS IS A KEY-TYPE CLASS AND NOT A VALUE-TYPE ONE, which matters now that the values have
/// started to diverge: `control_state_blocks` holds an `ObjectRecord` rather than a bare address.
/// The three still agree on `ModelKey`, which is what the shared array needs. And they are not
/// three equal candidates for that record -- `context_nodes` has NO production writer (its only
/// `insert` is `#[cfg(test)]`) and is reached in production only through an `.or_else()` after
/// `hashes` is consulted first, so it is a decode path for blocks written before that move. See
/// `block_store`'s note on `CONTEXT_NODE_FIELD`. Converting it would change the stored shape of
/// exactly the population it exists to read back, for no live value.
///
/// EVERY READ SHAPE IS UNCHANGED, which is why this is a key-type change and not a surface change.
/// `Box<str>: Borrow<str>`, so `get`, `get_mut`, `contains_key` and `remove` still take `&str` and
/// every call site that passes one compiles untouched. `keys()` yields `&Box<str>`, which derefs to
/// `&str`. Only the sites that CREATE a key change, and they change from `.clone()` to `.into()`.
///
/// NO STORED BYTES MOVE. The shard index is `serde_json`, where a map key is a string either way;
/// `the_string_map_key_is_the_same_json_either_way` round-trips a snapshot to prove it rather than
/// asserting it, so this carries no index format stamp.
pub(super) type ModelKey = Arc<str>;

/// Move one map's key onto the canonical allocation, if it is holding its own equal copy.
///
/// Factored out of `adopt_context_event_key` rather than copied per map: the body is identical for
/// every `ModelKey`-keyed per-kind map, and four hand-written copies of a pointer comparison is
/// four places for one of them to drift. The decision is taken before the move so the shared borrow
/// ends before the mutable one.
/// THE SAME MOVE ON AN ORDERED MAP, and a sibling rather than a generic bound.
///
/// Two of the three `u64_ledgers` domains are `BTreeMap`-keyed now -- the container changed so that
/// `expires_at_ms` could join without its iteration order becoming hash-seeded. `adopt_key_in`
/// takes a `HashMap`, and abstracting the two behind a "map-like" trait would be a new abstraction
/// for two callers. Four lines twice is the cheaper honesty.
fn adopt_key_in_ordered<V>(
    map: &mut BTreeMap<ModelKey, V>,
    object_key: &str,
    canonical: &ModelKey,
) -> bool {
    let moves = match map.get_key_value(object_key) {
        Some((held, _)) => !Arc::ptr_eq(held, canonical),
        None => false,
    };
    if !moves {
        return false;
    }
    match map.remove(object_key) {
        Some(value) => {
            map.insert(Arc::clone(canonical), value);
            true
        }
        None => false,
    }
}

fn adopt_key_in<V>(
    map: &mut HashMap<ModelKey, V>,
    object_key: &str,
    canonical: &ModelKey,
) -> bool {
    let moves = match map.get_key_value(object_key) {
        Some((held, _)) => !Arc::ptr_eq(held, canonical),
        None => false,
    };
    if !moves {
        return false;
    }
    match map.remove(object_key) {
        Some(value) => {
            map.insert(Arc::clone(canonical), value);
            true
        }
        None => false,
    }
}

/// Why a context-event key adoption did or did not move the pair. Counted rather than reasoned
/// about: a mechanism that is wired and still does nothing looks exactly like one that is not
/// wired, and only the split between "no handle to take" and "took one" tells them apart.
pub(super) static CONTEXT_EVENT_ADOPT_CALLS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(super) static CONTEXT_EVENT_ADOPT_NO_HANDLE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
pub(super) static CONTEXT_EVENT_ADOPT_MOVED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// The eight bytes, as a guard rather than as a sentence in the comment above: a `String` put back
/// here would be a silent per-live-key regression, and nothing else in the suite would notice.
const _: () = assert!(std::mem::size_of::<ModelKey>() == 16);
const _: () = assert!(std::mem::size_of::<ModelKey>() + 8 == std::mem::size_of::<String>());

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub(super) struct ShardState {
    /// On-disk shape of this index. 0 means "written before the stamp existed".
    ///
    /// The index is a serialized ShardState, so a change to what a field MEANS -- not just its
    /// type -- makes an older file decode into the right shape with the wrong contents, silently.
    /// That is exactly what keying context_events by event id did: a pre-rekey index decodes with
    /// timeline keys sitting in the event-id slot and an empty context_event_timeline, so every
    /// time-windowed read returns nothing and no error is raised anywhere.
    ///
    /// Stamped on write by serialize_index; checked on load by load_index_inner, which refuses a
    /// stale index rather than trusting it -- a refusal falls back to WAL replay, which rebuilds
    /// both maps correctly through insert_context_event_views.
    #[serde(default)]
    pub(super) index_format_version: u32,
    /// Blocks whose only durable copy is a WAL record, and which record holds each one.
    ///
    /// The resolver's table is process-local, so after a restart it is empty: the served index
    /// still points at a synthetic address and nothing can turn it back into bytes until a full
    /// replay re-derives the block. Recording the log id HERE means the mapping travels with the
    /// index that depends on it, and a reload hands it straight back.
    ///
    /// A stale entry costs a miss, never wrong bytes. The resolver reads the record at that log
    /// id and looks for the object inside it, so a reclaimed record or a superseded block simply
    /// is not found, and the read falls through exactly as it did before this existed.
    #[serde(default)]
    pub(super) wal_resident_blocks: BTreeMap<u64, WalResidentBlock>,
    /// Routing buckets whose derived runtime flags may be stale, so a write can refresh the
    /// buckets it touched instead of sweeping the shard.
    ///
    /// `refresh_bucket_runtime_flags` recomputes `in_memory` / `deleted` / `dirty` / `ttl_ms` /
    /// `layout` for EVERY bucket, and the single-write path called it on every write. Each bucket
    /// costs `O(its pages)`, so the sweep is `O(total pages)` per write and ingestion is quadratic
    /// in the corpus. Note that the routing-slot range does not soften this: fewer slots means
    /// fewer, larger buckets and the same total block count.
    ///
    /// Recorded where the routing bucket is already known -- the bucket-index upsert, the removal
    /// paths, and the async dirty mark -- rather than inferred from the key, because a stored
    /// address may carry an explicit routing bucket that disagrees with `block_routing_bucket`.
    ///
    /// Not persisted: on load this is empty, and every load and recovery path already runs the
    /// full sweep, so a fresh process starts from fully recomputed flags.
    #[serde(skip)]
    pub(super) buckets_pending_flag_refresh: BTreeSet<u32>,
    /// Uncompressed bytes of the live objects that carry a size class.
    ///
    /// Maintained where the class is installed and where the row is dropped, so the total and the
    /// classes cannot disagree about what left. EPHEMERAL: `serde(skip)`, so after a load it is 0
    /// until writes repopulate it, and the covered count below is what makes that visible rather
    /// than letting a small total read as a small live set. Same bargain the stale-range record
    /// makes, and stated for the same reason.
    #[serde(skip)]
    pub(super) live_user_bytes: u64,
    /// How many live objects contributed to `live_user_bytes`.
    ///
    /// The denominator for coverage. A ratio over `live_user_bytes` is only as complete as this
    /// count is against the live object count, and publishing it is what stops a partial total
    /// being read as a whole one.
    #[serde(skip)]
    pub(super) live_objects_with_logical_size: u64,
    // THE EXPIRY LEDGER IS A DOMAIN OF `u64_ledgers` NOW, behind `expires_at_ms()` and
    // `expires_at_ms_mut()`. It is a `u64` per object key, which is what the other two
    // domains are, so the merge adds a domain key and no union. The deadline-ORDERED
    // mirror below keeps its own field: a different key shape answering a different
    // question.
    /// The same deadlines, DEADLINE-ordered, so the keys that are due are a PREFIX.
    ///
    /// Walking `expires_at_ms` in key order to find due keys made time-to-expire a function of
    /// the keyspace rather than of how many keys were actually due: measured at
    /// keyspace/scan_budget rounds, so ten expired keys behind 10,000 live ones survived more
    /// than sixty rounds. Ordered by deadline, finding them costs the number that are due.
    ///
    /// The same pair as `SeenSet`'s `by_member`/`by_time`, for the same reason.
    ///
    /// Derived, never persisted: it is rebuilt from `expires_at_ms` on first use after a load,
    /// so no snapshot or wire format changes and an older snapshot needs no migration. The repair
    /// (`ensure_expiry_order`) fires ONLY on an entirely empty map -- a mirror left populated and
    /// wrong is never repaired, and shows up only as keys that silently never expire.
    ///
    /// The `engine::tests::expiry_scale` module holds the guards: that a round finds what is due
    /// at a hundred-thousand-key shard, that this view is rebuilt on load, manifest install and
    /// WAL replay, and -- at length -- why a bounded round-robin expiry cursor should not replace
    /// an ordered index here.
    #[serde(skip)]
    /// THE TUPLE'S SECOND ELEMENT IS A SHARED HANDLE, NOT A SECOND COPY. See
    /// `expires_at_ms` above: the ordering here is the whole point of this map and is untouched,
    /// but the characters it orders are the ones that map already holds.
    pub(super) expiry_by_deadline: BTreeMap<(u64, Arc<str>), ()>,
    // ONE FIELD FOR BOTH ADDRESS MAPS NOW, keyed by `AddressMap`, behind `strings()` /
    // `_mut()` and `context_nodes()` / `_mut()`. `default`, because an index written before
    // this merge carries `strings` and no `address_maps` and must still DECODE -- the stamp
    // is what refuses it, and a decode error would hide that, which is the defect
    // `an_ungated_store_is_refused_before_it_is_served` caught when `counter_series` shipped
    // without one.
    // ONE FIELD FOR EVERY DOMAIN-KEYED SIDE TABLE, keyed by `SideTable`, behind the same four
    // doors as before. THE VARIANT WRAPS THE TABLE, NOT THE ENTRY, so no key pays a byte: the tag
    // is paid once per domain, four times in total, and every inner map keeps its exact type.
    // That is what makes this merge free where a union of the two VALUE types would have cost
    // 16.4 B/key at best and 144.2 at worst -- see
    // `index_bytes_per_key::what_merging_the_key_sharing_maps_would_cost_as_a_record`.
    //
    // `with`, not `default`: each stored spelling has exactly ONE table type, so the decode reads
    // the key first and deserializes the value as that key's type. No variant tag reaches the
    // wire, and the stored image is the same four spellings it always was -- under one object
    // instead of split across two.
    #[serde(default, with = "side_table_serde")]
    pub(super) side_tables: HashMap<SideTable, SideTableData>,
    // DURABLE, AND THIS LINE SAID IT WAS NOT. It read "Rebuildable from the durable bucket/block
    // index on load; do not duplicate in checkpoints", which was true while this field was
    // `skip_serializing` and the element name on a page-index entry was the durable spelling of a
    // hash field. The entry has no element-name field now, so nothing could rebuild this map from
    // the index: `hashes` carries `#[serde(default)]` below, as `sets`, `zsets` and `lists` do, and
    // the four resident maps ARE the authority rather than a derived view of one.
    //
    // THE INNER CONTAINER IS A SORTED VECTOR, NOT A TABLE AND NOT A B-TREE, and it is the only one
    // of the eighteen nested model maps that is either. Measured on the counting allocator, a hash
    // of ONE field -- which is every context node in a store, because `write_context_node` files
    // its block under the single constant `CONTEXT_NODE_FIELD` -- cost 272 chunk bytes as a table
    // and 560 as a `BTreeMap`, against 64 as an exact-sized vector. Matching the seventeen ordered
    // siblings was measured and LOST, by more than 2x at the occupancy that is 100% of the product
    // write path. See `engine::hash_field_map` for the trade and
    // `engine::tests::model_map_container_cost` for the numbers.
    //
    // The wire shape is unchanged: `HashFieldMap` serializes to and from the same MAP the
    // `HashMap` did, so an index written before this field became `skip_serializing` still decodes.
    //
    // AND THE WRAPPER AROUND IT MOVES NO BYTES EITHER. `RecordedHashContainer` is
    // `#[serde(transparent)]` over exactly one field, `HashMap<String, HashFieldMap>` -- the type
    // this field had -- so the derived impls forward verbatim and no snapshot, manifest or
    // index-log encoding changes by a byte. `SHARD_INDEX_FORMAT_VERSION` does not move for it, and
    // `engine::tests::recorded_hash_container_invariant` holds the two encodings equal against the
    // bare map with a control that detects a planted difference.
    //
    // WHAT THE WRAPPER IS FOR IS NOT FOOTPRINT. It owns the map together with the durable record of
    // its mutations: the inner map is private to the container's module, so the only way to change
    // membership is to pass a value that could not have been constructed without the record already
    // being emitted. A writer that mutates this map without recording does not compile. The five
    // paths that legitimately mutate WITHOUT a record -- the reconcile, the delta fold, two replay
    // arms and the compactor's address rewrite -- are named methods there, each with its reason,
    // and that set is closed. See `engine::recorded_hash_container`.
    #[serde(default)]
    pub(super) hashes: super::recorded_map::RecordedMap<super::recorded_map::HashKind>,
    // THE WIRE IS UNCHANGED AND THE `with =` MOVED RATHER THAN WENT. `RecordedSetContainer` owns
    // this map together with the durable record of its mutations -- the inner map is private, so the
    // only way to change membership is to pass a value that could not exist unless the record was
    // already emitted, and a writer that mutates without recording does not compile. Its
    // `Serialize`/`Deserialize` impls CALL `set_index_serde`, the same functions this field named,
    // so no snapshot, manifest or index-log encoding moves a byte and
    // `SHARD_INDEX_FORMAT_VERSION` does not change for it.
    //
    // The four paths that legitimately mutate WITHOUT a record -- the reconcile, the delta fold,
    // the replay removal and the compactor's address rewrite -- are named methods there, each with
    // its reason, and that set is closed. FOUR and not hash's five: recovery's `set` arm re-files
    // its block, so unlike hash's `context_node` arm it needs no replay-install exception.
    #[serde(default)]
    pub(super) sets: super::recorded_map::RecordedMap<super::recorded_map::SetKind>,
    /// Windowed seen-sets backing idempotency keys: member -> when it was last seen, plus
    /// the same entries time-ordered so expiry pops from the front in bounded steps. Like the
    /// buckets, no blocks back this state -- it persists with the shard index snapshot, and a
    /// crash forgetting a window's worth of members re-admits a duplicate rather than
    /// dropping a legitimate first ingest.
    #[serde(default, with = "super::seen_index_serde")]
    pub(super) seen: HashMap<ModelKey, SeenSet>,
    /// Token buckets: key -> (tokens remaining, last refill ms). Config rides each command,
    /// never the store -- the caller owns policy, which is exactly what a quota layer wants.
    /// No blocks back this state: it persists only with the shard index snapshot, so a crash
    /// refills every bucket to capacity. That direction is deliberate and documented -- a
    /// limiter that briefly over-admits after a crash beats one that starves recovered
    /// tenants on stale counts.
    #[serde(default)]
    pub(super) buckets: HashMap<ModelKey, (f64, u64)>,
    /// Sorted sets: member -> (total-order score bits, element block). The score-ordered view
    /// is derived per query -- V1 accepts the per-range sort; the upgrade path is a second
    /// in-memory map rebuilt at load, never a second persisted structure (the index component
    /// already encodes score-then-member, so recovery has the order for free).
    // THE WIRE IS UNCHANGED AND THE `with =` MOVED RATHER THAN WENT: `ZSetKind` names
    // `zset_index_serde` as its codec, which is a REQUIRED associated function with no default --
    // so this kind cannot inherit the wrong encoding, which is the defect a blanket impl produced
    // for `sets` and which three reload tests caught.
    #[serde(default)]
    pub(super) zsets: super::recorded_map::RecordedMap<super::recorded_map::ZSetKind>,
    /// Redis-style lists: element blocks keyed by a signed sequence -- left pushes walk the
    /// low end down, right pushes walk the high end up, so both ends are O(log n) and the
    /// BTree's order IS the list's order.
    // A PLAIN MAP, STATED RATHER THAN INHERITED. An `i64` key has a string form a JSON object can
    // carry, so this kind never needed the codec `sets` and `zsets` do -- and `ListKind` says so
    // explicitly instead of defaulting to it.
    #[serde(default)]
    pub(super) lists: super::recorded_map::RecordedMap<super::recorded_map::ListKind>,
    // THE COUNTER SERIES ARE DOMAINS OF `side_tables` NOW, behind `control_state()` /
    // `_mut()` and `feature_values()` / `_mut()` exactly as before. Their stored spellings
    // -- "control_state" and "feature_values" -- are unchanged; only the object they sit
    // under is.
    /// Counter-series blocks, one per control-state object key.
    ///
    /// THE VALUE IS A RECORD RATHER THAN A BARE ADDRESS, and this is the first map to hold one.
    /// A live object was costing several key-addressed structures, each paying its own node AND
    /// its own copy of the key's characters; the record is the single row those collapse into. It
    /// carries the address this map always held, plus a deadline slot, state bits and a model id
    /// that are PRESENT AND UNUSED here -- so the width goes live with no behaviour change, and
    /// the deadline merge that fills them is a separate change with its own evidence.
    ///
    /// READ IT THROUGH `address()`, WRITE IT THROUGH `set_address()`. The second matters: writing
    /// back by building a fresh record drops the unused slots silently, and would not fail while
    /// they are unused. See `ObjectRecord::set_address`.
    ///
    /// A STORED-FORMAT CHANGE, which is why the format stamp moves with it. An index written
    /// before this carries a bare address here and is refused by the stamp check rather than
    /// decoded into the wrong shape; the refusal falls back to replay, and the reconcile rebuilds
    /// this map from the bucket index.
    /// THE CONSOLIDATED MAP: one durable structure, keyed kind then object key.
    ///
    /// Two levels and not a flat `(kind, key)` map, because a key may hold DIFFERENT KINDS AT
    /// ONCE. That is measured, not assumed: a string and a hash under the same key both answer
    /// ok and both read back their own bytes, in either write order. A flat map keyed by the
    /// object key alone would collide them, and the collision is reachable by any client through
    /// the public surface. Two levels also keep a per-kind walk at exactly today's cost -- one
    /// lookup, then iterate the kind -- where a flat map would make every such walk scan every
    /// kind.
    ///
    /// A `BTreeMap` OUTSIDE, FOR ITS ORDER AND NOT ITS SPEED. Seventeen entries at most, so the
    /// container's lookup cost is irrelevant and the temptation is to reach for a `HashMap`. The
    /// order is the point: the dump manifest feeds its digest from an iteration key by key and
    /// recomputes it on restore, so an unordered outer map makes that digest depend on hash
    /// iteration order and silently invalidates every manifest written before the change. See the
    /// test that pins it, which is the first thing in the tree asserting that digest is
    /// order-sensitive at all.
    ///
    /// ONE RECORD PER KEY, AND THE COMPILER IS WHAT HOLDS THAT -- see #2165. A per-element
    /// tenant needs `ObjectSlot`'s owned element array, which has no stored form, so the
    /// only door that fills it demands a tenancy witness whose durable arm is an uninhabited
    /// type and cannot be spelled: see [`super::object_record::OwnedElementTenancy`], which
    /// carries the reasoning and the condition for deleting it.
    #[serde(default)]
    pub(super) objects: std::collections::BTreeMap<
        super::storage_bucket_internals::StoredModelKind,
        HashMap<ModelKey, super::object_record::ObjectSlot>,
    >,

    /// WHERE A MULTI-PAGE OBJECT'S PAGES LIVE, so they hang off the object's own record.
    ///
    /// A record keeps ONE address word that is either a page address or a handle
    /// (`BlockAddress::One` / `BlockAddress::Many`). This is what the handle half points into. Until
    /// it existed, a handle was only a number: every production mention of `BlockAddress::Many`,
    /// `Deadline::PerElement` and `RecordFlags::ADDRESS_IS_POINTER` sat inside `object_record.rs`
    /// itself, so the arm was reachable from tests and from nothing else.
    ///
    /// SKIPPED IN BOTH DIRECTIONS, AND THAT IS PROVABLY LOSSLESS RIGHT NOW RATHER THAN A BARGAIN.
    /// Nothing writes a layout yet -- `objects` is filled for exactly one kind, `ControlState`,
    /// whose entries take the inline arm -- so this is empty in every image this binary writes.
    /// Skipped in BOTH directions rather than on the write half alone: no image in existence
    /// carries this field, so there is no old row for a deserialize half to deliver, and skipping
    /// both ways leaves the stored bytes byte-identical with no format stamp owed.
    ///
    /// The one-sided spelling is deliberately not named here. `tools/` scans this struct for that
    /// token to find fields whose contents a dump drops, and it carries the match forward across
    /// comment lines so a multi-line attribute still reaches its field -- so PROSE mentioning the
    /// token arms it and the next field is reported as dropping its contents. This field does not;
    /// it is empty in every image. The scan's sensitivity to prose is a defect in the scan and is
    /// filed as one, rather than worked around by making this comment vaguer than it should be.
    ///
    /// THE CONDITION ON THE FIRST WRITER, which is the part that will not be obvious later: the
    /// first path that files a layout here MUST bring a wire form with it, because at that moment
    /// this field becomes the only copy of where those pages are and a reload would drop them.
    /// That is the same trap `ObjectSlot`'s owned element array is held back by -- see #2165 -- and
    /// the reason it is written here is that the emptiness argument above stops being true the
    /// instant a writer lands, silently.
    // Serialized, not skipped. Its own doc above says this field becomes the only
    // copy of where a record's pages are and that a reload would drop them, and the
    // scan meant to catch exactly that could not see it: the scan looks for the long
    // form of the skip attribute, and the short form contains none of that text. The
    // inner types already derive Serialize, so carrying it costs only a `default` so
    // that dumps written before this still decode.
    #[serde(default)]
    #[serde(rename = "object_block_layouts", alias = "object_page_layouts")]
    pub(super) object_block_layouts: super::object_record::BlockLayoutStore,
    /// Per `(key, bucket)` distinct-count state, in whichever representation the bucket holds.
    ///
    /// ONE MAP, WHERE THERE WERE TWO. `control_state_change_sketch` stood beside this holding the
    /// sketched buckets, keyed identically, and `record_change` kept the two mutually exclusive by
    /// removing from one as it inserted into the other. They were one logical map with the
    /// exclusivity maintained by a remove/insert pair; [`super::hll::DistinctBucket`] makes it
    /// unrepresentable instead, and that type's doc records what the merge does and does not buy.
    ///
    /// THE STORED SHAPE MOVED, WHICH IS WHY THE FORMAT STAMP MOVED WITH IT. An index written
    /// before this carries `control_state_changes` whose bucket values are bare member ARRAYS and
    /// a separate `control_state_change_sketch` object. Decoded against this shape the member
    /// array does not match either variant, and the sketch map is a field no reader claims -- so
    /// the stamp refuses the index rather than letting a decode decide, and the load falls back to
    /// WAL replay.
    #[serde(default)]
    /// KEYED BY A SHARED HANDLE, like every other map that holds a raw object key.
    ///
    /// It was `String`-keyed, and `hll::record_change` filled it with
    /// `.entry(key.to_string())` -- a fresh allocation per key. Measured by the default-run
    /// guard: SEVEN holders of one raw key text reporting TWO allocations, at 8 of 8 keys, with
    /// this map owning the second. It was invisible to that guard until it was named in it,
    /// because the guard collects pointers out of a list of map names and this was not on the
    /// list -- the staleness its own doc warns about, arriving.
    ///
    /// The stored shape does not move: `Arc<str>` serialises exactly as `String` does, which is
    /// the same argument `control_state`'s conversion made, so no format stamp is owed.
    pub(super) control_state_changes:
        HashMap<ModelKey, BTreeMap<u64, super::hll::DistinctBucket>>,
    #[serde(default, alias = "control_state_fol")]
    pub(super) control_state_selection: HashMap<ModelKey, ControlStateSelectionValue>,
    // THE THREE u64 LEDGERS ARE DOMAINS OF `side_tables` NOW, behind the same six doors.
    // Their stored spellings are unchanged; only the object they sit under is.
    // Derived, in-memory rollup ladder over `control_state` for O(levels) sum-family
    // window aggregates (frequency caps / long-window counts). Serde-skipped: rebuilt
    // lazily from the authoritative counter series and never blocks recovery. Gated by
    // Config.control_rollup_enabled(); empty and inert when the gate is off.
    // ONE FIELD FOR BOTH LADDERS NOW, keyed by `RollupDomain`, behind
    // `control_state_rollups()` / `_mut()` and `feature_rollups()` / `_mut()`. Still
    // serde-skipped, so nothing on disk moved and no stamp is owed. The domain key is what keeps
    // the two key spaces from colliding; see `RollupDomain`.
    #[serde(skip)]
    pub(super) rollups: HashMap<RollupDomain, HashMap<String, RollupEntry>>,
    // Transient per-execute hint: when true (async_storage + control_coalesce_persist),
    // control-state counter writes skip the redundant per-write whole-series block rewrite
    // and rely on the index snapshot + WAL replay for durability, exactly like the
    // control_state_changes/fol sub-stores already do. Serde-skipped; set on every execute.
    #[serde(skip)]
    pub(super) control_coalesce_persist: bool,
    // Transient per-execute hint: gate for converting oversized exact distinct sets to HLL
    // sketches on the CHANGE write path. Serde-skipped; set on every execute.
    #[serde(skip)]
    pub(super) control_distinct_sketch: bool,
    // THE LEGACY COMPATIBILITY FIELD IS GONE, and the condition its own doc set is what
    // retired it: no door can deliver a top-level `context_events` key any more. Pre-stamp
    // images are refused on every one of `decode_index_bytes`'s callers, and images at stamp
    // 20 or later do not carry the key, because the event series moved into `objects` there
    // and this field was `skip_serializing`. The field, the decode fold and its counter went
    // together, and no format stamp was owed because the field never wrote anything.
    // Time index over the same events: timeline_key -> event_id_hash, where timeline_key stays
    // timestamp_ms * CONTEXT_TIMELINE_FANOUT + (id % FANOUT). The primary map above is ordered
    // by hash, which is effectively random, so a time window is no longer a contiguous range in
    // it -- and 13 sites scan events by time window (context_timeline_start/end). Those range
    // over this index and dereference into the primary, keeping time reads at log n + k rather
    // than degrading them to a full series scan to make deletes cheaper.
    //
    // Rebuilt at load from the same block decode the event load path already performs, so it
    // costs no extra on-disk state; it is serialized with the index like the primary because
    // ShardState is snapshotted whole.
    #[serde(default)]
    pub(super) context_event_timeline: HashMap<ModelKey, BTreeMap<u64, u64>>,
    // ON THE OWNED SORTED ARRAY, NOT A B-TREE A KEY.
    //
    // 98.1% of this map's keys hold exactly ONE point, measured over 2,000 extracted events in
    // `which_series_maps_the_grouped_shape_would_help`: the index fans each event out across five
    // index kinds whose cardinalities differ by orders of magnitude, and the high-cardinality ones
    // (entity above all) give a key per value. A one-element `BTreeMap` costs 312 B in 4
    // allocations where this array costs 48 B in 2, so that distribution is the worst possible fit
    // for a B-tree a key and the best possible for the array.
    //
    // AND THIS MAP OWES NO COMPATIBILITY FIELD, which is what makes it the first tenant rather
    // than the fifth. `OwnedTimestampedSeries` is deliberately not serializable, so a map adopting
    // it normally keeps its stored image in a compat field under its historical wire key -- but
    // this field was `#[serde(default, skip_serializing)]` and has no stored image at all: the key
    // was never written, so every decode found it absent and took the default. `#[serde(skip)]` is
    // that same pair of behaviours written as one attribute -- nothing on disk changes, no format
    // stamp is owed, and it is what lets the value type stay unserializable.
    //
    // THE ATTRIBUTE AUDIT AGREES, which is worth stating because this looks like the shape it
    // hunts. `a_field_that_stops_being_written` reports a field that GAINS `skip_serializing`
    // WITHOUT a default -- a key that stops being written while a decode still demands it -- and it
    // carries a test asserting that a bare `skip` is NOT reported, because `skip` is
    // `skip_serializing` plus a default and so cannot strand a reader. This moves out of that
    // audit's scope rather than into it.
    // IN `objects` NOW, under `StoredModelKind::ContextIndex`, behind `context_indexes()`
    // and `context_indexes_mut()`. The `skip` above is gone with the field: these series are
    // DURABLE now, as every other context kind's are, so the note that used to stand here --
    // that the key was never written and no stamp was owed -- no longer holds. A stamp IS
    // owed, and is spent.
    // ON THE OWNED SORTED ARRAY. Measured at 24.0 B a point against the B-tree's 49.1 over 40,040
    // keys (`what_the_feature_series_would_cost_as_an_owned_array`): 24 bytes is EXACTLY the
    // payload -- `(u64, ElementEntry)` with a 16-byte entry -- so the array carries no per-point
    // overhead and the B-tree roughly doubles it. Unlike the grouped shape, which loses by 5% where
    // ten points fit one nearly-full node, the plain array has no occupancy at which it is worse.
    //
    // THE STORED IMAGE DOES NOT MOVE: `as_btree_maps` writes the B-tree map this field has always
    // written and reads it back into the array, exactly as `features` above does. `default` is kept
    // because this field has always had it -- `with` covers a PRESENT field and an absent one must
    // still load as empty rather than fail.
    // MOVED INTO `objects`, under `StoredModelKind::ContextAudit` -- read it through
    // `context_audits()` and write it through `context_audits_mut()`, which pick the kind once.
    //
    // Its stored image moved with it, out of a top-level key and into the consolidated
    // map's inner table, where a series is carried by `ObjectSlotWire`'s `e` array. Each
    // such move REMOVES a top-level key, so each needs a stamp bump or it is dropped in
    // silence; this one shares a bump with the other two in its change.
    // IN-MEMORY ONLY, AND COALESCING, which are two decisions with a measurement behind each.
    //
    // EPHEMERAL: this map is `#[serde(skip)]`, so a restart loses it, and that is acceptable
    // because the async summary worker re-marks nodes on the next event and stale summaries are
    // self-healing. What it replaced was a persisted `ctx:dirty` block per event -- a real
    // end-to-end capture stored 47 dirty records for only 6 events, because nothing coalesced.
    //
    // COALESCING: one entry per (tenant, node, flavour), so repeated edits to one node collapse
    // into a single row and the record count is bounded by distinct dirty nodes rather than by
    // events.
    //
    // ONE MAP FOR BOTH FLAVOURS. Summary-dirty and embedding-dirty were two maps of the same type
    // over the same key shape -- one map per attribute, where the attribute was a flavour of
    // dirtiness -- and they are one map now with the flavour as a field on the key.
    //
    // AND THE KEY IS THE THREE VALUES IT IS ABOUT, not a string of them. It used to be
    // `ctx:dirty:{tenant}:{node}` or `ctx:embdirty:{tenant}:{node}`: a heap allocation per mark
    // spelling two integers `ContextDirtyEntry` already carries as fields, with a prefix standing
    // in for the flavour. Measured at 400 context events, that was 800 keys holding 14,000
    // characters -- 52% of every key character in the context family, in the only String-keyed map
    // among them, so it could never share an allocation with anything. `DirtyMark` is 24 bytes
    // inline and nothing on the heap.
    //
    // IT ALSO REMOVED A TRAP RATHER THAN GUARDING ONE. The summary-mark site never set
    // `tenant_hash` on the row -- it was left ZERO -- and the embedding drain's tenant clause
    // admits everything when asked for all tenants, so a prefix test was the only thing stopping
    // an all-tenant drain handing summary entries out as embedding work. Its own doc called that
    // load-bearing, which was true and is the tell: a boolean spelled as a string prefix has to be
    // parsed back, and every reader then depends on the parse. The key carries the tenant for both
    // flavours now, so the two cannot be confused whether or not a drain filters on flavour.
    #[serde(skip)]
    pub(super) context_dirty_index: HashMap<DirtyMark, ContextDirtyEntry>,
    // Per-node temporal-compression high-water mark: the latest event time already
    // folded into a ContextCompressionEvent for this event object key. In-memory and
    // ephemeral (serde-skipped); on loss the auto-compression trigger re-compresses
    // the oldest pending window idempotently (stable compression id per window).
    // Entities are grouped by their OWNING NODE, not one map key per entity: the key is
    // `ctx:entity:{tenant}:{node}` (context_entity_collection_key) and the BTreeMap holds every
    // entity of that node. The inner u64 is the ENTITY HASH, not a timestamp -- unlike
    // context_events/indexes/audits, which are time-keyed. That keeps upsert as an overwrite of
    // one slot (an entity has one current value; its history is the separate
    // context_entity_update_audit series) while making a node's entities enumerable, which the
    // per-entity key shape could not do: a HashMap cannot prefix-scan, so ContextQueryEntities
    // had to be handed every entity_hash by its caller.
    // IN `objects` NOW, under `StoredModelKind::ContextEntity`, behind `context_entities()`
    // and `context_entities_mut()`. It was already stored, so the stamp is owed for the key
    // leaving the top level rather than for a new durability guarantee. The note above about
    // the entity hash and the per-node collection key moved to the door.
    // No migration field is needed: the PERSISTED entry still carries the per-entity key
    // `ctx:entity:{tenant}:{node}:{entity_hash}`, which the load path splits back into
    // (collection key, entity hash). The on-disk shape is unchanged in both directions, so an
    // index written before this fold loads natively and one written after it stays readable by
    // an older binary.
    // MOVED INTO `objects`, under `StoredModelKind::ContextChild`. There is no field here any
    // more: read it through `context_children()` and write it through `context_children_mut()`,
    // which pick the kind once. See those doors for why a field would have been the wrong shape.
    //
    // Its stored image moved with it -- out of a top-level `context_children` key and into the
    // consolidated map's inner table, where a series is carried by `ObjectSlotWire`'s `e` array.
    // That is a format move, which `SHARD_INDEX_FORMAT_VERSION` 16 covers.
    // On the owned sorted array; see `context_audits` above for the measurement and for why
    // `default` is kept beside the adapter.
    // MOVED INTO `objects`, under `StoredModelKind::ContextSummary` -- read it through
    // `context_summaries()` and write it through `context_summaries_mut()`, which pick the kind once.
    //
    // Its stored image moved with it, out of a top-level key and into the consolidated
    // map's inner table, where a series is carried by `ObjectSlotWire`'s `e` array. Each
    // such move REMOVES a top-level key, so each needs a stamp bump or it is dropped in
    // silence; this one shares a bump with the other two in its change.
    // On the owned sorted array; see `context_audits` above for the measurement and for why
    // `default` is kept beside the adapter.
    // MOVED INTO `objects`, under `StoredModelKind::ContextCompression` -- read it through
    // `context_compressions()` and write it through `context_compressions_mut()`, which pick the kind once.
    //
    // Its stored image moved with it, out of a top-level key and into the consolidated
    // map's inner table, where a series is carried by `ObjectSlotWire`'s `e` array. Each
    // such move REMOVES a top-level key, so each needs a stamp bump or it is dropped in
    // silence; this one shares a bump with the other two in its change.
    #[serde(default)]
    #[serde(rename = "slot_index")]
    pub(super) bucket_index: CoreIndex,
    /// Highest WAL sequence whose effect is already materialized in this
    /// serialized index. On shard load, WAL records with sequence greater than this
    /// are replayed to rebuild in-memory state, matching startup load
    /// replaying the WAL from the dumped-log-id anchor. `None` marks an index
    /// written before this anchor existed (treated as fully authoritative -> no
    /// replay); a missing index file replays the whole retained WAL onto empty state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(super) applied_wal_sequence: Option<u64>,
    /// Per-bucket LRU recency: wall-clock ms of the last read/write that touched the
    /// bucket. In-memory and ephemeral (serde-skipped, like context_dirty_index); on
    /// restart every bucket resets to 0 (== never touched == evicted first), which
    /// self-corrects as traffic re-warms hot buckets. Mirrors SlotNode last_used
    /// without persisting it.
    #[serde(skip)]
    pub(super) bucket_recency: HashMap<u32, u64>,
    #[serde(skip)]
    pub(super) dirty_objects: DirtyObjectIndex,
    /// Deleted keys still owed before this shard's next whole-index served checkpoint, counted
    /// DOWN: zero means the next logged-deletion round is DUE.
    ///
    /// A delete_drop eviction round re-encoded and rewrote the whole base index every time it
    /// dropped anything, at a cost set by the size of the STORE rather than the size of the round
    /// (`what_one_eviction_round_writes_grows_with_the_store_not_the_round`: 609,692 encoded bytes
    /// to drop 2 objects at 8,000 resident keys). Each round now subtracts what it deleted from
    /// this countdown and only encodes when it reaches zero, then reloads it with the engine's
    /// `logged_deletion_index_flush_interval`.
    ///
    /// EPHEMERAL, AND COUNTED DOWN RATHER THAN UP SO THAT THE POST-RELOAD VALUE IS THE SAFE ONE.
    /// `#[serde(skip)]` like `bucket_recency` and `dirty_objects`, so a reload leaves it at the
    /// `u64` default of 0 -- which reads as DUE, so the first logged-deletion round after any
    /// reload checkpoints. Counting UP would have made the same default read as "nothing is owed",
    /// which is the direction that defers on a fresh process that has checkpointed nothing.
    ///
    /// A LATE, STALE OR MISSING CHECKPOINT IS NOT A LOST DELETION. The round appends a
    /// `CommonDelete` WAL tombstone per dropped key before it flushes and anchors
    /// `applied_wal_sequence` past them, so replay re-derives every deletion this countdown
    /// defers -- `a_deferred_checkpoint_still_reloads_with_the_keys_dropped` drives that rather
    /// than asserting it.
    #[serde(skip)]
    pub(super) index_flush_countdown: u64,
    /// Published keys still owed before this shard's next whole-index served checkpoint, counted
    /// DOWN: zero means the next visibility publish is DUE.
    ///
    /// `publish_shard_index_snapshot_for_keys` republished the pages of the keys it was named and
    /// then re-encoded and rewrote the whole base index, at a cost set by the size of the STORE
    /// rather than the size of the publish
    /// (`what_one_visibility_publish_writes_grows_with_the_store_not_the_publish`: holding the
    /// named set at one key and widening the store multiplies the encode, while widening the named
    /// set 256-fold on a fixed store does not). Each publish now subtracts the keys it published
    /// from this countdown and only encodes when it reaches zero, then reloads it with the
    /// engine's `visibility_publish_index_flush_interval`.
    ///
    /// SEPARATE FROM `index_flush_countdown` ABOVE, which is the logged-deletion interval. Two
    /// operations sharing one countdown would make either one's measurement a reading of both,
    /// and a store that only publishes would spend a budget that only deletions reload.
    ///
    /// EPHEMERAL, AND COUNTED DOWN RATHER THAN UP SO THAT THE POST-RELOAD VALUE IS THE SAFE ONE.
    /// `#[serde(skip)]`, so a reload leaves it at the `u64` default of 0 -- which reads as DUE, so
    /// the first publish after any reload checkpoints. Counting UP would have made the same
    /// default read as "nothing is owed", which is the direction that defers on a fresh process
    /// that has checkpointed nothing.
    ///
    /// A LATE, STALE OR MISSING CHECKPOINT IS NOT A LOST PUBLISH. The publish neither retires the
    /// relocated blocks' registrations nor clears any bucket's `first_dirty_wal_sequence`, so both
    /// WAL retention floors still sit below the records it rewrote and replay re-derives every row
    /// this countdown defers -- `a_deferred_publish_checkpoint_still_reloads_with_the_published_values`
    /// drives that rather than asserting it.
    #[serde(skip)]
    pub(super) publish_index_flush_countdown: u64,
    /// Phase-1 flat-append fast-skip flag for the per-execute
    /// `promote_model_maps_to_bucket_index_authority` reconciliation. The live write path already
    /// keeps `bucket_index` authoritative in step with the model maps (each mutating command
    /// upserts its block into `bucket_index` before returning), so once a full promote scan has
    /// confirmed the two are in sync a repeat per-command O(store) scan can only re-confirm it.
    /// Set true after a confirmed/rebuilt reconcile at the hot-path call site; `#[serde(skip)]`
    /// so it is false on every fresh load -> the first live command after any reload pays one
    /// full reconcile and re-establishes it. Only consulted when the log's `flat_append()`.
    #[serde(skip)]
    pub(super) promote_scan_done: bool,
    /// Resume point and candidate pool for sampled eviction. In-memory and ephemeral like
    /// `bucket_recency`: on restart the scan simply restarts from the top, which costs one
    /// pass, not correctness. Only consulted when the engine's `evict_sampled_lru` is on.
    #[serde(skip)]
    pub(super) evict_sampler: super::eviction_sampler::EvictionSamplerState,
    /// THE ROUTING RANGE THIS SHARD IS LOADED ON, travelling with the shard instead of with a
    /// caller. Read through [`ShardState::routing_range`], never directly.
    ///
    /// The two functions that decide WHERE A BLOCK IS FILED -- `upsert_bucket_index_block_inner`
    /// and `sync_bucket_index_object_blocks_with_mode` -- take `&mut ShardState` and a `ShardId`
    /// and never `&self`, so the engine's `shard_routing_range` accessor, which reads the info
    /// rows under `infos.read()`, is not reachable from either. They answered with the WHOLE
    /// range, which on a shard loaded on `0..1023` files an unrouted block in a bucket the shard
    /// does not hold, where nothing scoped to the shard will ever look for it.
    ///
    /// THREE `u32`-WIDE FIELDS RATHER THAN AN `Option<(u32, u32)>`, for the same reason
    /// `LiveBlockEntry` carries a value and a flag: the flag lands in the byte this struct was
    /// already padding out for its three other `bool`s, so the pair costs 8 bytes and not 16.
    ///
    /// `#[serde(skip)]`, SO THE STORED SHAPE DOES NOT MOVE. `ShardState` IS the serialized index
    /// -- see `index_format_version` at the top of this struct for what a change to that shape
    /// has already cost once -- and a skipped field is absent from the Serialize impl entirely,
    /// so an index written before this field reads identically after it and vice versa.
    /// `the_routing_range_field_changes_no_serialized_byte` drives that rather than asserting it.
    ///
    /// THE UNSTAMPED DEFAULT IS THE WHOLE RANGE, deliberately, and it is the same default
    /// `Engine::shard_routing_range` gives a shard whose info row is absent. A `ShardState` that
    /// never entered the engine -- a decoded image, a report's scratch copy -- keeps exactly the
    /// behaviour it had. Every state the engine SERVES is stamped, because there is one function
    /// that installs one (`install_shard_state`) and it stamps;
    /// `every_shard_the_engine_installs_carries_its_routing_range` holds that as a list.
    #[serde(skip)]
    pub(super) routing_range_start: u32,
    #[serde(skip)]
    pub(super) routing_range_end: u32,
    #[serde(skip)]
    pub(super) routing_range_known: bool,
    /// THE SHARD THIS STATE IS SERVED UNDER, travelling with the shard for the same reason the
    /// routing range above does. Read through [`ShardState::shard_id`], never directly.
    ///
    /// A block's object id is `stable_block_object_id(shard, kind, key)`. `BlockIndex` carries the
    /// kind and the key beside the address, so the only term a `&ShardState` could not supply was
    /// the first one, and four functions that take a shard and never `&self` say so in their own
    /// words: `BlockIndex::object_id`, `object_manager::runtime_report`,
    /// `settle_released_bucket_object_delete` -- whose comment reads "recomputing one needs a
    /// shard id this path does not carry" -- and `bucket_generation_fingerprints_by_bucket`. None
    /// of them is changed by this field existing; what changes is that the term is now reachable.
    ///
    /// A `u64` AND A FLAG, NOT AN `Option<ShardId>`, and the flag is not decoration: shard 0 is a
    /// real shard, so a bare zero cannot mean "unstamped" the way `u32::MAX` can stand in for an
    /// unknown range end.
    ///
    /// THE PAIR COSTS SIXTEEN BYTES, MEASURED, AND THIS FLAG DID NOT LAND FREE. `ShardState` goes
    /// from 1,888 to 1,904. The routing range's flag above DID land in padding this struct already
    /// had and its doc says so; that reasoning was written into this field's first draft and the
    /// measurement refuted it, so it is recorded here rather than repeated. An `Option<u64>` would
    /// have cost the same sixteen, so the flag is not what is being paid for -- the `u64` is.
    ///
    /// Paid rather than argued down, because this struct has ONE INSTANCE PER SHARD: that is the
    /// same reason `per_item_byte_budget` carries `ShardState` as a printed control and not as a
    /// budgeted row, and at a count of one, width is not worth trading the flag's clarity for.
    /// `the_shard_carried_range_costs_eight_bytes_on_a_structure_there_is_one_of` pins the number
    /// so a later widening is a named failure rather than a silent one.
    ///
    /// `#[serde(skip)]`, SO THE STORED SHAPE DOES NOT MOVE -- see `index_format_version` at the top
    /// of this struct for what a change to that shape has cost once, and
    /// `the_shard_id_field_changes_no_serialized_byte` for the drive rather than the claim. The
    /// stamp cannot be persisted anyway: it is the id the state is SERVED under, and an index file
    /// is read back by whichever shard the engine is loading.
    ///
    /// THERE IS NO DEFAULT AND THE ACCESSOR SAYS SO. An unstamped state answers `None`, which is
    /// the one behaviour that cannot derive a plausible WRONG object id -- a zero here would hash
    /// to a real-looking value for the wrong shard. Every state the engine SERVES is stamped,
    /// because there is one function that installs one (`install_shard_state`) and it stamps; the
    /// existing `every_shard_the_engine_installs_carries_its_routing_range` holds that there is no
    /// other way in, and `every_served_shard_carries_the_id_its_owner_routes_it_under` holds that
    /// what it stamps equals the key the served map routes under, for every shard it holds.
    #[serde(skip)]
    shard_id: u64,
    #[serde(skip)]
    shard_id_known: bool,
}

impl ShardState {
    /// Record the range this shard is loaded on. Called once, by `install_shard_state`.
    pub(super) fn set_routing_range(&mut self, start_routing_bucket: u32, end_routing_bucket: u32) {
        self.routing_range_start = start_routing_bucket;
        self.routing_range_end = end_routing_bucket;
        self.routing_range_known = true;
    }

    /// The range to file an unrouted block under.
    ///
    /// An unstamped state answers the WHOLE range -- what every caller of these two writers
    /// passed unconditionally before the field existed -- so a state that never entered the
    /// engine is unchanged by this field's existence.
    pub(super) fn routing_range(&self) -> (u32, u32) {
        if self.routing_range_known {
            (self.routing_range_start, self.routing_range_end)
        } else {
            (0, u32::MAX)
        }
    }

    /// Record the shard this state is served under. Called once, by `install_shard_state`.
    pub(super) fn set_shard_id(&mut self, shard_id: crate::types::ShardId) {
        self.shard_id = shard_id;
        self.shard_id_known = true;
    }

    /// The shard this state is served under, or `None` for a state that never entered the engine.
    ///
    /// `None` and not a default, deliberately: there is no shard id that is safe to guess. A
    /// caller that answers with a zero here derives `stable_block_object_id(0, kind, key)`, which
    /// is a well-formed id belonging to a different shard -- worse than having no id at all,
    /// because nothing downstream can tell it from the right one.
    pub(super) fn shard_id(&self) -> Option<crate::types::ShardId> {
        if self.shard_id_known {
            Some(self.shard_id)
        } else {
            None
        }
    }
}

// BOTH DIRECTIONS ARE HAND-WRITTEN NOW, AND THE DECODER IS WHY THE ATTRIBUTES ARE GONE.
//
// The encoder has to be hand-written because the page index needs a SIBLING FIELD of this
// struct in scope -- serde hands a `Serialize` impl no context, so a bucket's encoder can see
// nothing but the bucket, and the blanket map impl between them is where the chain breaks.
//
// THE DECODER NEEDS THE SAME THING IN THE OTHER DIRECTION. A page's identity arrives inside the
// page index, and `object_block_lookup` -- a sibling field of this struct -- is keyed by it.
// Rebuilding that lookup after the load by walking the decoded entries is what this replaces, and
// that walk reads the identity off each entry, which is the one place it is about to stop being.
//
// AND THAT COST THE ATTRIBUTES, WHICH WAS THE RECORDED RISK OF DOING IT. They are only valid
// while a derive consumes them, so `alias = "slots"`, the defaults and the skips stop being
// DECLARATIONS the compiler enforces and become code that has to be read to be believed. Each is
// restated below as the arm that implements it, and
// `the_legacy_spelling_of_the_one_wire_field_still_loads_and_an_unknown_one_does_not` is the guard
// that was landed against the derive for exactly this moment: its discriminating pair is that the
// legacy spelling decodes POPULATED and an unrecognised one decodes EMPTY, which is the only form
// of the claim that is not true of any input whatsoever.
#[derive(Debug, Clone)]
pub(super) struct CoreIndex {
    pub(super) bucket_map: BucketMap,
    /// Per-slab live block refs and live bytes, maintained by every mutation of the map above.
    ///
    /// HERE, and not one level up on `ShardState`, for two reasons. `fold_delta_block_items` files
    /// blocks through a bare `&mut CoreIndex` and has no shard to reach for -- a tally it could not
    /// see would be a hole in the mutation surface, which is the one thing this must not have. And
    /// two fields of ONE struct are what make the borrows work at every other site:
    /// `bucket_map.get_mut(..)` loans one field while `&mut ..block_slab_live` takes the other,
    /// which the borrow checker accepts because the two places are disjoint.
    ///
    /// Derived, never persisted. Seeded by `seed_block_slab_live` wherever the index is rebuilt
    /// wholesale; until then `is_ready` is false and every consumer falls back to the walk.
    ///
    /// NOT ON THE WIRE IN EITHER DIRECTION. This carried `#[serde(skip)]`; the decoder below
    /// names no arm for it, so a stored key of this name is ignored exactly as the derive ignored
    /// it, and the encoder never wrote it.
    pub(super) block_slab_live: BlockSlabLiveIndex,
    // Derived lookup tables rebuilt from the bucket map on load. Persisting them duplicates
    // block references already carried by the bucket index and made large context backfill
    // checkpoints tens of MB larger without adding authoritative recovery state.
    ///
    /// DECODED IF PRESENT AND NEVER WRITTEN, which is what `#[serde(default, skip_serializing)]`
    /// said and what the decoder below still does: it takes a stored value when a store carries
    /// one, defaults when it does not, and files each decoded page on top of whatever it started
    /// from.
    pub(super) object_block_lookup: ObjectBlockLookup,
    /// One shared copy of each block kind, so a block holds a pointer rather than its own string.
    ///
    /// Measured over 2700 blocks: `model_id` had 2 distinct values and 2700 copies -- 1350 copies
    /// of each, and an allocation apiece. The object key, by contrast, had 1650 distinct values
    /// for those same 2700 blocks, which is why it is not interned here: sharing something that is
    /// nearly unique saves nothing.
    ///
    /// COMPONENT names share it, and that used to destroy it. The cap exists so that being
    /// bounded is a property of this code rather than a promise about every future caller -- but
    /// components are one per field, member or element of a container, so the FIRST container key
    /// written fills the cap, and after that the pool took nothing new. Including the kinds of
    /// every container written afterwards, which then allocated their own copy of a
    /// four-character string once per block. Measured over a container store of 40,000 blocks in
    /// 400 objects: 300 of the 400 held up to ONE HUNDRED distinct allocations of their model id,
    /// a name with four distinct values in the whole store.
    ///
    /// ONE POOL, TWO CEILINGS, and no second `HashSet`: a set of its own is 48 bytes on a
    /// structure there is one of per shard, and `shard_carried_range` holds `ShardState` at 1,888
    /// for exactly that reason. Components may fill the pool to `KIND_POOL_CAP`; kinds may use
    /// `KIND_RESERVE` slots beyond it, which no component can reach. The pool is still bounded,
    /// by the sum, and a component can no longer crowd out a kind.
    ///
    /// Owned by the index rather than a global, so the write path interns through the `&mut` it
    /// already holds and no lock appears on it. Not serialized -- it is a sharing detail, not part
    /// of the index, and the decoder below names no arm for it.
    pub(super) kind_pool: std::collections::HashSet<Arc<str>>,
    /// Running total of block refs across `object_component_lookup`, or `None` when not known.
    ///
    /// The stats path reports this number, and computing it as
    /// `object_component_lookup.values().map(BTreeSet::len).sum()` walks every object in the
    /// shard. That runs on a TIMER -- the server heartbeat, every 3s by default -- while holding
    /// the shard read lock that writers need, so its cost grows with the store and lands on the
    /// write path as lock contention. Measured on a 200k-record ingest in five equal phases: with
    /// the heartbeat at 1s the last phase cost 3.0x the first and the datanode's own CPU grew
    /// 3.3-3.7x; with the heartbeat off, 0.84-0.94x -- flat.
    ///
    /// Maintained by the two methods below, which are the only places the lookup is mutated, and
    /// recomputed by `rebuild_object_block_lookup`. `None` means "not established yet" (a
    /// freshly deserialized index, before any rebuild) and the reader falls back to the walk,
    /// so a missing value costs time rather than correctness.
    ///
    /// ESTABLISHED BY THE DECODE NOW, where it used to be established by the rebuild that followed
    /// one. The decoder sets it to zero before it files a single page and lets each filing count,
    /// which is the same bargain `rebuild_object_block_lookup` makes -- a count that starts at
    /// "unknown" cannot be incremented into a right answer.
    pub(super) object_component_block_refs: Option<usize>,
    /// Buckets whose block list has been released: present in `bucket_map`, `in_memory: false`,
    /// `block_index` empty, reloadable from the model maps on demand.
    ///
    /// A registry rather than a scan, because three hot paths need the answer "is anything
    /// released" in O(1): the per-execute promote reconcile (which would otherwise see a released
    /// bucket as an index that has fallen out of sync and rebuild the whole shard), the
    /// bucket-index block walk (which must supplement released buckets from the model maps rather
    /// than report them as empty), and the write path (which must reload a bucket before filing a
    /// block into it, so a node never ends up half-resident).
    ///
    /// Not serialized: release is a memory state, not a durable one. An index written while a
    /// bucket is released decodes with that bucket simply holding no blocks, and every load path
    /// re-derives `bucket_map` from the model maps anyway -- which is why the release rules above
    /// refuse the three model maps that are themselves rebuilt from the index.
    ///
    /// Not serialized in either direction; the decoder below names no arm for it.
    pub(super) released_buckets: BTreeSet<u32>,
}

/// HAND-WRITTEN FOR ONE FIELD, AND BECAUSE A LIST OF CONSTRUCTION SITES COULD NOT BE EXHAUSTIVE.
///
/// `object_component_block_refs` is `Some(0)` from birth rather than `None`. `None` means "nobody
/// has established this count", and `insert_object_block_lookup` increments only `if let Some`, so
/// a count left at `None` can never be incremented into a right answer -- it has to START at a
/// number. The decoder already said exactly that and set zero before its first filing; this makes
/// the same statement for every other way an index comes into existence.
///
/// # WHY NOT VISIT THE CONSTRUCTION SITES INSTEAD -- ENUMERATED BY THE COMPILER, NOT GREPPED
///
/// Taking `Default` off the derive and compiling names every construction. There are two EXPLICIT
/// production sites -- `rebuild_bucket_first_index` and the decoder -- and THREE IMPLICIT routes
/// that no site list can cover:
///
///   * `ShardState` derives `Default`, so `ShardState::default()` builds one;
///   * `bucket_index` carries `#[serde(default)]`, so a stored index that omits `slot_index`
///     decodes through THIS and not through the hand-written `Deserialize` that sets zero -- which
///     is the route that matters, because it is a real file on a real load; and
///   * any `..Default::default()` in a struct update.
///
/// A site list would also go stale the first time someone adds a fourth route, with nothing
/// failing. Put on the type, the claim holds for every route that exists and every route anyone
/// adds, and a field added to this struct later is a COMPILE ERROR here rather than a silent
/// `Default::default()` nobody chose.
///
/// AN EMPTY INDEX'S REF TOTAL IS NOT UNKNOWN, IT IS ZERO, which is the other half of why this is a
/// correction rather than a convenience. The two production readers test
/// `object_block_lookup.is_empty() || object_component_block_refs.is_none()` and read either half
/// as "needs establishing". The FIRST half still answers for an index holding pages its lookup
/// does not name, so what this removes is only the arm that called a known zero unknown -- and in
/// exchange, an index that files as it goes now carries a maintained total without a rebuild
/// having to establish one.
///
/// `a_default_index_reports_a_ref_total_of_zero_by_every_route_one_is_built` drives all three
/// implicit routes plus both decode paths.
impl Default for CoreIndex {
    fn default() -> Self {
        Self {
            bucket_map: BucketMap::default(),
            block_slab_live: BlockSlabLiveIndex::default(),
            object_block_lookup: ObjectBlockLookup::default(),
            kind_pool: std::collections::HashSet::default(),
            object_component_block_refs: Some(0),
            released_buckets: BTreeSet::default(),
        }
    }
}

/// WHICH KEY OF A STORED INDEX THIS IS, including the one spelling that is only ever read.
enum CoreIndexField {
    /// `bucket_map`, and `slots` -- THE LEGACY SPELLING, which was `alias = "slots"` while a
    /// derive consumed it. An index written before the rename is keyed `slots`, and dropping this
    /// arm is a store that stops loading with no other symptom: a round trip would still pass,
    /// because a round trip only ever writes the CURRENT spelling and reads it back.
    BucketMap,
    /// `object_block_lookup`: decoded if a store carries one, never written.
    ObjectBlockLookup,
    /// Everything else, INCLUDING the four fields that are not on the wire at all. Ignored rather
    /// than refused, which is what lets an index written by a newer engine load into an older one.
    Ignore,
}

impl<'de> Deserialize<'de> for CoreIndexField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct FieldVisitor;

        impl serde::de::Visitor<'_> for FieldVisitor {
            type Value = CoreIndexField;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a core index field name")
            }

            fn visit_str<E>(self, value: &str) -> Result<CoreIndexField, E>
            where
                E: serde::de::Error,
            {
                Ok(match value {
                    "bucket_map" | "slots" => CoreIndexField::BucketMap,
                    "object_block_lookup" => CoreIndexField::ObjectBlockLookup,
                    _ => CoreIndexField::Ignore,
                })
            }
        }

        deserializer.deserialize_identifier(FieldVisitor)
    }
}

const CORE_INDEX_FIELDS: &[&str] = &["bucket_map", "object_block_lookup"];

impl<'de> Deserialize<'de> for CoreIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct IndexVisitor;

        impl<'de> serde::de::Visitor<'de> for IndexVisitor {
            type Value = CoreIndex;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a core index")
            }

            fn visit_map<M>(self, mut map: M) -> Result<CoreIndex, M::Error>
            where
                M: serde::de::MapAccess<'de>,
            {
                use serde::de::Error as _;

                let mut buckets: Option<BTreeMap<u32, BucketNodeWithBlockKeys>> = None;
                let mut stored_lookup: Option<ObjectBlockLookup> = None;
                while let Some(key) = map.next_key::<CoreIndexField>()? {
                    match key {
                        CoreIndexField::BucketMap => {
                            // A SECOND STATEMENT OF THE SAME FACT IS REFUSED, as every arm of the
                            // node's decoder refuses one: a store carrying both spellings of this
                            // key is two disagreeing statements and must fail loudly.
                            if buckets.is_some() {
                                return Err(M::Error::duplicate_field("bucket_map"));
                            }
                            buckets = Some(map.next_value()?);
                        }
                        CoreIndexField::ObjectBlockLookup => {
                            if stored_lookup.is_some() {
                                return Err(M::Error::duplicate_field("object_block_lookup"));
                            }
                            stored_lookup = Some(map.next_value()?);
                        }
                        CoreIndexField::Ignore => {
                            map.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }

                let mut index = CoreIndex {
                    object_block_lookup: stored_lookup.unwrap_or_default(),
                    ..CoreIndex::default()
                };
                // ZERO BEFORE THE FIRST FILING, not after the last. `insert_object_block_lookup`
                // increments only `if let Some(total)`, so a count left at `None` can never be
                // incremented into a right answer -- it has to START at a number.
                //
                // KEPT THOUGH `Default` NOW SAYS THE SAME THING, because this decoder does not go
                // through `Default` for this field in every arm it could gain, and because the
                // statement belongs where the filing loop below can be read beside it. It is a
                // restatement rather than a second mechanism: both write the same zero, and
                // `impl Default for CoreIndex` carries the argument for why a site list cannot be
                // relied on to.
                index.object_component_block_refs = Some(0);
                for (routing_bucket, decoded) in buckets.unwrap_or_default() {
                    for (handle, object_key) in &decoded.block_keys {
                        // THE PAGE IS LOOKED UP BY THE HANDLE IT WAS FILED UNDER rather than
                        // carried beside its key, so the entry the lookup records is the entry the
                        // map holds. A page whose handle collided with an earlier one is not in
                        // the map twice and must not be in the lookup twice either.
                        if let Some(block) = decoded.node.block_index.get(handle) {
                            index.insert_object_block_lookup(
                                routing_bucket,
                                *handle,
                                object_key,
                                block,
                            );
                        }
                    }
                    index.bucket_map.insert(routing_bucket, decoded.node);
                }
                Ok(index)
            }
        }

        deserializer.deserialize_struct("CoreIndex", CORE_INDEX_FIELDS, IndexVisitor)
    }
}

/// WHERE EACH PAGE'S OBJECT IDENTITY LIVES, INDEXED BY THE POSITION THE PAGE IS HELD AT.
///
/// The object lookup runs object -> refs. This is that map inverted once per encode, so a page can
/// be asked for its identity by the `(routing_bucket, handle)` it is filed under, which is the only
/// thing a page's position actually says about it.
///
/// BUILT IN EVERY BUILD NOW, AND THAT COSTS ONE PASS PER READER THAT NEEDS IT.
///
/// It was `#[cfg(debug_assertions)]`, because while the identity was still on the entry this was
/// only a CHECK on the bytes the entry produced. It is a SOURCE now: the readers that group pages
/// by object have no other way to ask which object a page belongs to, so the inversion has to
/// exist in a release build.
///
/// WHAT THAT COSTS, STATED RATHER THAN GLOSSED. One pass over the lookup per reader that builds
/// one, which is O(block refs) -- the same order as the walk over pages the reader is already
/// doing. It is built ONCE per report and queried per page, never built per page, and never built
/// by a point reader: a reader that starts from an object goes through `block_refs_for` in the
/// forward direction and needs none of this.
///
/// THE DELETED PAGES ARE NOT IN IT, and that is the lookup's rule rather than this type's:
/// `insert_object_block_lookup` returns early on a deleted page. [`page_identity`] is the door
/// that knows about both sources, and every reader goes through it rather than through `get`.
/// WHAT A PAGE'S NEIGHBOURS SAY ITS OBJECT IS.
///
/// The object key is handed back as the SHARED POINTER rather than as characters, because every
/// caller that keeps it keeps a pointer -- the report rows, the filing map and the index-log items
/// all held `page.object_key.clone()` and must go on costing one pointer rather than a copy of the
/// characters.
///
/// THE KIND TERM IS GONE FROM THIS STRUCT, AND THE COMPILER IS WHAT SAID SO.
///
/// It carried `model_id: &'a str` beside the key, "because that is what both sources hold it as:
/// the lookup keys on the shared name and a tombstone row holds the one-byte spelling, whose
/// `as_str` is static". Its only readers were the agreement ties that compared the identity beside
/// a page against the copy ON it -- those ties compared the MODEL term as well as the key, which
/// is what made a neighbour disagreeing about either half fail. The ties went with the field and
/// `dead_code` named this immediately.
///
/// IT IS REMOVED RATHER THAN KEPT FOR A FUTURE READER. Both sources still hold the kind and
/// `identity_of` can hand it back again in one line; what a field with no reader buys is a term
/// every caller has to pattern-match past, and the entry's own `model_id` is in scope at every one
/// of those call sites anyway.
pub(super) struct BlockObjectIdentity<'a> {
    pub(super) object_key: &'a Arc<str>,
}

pub(super) struct BlockKeysByPosition<'a> {
    by_position: std::collections::HashMap<(u32, u64), (&'a Arc<str>, &'a Arc<str>)>,
    /// THE SAME ANSWER WITHOUT THE BUCKET, FOR THE ONE QUESTION POSITION CANNOT ANSWER.
    ///
    /// `by_position` is the right index for every reader that asks "which object is the page at
    /// this position", because position is what a page's filing states about it. It cannot answer
    /// for a page that is filed in the WRONG bucket -- the lookup names it at the position it was
    /// filed under, so asking at the position it now sits in finds nothing.
    ///
    /// THAT STATE IS NOT HYPOTHETICAL AND IT IS NOT AN ERROR TO SWALLOW. It is precisely what
    /// `validate_bucket_ownership_index_from_entries` exists to find, and
    /// `bucket_filing_range::the_ownership_report_names_a_block_filed_where_its_key_does_not_route`
    /// drives it: a node moved under a bucket its key does not route to must be NAMED, once per
    /// page. A walk that dropped those rows would hand the validator an empty set and the report
    /// would say the index is healthy.
    ///
    /// SO THIS IS A SECOND INDEX AND NOT A REPLACEMENT. Only the entry-collecting walk reads it,
    /// through [`Self::identity_anywhere`], and the doc there says why. A handle is hashed from a
    /// block's identity and its address, so two pages share one only by being the same block --
    /// which is the same assumption `BlockIndexMap` already keys on.
    by_handle: std::collections::HashMap<u64, (&'a Arc<str>, &'a Arc<str>)>,
}

impl<'a> BlockKeysByPosition<'a> {
    /// REACHABLE FROM THE SIBLING MODULE THAT DERIVES, not only from this file's encoder.
    ///
    /// It was private while the only builder was `CoreIndex::serialize` one screen below. The
    /// bucket-layout rebuild in `storage_bucket_internals` is a second builder, and it has to
    /// hoist its own: the inversion borrows `object_block_lookup` while the rebuild holds
    /// `bucket_map` mutably, and only the caller can split those two borrows apart by naming the
    /// fields. `CoreIndex::block_identities` cannot be used there for exactly that reason -- it
    /// borrows the whole index.
    pub(super) fn from_lookup(lookup: &'a ObjectBlockLookup) -> Self {
        let mut by_position = std::collections::HashMap::new();
        let mut by_handle = std::collections::HashMap::new();
        for (model_id, object_key, refs) in lookup.iter() {
            for block_ref in refs.iter() {
                by_position.insert(
                    (block_ref.routing_bucket, block_ref.block_ref_key),
                    (model_id, object_key),
                );
                by_handle.insert(block_ref.block_ref_key, (model_id, object_key));
            }
        }
        Self {
            by_position,
            by_handle,
        }
    }

    /// THE IDENTITY OF A LIVE PAGE AT A POSITION, AND ONLY OF A LIVE ONE.
    ///
    /// Named rather than spelled `get` because it answers for ONE of the two sources: the lookup
    /// holds no deleted page, so a caller that may be looking at a tombstone wants
    /// [`Self::identity_of`], which knows about both. A caller that has already filtered
    /// `!page.deleted` wants this, and wants it WITHOUT the node: this borrows the lookup alone,
    /// so it can be asked while the node is held mutably. `identity_of` cannot be -- it hands back
    /// a tombstone row and so borrows the node for as long as the answer lives.
    pub(super) fn live_identity(
        &self,
        routing_bucket: u32,
        handle: u64,
    ) -> Option<(&'a Arc<str>, &'a Arc<str>)> {
        self.by_position.get(&(routing_bucket, handle)).copied()
    }

    /// THE OBJECT A PAGE BELONGS TO, FROM WHICHEVER STRUCTURE BESIDE THE PAGE STATES IT.
    ///
    /// TWO SOURCES AND NOT ONE, because a page has one of two kinds of neighbour. A LIVE page is
    /// named by the object lookup, inverted by the `(routing_bucket, handle)` it is filed under. A
    /// DELETED page is not in that lookup at all -- `insert_object_block_lookup` returns early on
    /// one -- and is named instead by the tombstone row the removal filed in the same bucket,
    /// which is keyed by the page triple and carries both the object key and the kind.
    ///
    /// `None` HAS EXACTLY TWO CAUSES, both named so that a caller's skip is not a shrug. A live
    /// page is missing from the lookup only between a decode and a rebuild, a window the decoder
    /// closed by filing as it decodes. A deleted page is missing a row only when the removal that
    /// made it named no element -- a tombstone with nothing to say about which member went -- and
    /// that is the one page in this store whose object nothing beside it states.
    pub(super) fn identity_of(
        &self,
        node: &'a BucketNode,
        handle: u64,
        block: &BlockIndex,
    ) -> Option<BlockObjectIdentity<'a>> {
        if block.deleted {
            return node
                .tombstone_element_at(&block.address)
                .map(|row| BlockObjectIdentity { object_key: &row.object_key });
        }
        self.live_identity(node.routing_bucket, handle)
            .map(|(_model_id, object_key)| BlockObjectIdentity { object_key })
    }

    /// WHETHER THIS NAMES NO POSITION AT ALL, which is what an UNESTABLISHED lookup looks like.
    ///
    /// The lookup is `skip_serializing` and empty between a decode and the rebuild that fills it,
    /// so an encode taken in that window has no identity beside any page. That is a state with a
    /// NAME, and the check below refuses a missing source unless this is true -- which is the
    /// difference between a skip with a reason and a skip that hides the one case that matters.
    pub(super) fn names_no_position(&self) -> bool {
        self.by_position.is_empty()
    }

    /// THE OBJECT A PAGE BELONGS TO, WHEREVER IT IS FILED -- FOR ONE CALLER AND ONE REASON.
    ///
    /// Every other reader asks by POSITION, because what a page's filing states about it is where
    /// it sits. This asks by HANDLE alone, which answers for a page the lookup names at a
    /// different position than the one it is now in.
    ///
    /// # WHY THAT IS NOT A LOOPHOLE
    ///
    /// The one caller is `collect_bucket_index_live_block_entries`, whose rows feed
    /// `validate_bucket_ownership_index_from_entries` -- a check whose entire content is comparing
    /// where a page IS against where its key ROUTES. A mis-filed page is the subject of that
    /// check, so a walk that could not name one would hand it an empty set and the report would
    /// call a damaged index healthy. Measured: moving one node under a bucket its key does not
    /// route to must produce one mismatch per page, and does.
    ///
    /// IT IS STRICTLY WIDER THAN `identity_of` AND NEVER DISAGREES WITH IT. A page the position
    /// index names is named here with the same pair, because both are filled from the same refs.
    /// What it adds is an answer where the position index has none.
    ///
    /// DELETED PAGES ARE STILL NOT IN IT, for the lookup's own reason:
    /// `insert_object_block_lookup` returns early on one. A caller that must answer for a
    /// tombstone wants [`Self::identity_of`], which knows about the bucket's rows as well.
    pub(super) fn identity_anywhere(&self, handle: u64) -> Option<(&'a Arc<str>, &'a Arc<str>)> {
        self.by_handle.get(&handle).copied()
    }
}

/// THE IDENTITY OF EVERY PAGE IN ONE BUCKET, OWNED, SO A MUTATION CAN HOLD THE BUCKET.
///
/// # WHY THIS EXISTS AND IS NOT JUST `identity_of` IN A LOOP
///
/// `BlockIndexMap::retain` borrows the page index MUTABLY, and [`BlockKeysByPosition::identity_of`]
/// needs the NODE -- because a deleted page's identity lives in the node's tombstone rows and not
/// in the lookup. So a `retain` closure cannot ask which object the page it is looking at belongs
/// to: the borrow checker refuses it, and rightly, since the answer is drawn in part from the
/// structure being mutated.
///
/// Taken once BEFORE the mutation, from the same two sources, keyed by the handle the closure is
/// handed. A handle the result does not contain is a page NEITHER source names -- the caller
/// decides what that means, and in every current caller it means "this page is not the one I am
/// matching", which is the conservative direction: a predicate cannot match an object key the
/// page has none of, and widening it would take every sibling page of the bucket with it.
///
/// ONE PASS OVER ONE BUCKET, not over the shard. The inversion it is given is built once per
/// caller and shared across that caller's buckets; this is the per-bucket projection of it.
pub(super) fn block_identities_in(
    node: &BucketNode,
    keys: &BlockKeysByPosition<'_>,
) -> std::collections::HashMap<u64, Arc<str>> {
    node.block_index
        .iter()
        .filter_map(|(handle, block)| {
            keys.identity_of(node, *handle, block)
                .map(|beside| (*handle, Arc::clone(beside.object_key)))
        })
        .collect()
}

/// THE ONE FIELD OF THIS STRUCT THAT REACHES THE WIRE, and the context the rest of the encode needs.
///
/// Hand-written for the reason stated at the struct: the page index has to be composed where a
/// SIBLING of the bucket map is in scope, and no derive and no `serialize_with` can offer that,
/// because both see one field at a time.
///
/// THE FIELD COUNT IS PART OF THE ENCODING, not a hint. The served index is msgpack
/// `with_struct_map`, so `serialize_struct` writes a map header of exactly this length -- the same
/// reason `BucketNode` counts its own optional key rather than hard-coding one. One field, because
/// five of the six are not written, and `what_the_core_index_puts_on_the_wire` observes that from a
/// real encode rather than reading it off the attributes.
impl Serialize for CoreIndex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        // BUILT IN EVERY BUILD, BECAUSE IT IS THE SOURCE AND NOT A CHECK.
        //
        // It was `#[cfg(debug_assertions)]` here while the written key was composed from the
        // entry's own copy of the identity and this inversion only CONFIRMED it. The key is
        // composed from the inversion now, so a release build without one would write no key at
        // all. The type's own doc prices the pass.
        let keys = BlockKeysByPosition::from_lookup(&self.object_block_lookup);
        let mut index = serializer.serialize_struct("CoreIndex", 1)?;
        index.serialize_field(
            "bucket_map",
            &BucketMapAt {
                buckets: &self.bucket_map,
                keys: &keys,
            },
        )?;
        index.end()
    }
}

/// The bucket map, with each node given the context the derive could not pass it.
///
/// The map is written exactly as the blanket impl wrote it -- same length header, same key order,
/// same key encoding -- because a `BTreeMap` iterates in key order and that is what it did before.
/// What changes is which `Serialize` each VALUE goes through.
struct BucketMapAt<'a> {
    buckets: &'a BucketMap,
    keys: &'a BlockKeysByPosition<'a>,
}

impl Serialize for BucketMapAt<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.buckets.len()))?;
        for (routing_bucket, node) in self.buckets {
            map.serialize_entry(
                routing_bucket,
                &BucketNodeAt {
                    node,
                    keys: self.keys,
                },
            )?;
        }
        map.end()
    }
}

/// One node, with the page-key context in scope.
///
/// ONE PRODUCTION DOOR NOW, AND THE CONTEXT IS NOT OPTIONAL. `impl Serialize for BucketNode`
/// stood beside this as the door for a fixture holding a bare node, passing `None` for the
/// context, and that `None` is the escape hatch this commit closes: a keyless node encode
/// composes every page key from the entry's own copy of the identity, which with the identity off
/// the entry is a key naming nothing, once per page, returned as `Ok`.
///
/// So the context arrives by value rather than as an `Option`, and the type system is what says a
/// node this engine writes cannot be encoded without one.
///
/// THE FIXTURE DOOR SURVIVES AS A `#[cfg(test)]` SCAFFOLD WITH A NAME THAT SAYS WHAT IT IS, and
/// it is NOT reachable from production -- `--lib` compiles with no `Serialize for BucketNode` at
/// all. Twenty of the twenty-six callers of the two fixture doors encode a bare node and six a
/// bare page map, every one of them a byte-shape or byte-budget assertion over a hand-built
/// fixture. They are named by [`PageIndexFromTheEntriesOwnCopies`] and they end when the field
/// does.
struct BucketNodeAt<'a> {
    node: &'a BucketNode,
    keys: &'a BlockKeysByPosition<'a>,
}

impl Serialize for BucketNodeAt<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_bucket_node(
            self.node,
            &BlockIndexAt { node: self.node, keys: self.keys },
            serializer,
        )
    }
}

pub(super) type BucketMap = BTreeMap<u32, BucketNode>;
/// The object ids a bucket holds.
///
/// Almost always exactly one: keys route one to a bucket, and even an object with many components
/// is still one object. A `BTreeSet` holding a single id costs 128 live bytes of node to carry
/// eight bytes of id, once per bucket and so once per key.
///
/// The shape `BlockIndexMap` and `BlockRefs` already use here, for the same reason.
///
/// THE RARE ARM IS A SLOT ARRAY AND NOT A SORTED RUN, so a slot number names the same object
/// for as long as that object is in the bucket. See [`ObjectSlots`] for the three rules that
/// make that true, the bound they give, and what they cost. The tiering above it is unchanged.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) enum ObjectIndex {
    #[default]
    Empty,
    One(u64),
    /// Several ids, held as a SLOT ARRAY behind one pointer.
    ///
    /// Boxed because an enum is as wide as its widest arm: held inline the collection made every
    /// `ObjectIndex` wider than a word whether or not it held anything, and a `BucketNode`
    /// carries two of them. Behind a box the enum is 16, and the box is only allocated by the
    /// buckets that actually hold more than one object.
    ///
    /// AN ARRAY AND NOT A TREE, because this arm is not rare. The measured occupancy is 46.6% of
    /// buckets holding two or more -- a collection key files one object id PER MEMBER into the
    /// one bucket its key routes to, while a string or a series files one -- so what this arm
    /// costs is paid by nearly half the store. A search tree charges a fixed 128 bytes for it: 24
    /// for the collection and 104 for a node sized to hold eleven ids whether it holds two or
    /// eleven.
    ///
    /// NOT SORTED, AND THAT IS THE CHANGE. It was a sorted run, which cannot be indexed: `insert`
    /// placed a new id at its bisection position, so every later position silently repointed. See
    /// [`ObjectSlots`] for the three rules that replace it, the bound they give, and the two things
    /// they cost -- the bisection `contains` used to answer by, and eight bytes a slot.
    Many(Box<ObjectSlots>),
}

/// A pointer-wide arm plus its tag, once per bucket.
///
/// It used to be twice: the tombstone side held the same enum for a case it is in 2.32% of
/// the time, and is now `DeletedObjectIndex` below, at eight.
/// ARITHMETIC AND NOT A LITERAL: the discriminant rounded up to the payload's alignment, plus
/// the payload. A literal 16 passes for the wrong reason the moment an arm widens into padding
/// that was already there, which is how three pins in this campaign under-reached.
/// `the_object_index_is_a_tag_and_one_pointer` measures the same identity with `offset_of!`.
const _: () = assert!(
    std::mem::size_of::<ObjectIndex>()
        == std::mem::align_of::<ObjectIndex>() + std::mem::size_of::<Box<ObjectSlots>>()
);

/// THE IDS OF A BUCKET HOLDING MORE THAN ONE OBJECT, AS A SLOT ARRAY: slot `i` names the same
/// object for as long as that object is in the bucket.
///
/// WHY NOT A SORTED RUN, which is what this was. A sorted run cannot be indexed, and #2023 drove
/// exactly that on twenty-five ids: position 12 named id 1300, inserting one unrelated SMALLER id
/// made the same position name 1200, a removal made it 1400, and `shrink` collapsing the arm to
/// `One` retired position 1 outright. Nothing in the store stored such a position, so nothing was
/// broken -- but nothing could, and that is what closed the route of naming an object by its
/// position instead of by sixteen bytes of `Arc<str>` on every page entry.
///
/// WHAT MAKES A SLOT STABLE, and it is three rules rather than one:
///
///   * `insert` takes the FIRST FREE SLOT, else appends. It never moves an id already filed, so no
///     insert can repoint a slot another id is in. This is the rule the sorted run broke.
///   * `remove` leaves a PLACEHOLDER. The slot keeps its position and `valid` falls; the array
///     never closes a hole, because closing one is exactly what renumbers every id above it.
///   * `valid` is tracked SEPARATELY from `slots.len()`. They are different numbers the moment a
///     placeholder exists, so every reader has to say which it wants: `ObjectIndex::object_count`
///     or `ObjectIndex::slot_count`, and there is no `len` to read by accident.
///
/// AND ONE THING THAT IS GIVEN BACK, which renumbers nothing. A removal that leaves free slots at
/// the END pops them, and the arm collapses to `One` only when a single id is left IN SLOT ZERO with
/// no slot above it. Both are index-preserving by inspection rather than by argument: a live id's
/// slot is never renumbered, and the positions dropped held no id for anyone to be naming.
///
/// SO THE ARRAY IS BOUNDED BY ITS OWN HIGH WATER MARK AND NOT BY CHURN. A bucket that has held at
/// most `n` objects at once is at most `n` slots long however many times it has been added to and
/// deleted from: a hole is refilled before the array grows, and a tail of holes is not kept. An
/// array that gave nothing back would instead grow with the number of OPERATIONS, which is the
/// failure this shape has to be shown to avoid rather than assumed to.
///
/// WHAT IT COSTS, both of which are measured and neither of which is nothing:
///
///   * THE BISECTION. `contains` was `binary_search` over a sorted run -- `floor(log2(n)) + 1`
///     entries examined on the one door every membership question goes through. A slot array is in
///     slot order, so the walk is linear in the SLOTS, placeholders included. At the run lengths
///     this store actually holds the two coincide (`floor(log2(n)) + 1 == n` for n of 1 and 2);
///     they diverge on the long lists a container bucket produces.
///   * EIGHT BYTES A SLOT. A slot is `Option<u64>` and not a `u64` with a reserved value, because
///     object ids are FNV-1a hashes over the shard, kind and key
///     (`hashing::stable_block_object_id`) and so span the whole of `u64`: any value picked to mean
///     free is a value some key hashes to, and the collision would read as a missing object rather
///     than as an error. `Option<u64>` has no niche, so a slot is 16 bytes where the run's element
///     was 8. `what_a_slot_array_costs_under_churn` prints what a reserved value would buy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ObjectSlots {
    /// Slot `i` holds the id filed there, or `None` for a placeholder.
    slots: Vec<Option<u64>>,
    /// How many slots hold an id. NOT `slots.len()`, which counts placeholders too.
    ///
    /// `usize` and not the `u8` a 255-slot ceiling would allow: boxed, this struct is 32 bytes with
    /// either -- a `Vec` is 24 and the tail rounds to 8 -- so the narrow one buys no byte and costs
    /// a truncation to reason about. A ceiling here would also be a ceiling on a bucket, and a
    /// bucket fills with the corpus.
    valid: usize,
}

impl ObjectSlots {
    /// Two ids, in the slots they keep.
    fn pair(first: u64, second: u64) -> Self {
        ObjectSlots { slots: vec![Some(first), Some(second)], valid: 2 }
    }

    /// Slots read back from a stored index, IN THE POSITIONS THEY WERE WRITTEN IN.
    ///
    /// NOT through `insert`, and that is the whole change. `insert` hands out the first free slot,
    /// so re-filing a stored array through it reproduces the written ORDER and not the written
    /// POSITIONS -- which is how a slot came back naming a different object after a load, and why
    /// nothing was allowed to store one.
    ///
    /// A REPEATED ID IS PLACEHOLDERED WHERE IT REPEATS, not dropped: dropping it would shorten the
    /// array and move every id above it, which is the renumber this constructor exists to avoid.
    /// The first position wins, because it is the one anything already stored would be naming. The
    /// count is returned so the caller can report it rather than swallow it.
    fn from_stored_slots(mut slots: Vec<Option<u64>>) -> (Self, u64) {
        let mut repeated = 0u64;
        let mut seen: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
        for slot in slots.iter_mut() {
            if let Some(id) = *slot {
                if !seen.insert(id) {
                    *slot = None;
                    repeated += 1;
                }
            }
        }
        let mut held = ObjectSlots { valid: slots.iter().filter(|slot| slot.is_some()).count(), slots };
        // Trailing placeholders are not kept, exactly as a removal does not keep them: the
        // positions dropped hold no id, so nothing can be naming them.
        held.drop_trailing_placeholders();
        (held, repeated)
    }

    fn valid(&self) -> usize {
        self.valid
    }

    fn slot_count(&self) -> usize {
        self.slots.len()
    }

    /// A LINEAR WALK, charging what it actually looked at.
    ///
    /// Placeholders are counted: they are words the walk reads, so a bucket whose array has grown
    /// holes pays for them here, and the probe counter shows the waste instead of hiding it behind
    /// a valid count.
    fn locate(&self, id: &u64) -> (Option<usize>, u64) {
        let mut examined = 0u64;
        for (at, slot) in self.slots.iter().enumerate() {
            examined += 1;
            if slot.as_ref() == Some(id) {
                return (Some(at), examined);
            }
        }
        (None, examined)
    }

    fn id_at(&self, slot: usize) -> Option<u64> {
        self.slots.get(slot).copied().flatten()
    }

    /// FIRST FREE SLOT, ELSE APPEND -- and nothing already filed moves.
    fn insert(&mut self, id: u64) -> bool {
        if self.locate(&id).0.is_some() {
            return false;
        }
        match self.slots.iter().position(Option::is_none) {
            Some(free) => self.slots[free] = Some(id),
            None => self.slots.push(Some(id)),
        }
        self.valid += 1;
        true
    }

    /// A PLACEHOLDER, NOT A CLOSED HOLE. The slot keeps its position.
    fn remove(&mut self, id: &u64) -> bool {
        let Some(at) = self.locate(id).0 else {
            return false;
        };
        self.slots[at] = None;
        self.valid -= 1;
        self.drop_trailing_placeholders();
        true
    }

    /// Free slots at the END are given back, which renumbers nothing: every position dropped held
    /// no id, and every id still filed is below the new length. This is what keeps the array bounded
    /// by the bucket high water mark instead of by its churn.
    fn drop_trailing_placeholders(&mut self) {
        while self.slots.last().is_some_and(Option::is_none) {
            self.slots.pop();
        }
    }

    fn iter(&self) -> std::slice::Iter<'_, Option<u64>> {
        self.slots.iter()
    }

    fn into_ids(self) -> Vec<u64> {
        self.slots.into_iter().flatten().collect()
    }
}

pub(super) enum ObjectIndexIter<'a> {
    Empty,
    One(std::iter::Once<&'a u64>),
    Many(std::slice::Iter<'a, Option<u64>>),
}

impl<'a> Iterator for ObjectIndexIter<'a> {
    type Item = &'a u64;
    fn next(&mut self) -> Option<&'a u64> {
        match self {
            ObjectIndexIter::Empty => None,
            ObjectIndexIter::One(once) => once.next(),
            // Placeholders are skipped: a hole is not an object, and `object_count` is what
            // says how many this will yield.
            ObjectIndexIter::Many(slots) => slots.find_map(|slot| slot.as_ref()),
        }
    }
}

impl ObjectIndex {
    /// HOW MANY OBJECTS THIS HOLDS -- which is not how long the slot array is.
    ///
    /// Named rather than spelt `len`, and the rename is half the change. A `len` on a slot array is
    /// ambiguous between the count and the array, and the two differ the moment a placeholder
    /// exists; renaming it made the COMPILER name every reader of the old number, instead of leaving
    /// each one to be guessed at from a grep over a field name five structs share. See `slot_count`
    /// for the other number.
    pub(super) fn object_count(&self) -> usize {
        match self {
            ObjectIndex::Empty => 0,
            ObjectIndex::One(_) => 1,
            ObjectIndex::Many(slots) => slots.valid(),
        }
    }

    /// HOW LONG THE SLOT ARRAY IS, placeholders included.
    ///
    /// Never an object count. It is the bound on a slot number, and the number
    /// `what_a_slot_array_costs_under_churn` divides by `object_count` to report the waste.
    pub(super) fn slot_count(&self) -> usize {
        match self {
            ObjectIndex::Empty => 0,
            ObjectIndex::One(_) => 1,
            ObjectIndex::Many(slots) => slots.slot_count(),
        }
    }

    /// The slot an id is filed in, stable for as long as the id is in the bucket.
    ///
    /// The observable surface of the whole change: without it the three rules hold but nothing can
    /// read them, and a test driving them would be asserting about a private field. A page entry
    /// naming its object by slot -- the step this one is the precondition for -- consumes this and
    /// `id_at` and nothing else.
    pub(super) fn slot_of(&self, id: &u64) -> Option<usize> {
        match self {
            ObjectIndex::Empty => None,
            ObjectIndex::One(held) => (held == id).then_some(0),
            ObjectIndex::Many(slots) => slots.locate(id).0,
        }
    }

    /// The id a slot holds: `None` for a placeholder, and `None` past the end of the array.
    pub(super) fn id_at(&self, slot: usize) -> Option<u64> {
        match self {
            ObjectIndex::Empty => None,
            ObjectIndex::One(held) => (slot == 0).then_some(*held),
            ObjectIndex::Many(slots) => slots.id_at(slot),
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        matches!(self, ObjectIndex::Empty)
    }

    /// By a WALK over the slot array, because slot order is not id order.
    ///
    /// This is what the slot array costs on the one door every membership question goes through.
    /// The run answered by bisection -- `floor(log2(n)) + 1` entries -- and the walk charges the
    /// slots it reads, placeholders included. `the_join_a_name_reader_would_pay` prints both models
    /// against the run lengths this store holds, where for n of 1 and 2 they are the same number.
    pub(super) fn contains(&self, id: &u64) -> bool {
        match self {
            ObjectIndex::Empty => {
                note_object_index_entries_examined(0);
                false
            }
            ObjectIndex::One(held) => {
                note_object_index_entries_examined(1);
                held == id
            }
            ObjectIndex::Many(slots) => {
                // Charged here rather than at the callers because this is the one door every
                // membership question goes through.
                let (found, examined) = slots.locate(id);
                note_object_index_entries_examined(examined);
                found.is_some()
            }
        }
    }

    pub(super) fn insert(&mut self, id: u64) -> bool {
        match self {
            ObjectIndex::Empty => {
                *self = ObjectIndex::One(id);
                true
            }
            ObjectIndex::One(held) => {
                if *held == id {
                    return false;
                }
                // The id already here KEEPS SLOT ZERO. The sorted run put the smaller of the two
                // first, which is what made slot zero name a different object after an insert of
                // something smaller than what was already filed.
                *self = ObjectIndex::Many(Box::new(ObjectSlots::pair(*held, id)));
                true
            }
            ObjectIndex::Many(slots) => slots.insert(id),
        }
    }

    pub(super) fn remove(&mut self, id: &u64) -> bool {
        match self {
            ObjectIndex::Empty => false,
            ObjectIndex::One(held) => {
                if held != id {
                    return false;
                }
                *self = ObjectIndex::Empty;
                true
            }
            ObjectIndex::Many(slots) => {
                let removed = slots.remove(id);
                self.settle();
                removed
            }
        }
    }

    /// Give up the array when it no longer names anything -- WITHOUT RENUMBERING A LIVE SLOT.
    ///
    /// THIS IS THE COLLAPSE THAT USED TO RETIRE A SLOT. It was `shrink`, and on reaching one id it
    /// rewrote `Many` to `One(that id)` whichever slot the id was in. A bucket holding
    /// `[placeholder, id]` became `One(id)`: a holder of slot 1 was left naming nothing, and a
    /// holder of slot 0 -- which had held a different object -- was left naming this one. #2023
    /// drove it as "the collapse retired ordinal 1", and it is the third of the three mutations that
    /// closed the route.
    ///
    /// WHAT IT DOES NOW. Exactly two collapses survive, and both are index-preserving by inspection
    /// rather than by argument:
    ///
    ///   * NO IDS LEFT -> `Empty`. There is no live slot to preserve, so there is nothing anyone can
    ///     be naming. `drop_trailing_placeholders` has already emptied the array by then, which is
    ///     why this is the only shape `valid == 0` can be in.
    ///   * ONE ID LEFT, IN SLOT ZERO, WITH NO SLOT ABOVE IT -> `One(id)`. Slot 0 names that id in
    ///     both shapes, and no other slot exists in either, so `slot_of` and `id_at` cannot tell the
    ///     two apart. That indistinguishability is what the test drives.
    ///
    /// A single id in slot 1, or in slot 0 with a placeholder above it, STAYS `Many`. That is
    /// precisely the case the old collapse got wrong, and it is the case a bucket is in whenever the
    /// object deleted is not the one filed first.
    fn settle(&mut self) {
        let (valid, slot_count) = match self {
            ObjectIndex::Many(slots) => (slots.valid(), slots.slot_count()),
            _ => return,
        };
        if valid == 0 {
            *self = ObjectIndex::Empty;
            return;
        }
        if valid == 1 && slot_count == 1 {
            let id = match self {
                ObjectIndex::Many(slots) => slots.id_at(0).expect("one valid id in one slot"),
                _ => unreachable!("just matched Many"),
            };
            *self = ObjectIndex::One(id);
        }
    }

    /// An index read back from a stored one, IN THE POSITIONS IT WAS WRITTEN IN.
    ///
    /// Goes through the same two collapses `settle` applies, so a loaded index is in the shape a
    /// live one would be in and `slot_of` and `id_at` cannot tell a loaded bucket from a built one.
    /// That indistinguishability is what the reload test drives.
    ///
    /// A repeated id is reported rather than swallowed: it means the stored array disagreed with
    /// itself, which is a thing an operator should be able to see a count of.
    pub(super) fn from_stored_slots(slots: Vec<Option<u64>>) -> Self {
        let (held, repeated) = ObjectSlots::from_stored_slots(slots);
        if repeated > 0 {
            note_stored_slots_repeated_an_id(repeated);
        }
        // THE ARM IS CHOSEN BEFORE ANYTHING IS BOXED, and that is not a tidy-up.
        //
        // This built `Many(Box::new(held))` and then called `settle`, which allocated a box for
        // every bucket on every load and threw it away again for the two arms most buckets are
        // actually in. `a_bucket_holding_one_object_holds_no_node` and
        // `the_tombstone_index_gives_its_allocation_back_when_it_empties` both caught it: a shard
        // index is read once per bucket and twice counting the tombstone side, so that was an
        // allocation per bucket per load on a path that had none.
        //
        // Constructing `held` itself allocates nothing -- it moves the vector the deserializer
        // already built -- so the branch is free and the box is taken only by the arm that needs it.
        // The two collapses are `settle`'s, spelled here against the same two conditions.
        match (held.valid(), held.slot_count()) {
            (0, _) => ObjectIndex::Empty,
            (1, 1) => ObjectIndex::One(held.id_at(0).expect("one valid id in one slot")),
            _ => ObjectIndex::Many(Box::new(held)),
        }
    }

    pub(super) fn clear(&mut self) {
        *self = ObjectIndex::Empty;
    }

    pub(super) fn iter(&self) -> ObjectIndexIter<'_> {
        match self {
            ObjectIndex::Empty => ObjectIndexIter::Empty,
            ObjectIndex::One(id) => ObjectIndexIter::One(std::iter::once(id)),
            ObjectIndex::Many(slots) => ObjectIndexIter::Many(slots.iter()),
        }
    }

    /// The ids this holds, ASCENDING, whatever slots they are in.
    ///
    /// `iter` yields SLOT ORDER, which is an order over slots and not over ids. Exactly one reader
    /// wants them sorted -- the Serialize impl, so that the bytes already on disk do not move -- and
    /// it says so here instead of relying on the container to be a sorted run, which it no longer
    /// is.
    pub(super) fn sorted_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.iter().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// The ids this holds that `other` does not.
    pub(super) fn difference<'a>(
        &'a self,
        other: &'a ObjectIndex,
    ) -> impl Iterator<Item = &'a u64> + 'a {
        self.iter().filter(move |id| !other.contains(id))
    }
}

impl<'a> IntoIterator for &'a ObjectIndex {
    type Item = &'a u64;
    type IntoIter = ObjectIndexIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for ObjectIndex {
    type Item = u64;
    type IntoIter = std::vec::IntoIter<u64>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            ObjectIndex::Empty => Vec::new().into_iter(),
            ObjectIndex::One(id) => vec![id].into_iter(),
            ObjectIndex::Many(slots) => (*slots).into_ids().into_iter(),
        }
    }
}

impl From<BTreeSet<u64>> for ObjectIndex {
    fn from(ids: BTreeSet<u64>) -> Self {
        ids.into_iter().collect()
    }
}

impl Extend<u64> for ObjectIndex {
    fn extend<I: IntoIterator<Item = u64>>(&mut self, ids: I) {
        for id in ids {
            self.insert(id);
        }
    }
}

impl<'a> Extend<&'a u64> for ObjectIndex {
    fn extend<I: IntoIterator<Item = &'a u64>>(&mut self, ids: I) {
        for id in ids {
            self.insert(*id);
        }
    }
}

impl FromIterator<u64> for ObjectIndex {
    fn from_iter<I: IntoIterator<Item = u64>>(ids: I) -> Self {
        let mut index = ObjectIndex::default();
        index.extend(ids);
        index
    }
}

impl ObjectIndex {
    /// THE IDS ASCENDING, HOLES OMITTED -- the spelling this wrote before the slot array became
    /// durable, kept for the one side that does not need a durable slot.
    ///
    /// WHY IT STILL EXISTS. Nothing names a TOMBSTONE by slot: the deleted-object side is the ids a
    /// bucket has had removed and not yet reclaimed, read as a set and never indexed into. Writing
    /// it in slot order would move bytes already on disk for no gain at all, on the 2.32% of
    /// buckets that carry one.
    ///
    /// SO THE TWO SIDES NOW DIFFER DELIBERATELY, and that is why this is a named method rather than
    /// a copy. `DeletedObjectIndex` used to delegate to the live side's `Serialize` on the
    /// reasoning that two impls which agreed would be two impls to keep agreeing. They no longer
    /// agree -- one is positional and one is ascending -- so delegation would silently drag the
    /// tombstone side along. One body for each rule, and the caller says which rule it wants.
    ///
    /// Allocation-free on the two arms that hold nothing or one id, which is almost all of them.
    pub(super) fn serialize_ascending<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        match self {
            ObjectIndex::Empty => serializer.serialize_seq(Some(0))?.end(),
            ObjectIndex::One(id) => {
                let mut seq = serializer.serialize_seq(Some(1))?;
                seq.serialize_element(id)?;
                seq.end()
            }
            ObjectIndex::Many(_) => {
                let ids = self.sorted_ids();
                let mut seq = serializer.serialize_seq(Some(ids.len()))?;
                for id in &ids {
                    seq.serialize_element(id)?;
                }
                seq.end()
            }
        }
    }
}

impl Serialize for ObjectIndex {
    /// THE SLOT ARRAY, IN SLOT ORDER, WITH ITS PLACEHOLDERS -- which is the change.
    ///
    /// This wrote `sorted_ids()`: ascending, holes omitted. That kept every byte where a sorted run
    /// had put it and took no format stamp, and it also meant NOTHING PERSISTED A SLOT. A load
    /// re-filed the written ids through `insert`, so positions were handed out afresh on every load
    /// and a slot was a resident fact. A page entry naming its object by slot cannot be built on
    /// that, and this is the step that makes the slot durable instead.
    ///
    /// WHAT MOVES ON DISK, AND IT IS TWO THINGS. The sequence is no longer ascending -- it is slot
    /// order, which is filing order -- and a hole is written as `null` where it used to be left out.
    /// Both change the bytes of every bucket holding more than one object, which the measurement
    /// puts at 46.6% of them, so this takes the next `SHARD_INDEX_FORMAT_VERSION`.
    ///
    /// AN OLDER INDEX STILL DECODES, AND LANDS WHERE IT WOULD HAVE -- AS DEFENCE IN DEPTH AND NOT
    /// AS THE UPGRADE PATH. An ascending sequence of bare ids deserializes as `Some` in the
    /// positions it is written in, and ascending positions are exactly what the old loader's
    /// `insert` would have handed out for it.
    ///
    /// THE UPGRADE PATH IS THE STAMP, THOUGH. `persistence.rs` compares the stored stamp with `<`
    /// and refuses anything below the constant, so an index written before this change is REFUSED
    /// and rebuilt from the log rather than decoded -- which is the established cost of a stamp
    /// here. The tolerance above matters on the one shape where a decode runs before the check, and
    /// it is worth having for exactly that reason, but it is not what an upgrade exercises.
    ///
    /// AND THE TWO COMMON ARMS STILL DO NOT ALLOCATE. A shard index is written on every dump, once
    /// per bucket and twice counting the tombstone side, and `Empty` and `One` are what most buckets
    /// are in. Neither builds a vector: the sort that used to stand here is gone, because slot order
    /// is now the order being written.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        match self {
            ObjectIndex::Empty => serializer.serialize_seq(Some(0))?.end(),
            ObjectIndex::One(id) => {
                let mut seq = serializer.serialize_seq(Some(1))?;
                // `Some(id)` and not `id`: the element type of this sequence is now nullable, and
                // writing the one-object arm as a bare number would make the two arms write
                // different shapes for the same array.
                seq.serialize_element(&Some(*id))?;
                seq.end()
            }
            ObjectIndex::Many(slots) => {
                let mut seq = serializer.serialize_seq(Some(slots.slot_count()))?;
                for slot in slots.iter() {
                    seq.serialize_element(slot)?;
                }
                seq.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ObjectIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // BY POSITION, NOT THROUGH `insert`. This read `Vec<u64>` and collected it, which put
        // each id in the first free slot and so reproduced the written ORDER rather than the
        // written POSITIONS. That is what made a slot resident-only.
        //
        // `Option<u64>` is what accepts both shapes: a `null` is the placeholder this now writes,
        // and a bare number from an index written before this change comes back as `Some` in the
        // position it was written in. So the tolerance is in the element type and needs no probing
        // of the stored form.
        let slots = Vec::<Option<u64>>::deserialize(deserializer)?;
        Ok(ObjectIndex::from_stored_slots(slots))
    }
}

/// THE TOMBSTONE SIDE OF A BUCKET, held so that ABSENCE costs a pointer and nothing else.
///
/// `deleted_object_index` is the ids a bucket has had deleted and not yet reclaimed. It is
/// written by one path -- a delete that finds blocks to retire -- and cleared by the next write of
/// the same object, so on a store that is not being deleted from it holds nothing at all.
/// MEASURED over a seeded corpus at two sizes with one string key in twenty deleted: 97.68% of
/// buckets carry no tombstone, and the widest bucket that carries one carries a single id.
///
/// Held as `ObjectIndex` that is the sixteen bytes of an enum whether or not it holds anything,
/// twice over on the two arms that never run. Held as one nullable pointer it is eight, and the
/// sixteen it used to spend are only spent by the buckets that are actually carrying a tombstone
/// -- which is the same tiering `ObjectIndex` already applies one level down, with the tier that
/// costs nothing moved to the case this field is actually in.
///
/// WHERE THE TRADE TURNS OVER, because it is not free in the other direction: a bucket that DOES
/// carry a tombstone pays the pointer plus a sixteen-byte allocation for the index behind it,
/// which the allocator serves out of its smallest chunk. So the shape wins while fewer than
/// about a quarter of buckets carry one and loses above that, against a measured 2.32%.
/// `what_the_object_side_of_the_bucket_node_costs` prints the share beside the saving so a
/// workload that moved it would be visible rather than assumed.
///
/// `Some` ALWAYS HOLDS A NON-EMPTY INDEX. `remove` gives the allocation back when it takes the
/// last id, so "carrying nothing" has exactly one spelling and two buckets holding no tombstone
/// cannot compare unequal.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct DeletedObjectIndex(Option<Box<ObjectIndex>>);

/// One nullable pointer, against the sixteen bytes the enum spends inline.
const _: () = assert!(std::mem::size_of::<DeletedObjectIndex>() == 8);

impl DeletedObjectIndex {
    pub(super) fn object_count(&self) -> usize {
        self.0.as_ref().map_or(0, |index| index.object_count())
    }

    pub(super) fn is_empty(&self) -> bool {
        self.0.as_ref().map_or(true, |index| index.is_empty())
    }

    pub(super) fn contains(&self, id: &u64) -> bool {
        self.0.as_ref().is_some_and(|index| index.contains(id))
    }

    pub(super) fn insert(&mut self, id: u64) -> bool {
        self.0
            .get_or_insert_with(|| Box::new(ObjectIndex::default()))
            .insert(id)
    }

    /// Removing the last id gives the allocation back, which is what keeps the empty state to a
    /// single spelling and keeps a bucket that was briefly deleted from holding a box for ever.
    pub(super) fn remove(&mut self, id: &u64) -> bool {
        let Some(index) = self.0.as_mut() else {
            return false;
        };
        let removed = index.remove(id);
        if index.is_empty() {
            self.0 = None;
        }
        removed
    }

    pub(super) fn iter(&self) -> ObjectIndexIter<'_> {
        match &self.0 {
            None => ObjectIndexIter::Empty,
            Some(index) => index.iter(),
        }
    }

    /// The ids this holds, ASCENDING. See [`ObjectIndex::sorted_ids`]: `iter` is in slot order, and
    /// the Serialize impl is the one reader that needs the other.
    pub(super) fn sorted_ids(&self) -> Vec<u64> {
        self.0.as_ref().map_or_else(Vec::new, |index| index.sorted_ids())
    }
}

impl<'a> IntoIterator for &'a DeletedObjectIndex {
    type Item = &'a u64;
    type IntoIter = ObjectIndexIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl IntoIterator for DeletedObjectIndex {
    type Item = u64;
    type IntoIter = std::vec::IntoIter<u64>;
    fn into_iter(self) -> Self::IntoIter {
        match self.0 {
            None => Vec::new().into_iter(),
            Some(index) => (*index).into_iter(),
        }
    }
}

impl Extend<u64> for DeletedObjectIndex {
    fn extend<I: IntoIterator<Item = u64>>(&mut self, ids: I) {
        for id in ids {
            self.insert(id);
        }
    }
}

impl<'a> Extend<&'a u64> for DeletedObjectIndex {
    fn extend<I: IntoIterator<Item = &'a u64>>(&mut self, ids: I) {
        for id in ids {
            self.insert(*id);
        }
    }
}

impl FromIterator<u64> for DeletedObjectIndex {
    fn from_iter<I: IntoIterator<Item = u64>>(ids: I) -> Self {
        let mut index = DeletedObjectIndex::default();
        index.extend(ids);
        index
    }
}

impl Serialize for DeletedObjectIndex {
    /// The same sequence of ids in the same order, and the same empty sequence when there is
    /// nothing to write -- a bucket carrying no tombstone spells it `[]` exactly as it did when
    /// the field held an empty enum.
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        // ASCENDING, and allocation-free on the arms that hold nothing or one id -- through
        // `ObjectIndex::serialize_ascending`, which is where that rule now lives. This field is
        // serialized, so writing it in slot order would move bytes already on disk for no gain, and
        // it holds a single id on 97.68% of the buckets that carry one at all.
        //
        // NO LONGER `index.serialize(..)`, AND THAT IS THE POINT. The live side's `Serialize` is
        // POSITIONAL now -- slot order, with a null for a placeholder -- because a page entry has to
        // be able to name its object by a slot that survives a load. Nothing names a tombstone by
        // slot, so delegating to it would have moved this field's bytes for nothing. The two rules
        // are two bodies with one caller each, rather than one body serving a claim that split.
        match &self.0 {
            None => {
                use serde::ser::SerializeSeq;
                serializer.serialize_seq(Some(0))?.end()
            }
            Some(index) => index.serialize_ascending(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for DeletedObjectIndex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // An empty sequence loads as no allocation at all, which is the whole point of the
        // shape: the common bucket comes back holding one null pointer.
        Ok(Vec::<u64>::deserialize(deserializer)?.into_iter().collect())
    }
}

/// Keyed by a SHARED block-ref key: the same allocation is held by the lookups that point at this
/// block, instead of each of the three keeping its own copy of the same ~117-byte string.
/// Blocks of one bucket, keyed by an id assigned when the block is filed.
///
/// The key used to be a rendered string of the block's identity and address -- 45.6 B a block, and
/// three quarters of what a block cost on the heap. It was never read as a name: every lookup goes
/// through a ref this map handed out, and a rewrite produces a different key while leaving one
/// entry, so identity comes from the lookup rather than from key equality.
///
/// Serializes as the string map it always was. The key is rebuilt from the value, which carries
/// every part of it, and the handle is recomputed from the block on load.
///
/// The handle is NOT free to choose: the lookup's refs hold handles and the lookup is written to
/// disk, so a handle has to mean the same block in every process that reads the file. Assigning
/// them from a counter compiled, round-tripped, and lost an object on the first reload.
#[derive(Debug, Default, Clone, Deserialize)]
#[serde(try_from = "BTreeMap<String, StoredBlockEntry>")]
pub(super) enum BlockIndexMap {
    /// A bucket holding nothing: no list, no node, no allocation.
    ///
    /// KEPT FOR THE NAME, NOT FOR THE BYTE. An empty `Vec` allocates nothing, so this arm saves no
    /// allocation over `Many(vec![])` -- and it costs no width either, because it rides in the
    /// vector pointer's niche and the enum is exactly as wide as the list it holds. What it buys is
    /// that "this bucket holds nothing" has a name at every site that tests for it, including the
    /// one the release lifecycle assigns. `the_empty_arm_is_an_alias_for_no_blocks_and_costs_nothing_to_keep`
    /// asserts both halves of "free" and the release/reload round trip that depends on it.
    ///
    /// AN ALIAS, NOT A RELEASE MARKER. A released bucket has an empty block index, but what makes it
    /// RELEASED rather than legitimately empty is `released_buckets` plus its retained
    /// `object_index` -- `release_bucket_blocks` says so where it keeps that index. Nothing reads
    /// this discriminant as a lifecycle state.
    #[default]
    Empty,
    /// One block, held behind a POINTER rather than inline.
    ///
    /// THE ARM SURVIVES; WHAT IT COSTS THE NODE DOES NOT. It used to hold the whole entry inline, so
    /// this enum was `8 + size_of::<BlockIndex>()` and every `BucketNode` in the `BucketMap` paid
    /// that width whether or not its bucket held exactly one block. Now the arm is a handle and a
    /// box, the enum is exactly as wide as the list it also holds -- the tag rides in a pointer
    /// niche -- and the single-block case still has a representation of its own.
    ///
    /// WHY THE WIDTH WAS PAYABLE ONLY NOW. #1964 kept the inline entry on an explicit measurement,
    /// and the measurement was sound on the population it sampled: `load_shard` defaulted
    /// `end_routing_bucket` to the whole `u32` keyspace, which gives EVERY KEY A BUCKET OF ITS OWN
    /// BY CONSTRUCTION, so 100% of buckets were single-block and the arm was free of charge there.
    /// #1973 made 1023 the shipped default. At 1,024 buckets the same routed keys FILL them --
    /// measured 4.510% single-block at 4,000 records and 0.000% at 40,000, p50 39 and MAX 52 -- so
    /// the inline width became a toll on 95.5% to 100% of buckets that could never use it.
    ///
    /// AND WHY BOXING RATHER THAN DROPPING THE ARM ALTOGETHER, which recovers the identical 24-byte
    /// width. Dropping it makes a single-block bucket hold a one-entry LIST, and the list's first
    /// block is a whole growth step -- four entries to carry one. Measured over the real bucket
    /// population in both allocator columns: at the shipped range the two are within 13.7 B a bucket
    /// at 4,000 records and identical at 40,000, but on the WHOLE-KEYSPACE range dropping the arm
    /// costs 482.0 B a bucket against boxing's 178.3 and the inline arm's 284.2 -- a REGRESSION of
    /// 197.9 B a bucket against doing nothing. A store records its routing range beside its index
    /// and a load honours that file, so stores built before #1973 still run the wide range. Boxing
    /// is the only one of the three that wins at both. `inline_arm_trade.rs` is that measurement,
    /// four shapes wide.
    ///
    /// WHAT THE ARM STILL COSTS, AND IT IS REAL. One allocation per single-block bucket that the
    /// inline entry did not take, and one DEPENDENT load on every read of such a block -- the entry's
    /// address is not known until the node has been read. Both are measured; dropping the arm would
    /// have paid both too.
    One(u64, Box<BlockIndex>),
    /// Several blocks -- an object with components, or several keys routed to one bucket -- held
    /// as a FLAT LIST SORTED BY HANDLE rather than as a tree.
    ///
    /// WHY A LIST AND NOT A TREE. A `BTreeMap` leaf holds eleven value slots whether or not it
    /// fills them: at 104 bytes a block that is a 1,248-byte node per eleven blocks, and the fill
    /// was measured at 63%. A block list is short, and the operations it actually takes are a
    /// lookup by handle, an ordered walk, and an insert -- none of which needs a tree's
    /// rebalancing. `the_block_index_of_a_real_store_costs_less_as_a_list_than_as_a_tree` measures
    /// what the node costs against what the list costs at the real length distribution, and
    /// `the_block_list_length_distribution_is_reported_as_a_histogram` publishes that distribution.
    ///
    /// SORTED BY HANDLE, AND THAT IS NOT AN IMPLEMENTATION DETAIL. A `BTreeMap<u64, _>` iterates
    /// in ascending key order, and readers of this index depend on that: the index-log item
    /// builder emits blocks in this order, `collect_live_block_entries` materialises them in it,
    /// the storage-topology sampler TRUNCATES at a sample cap so the order decides which blocks
    /// are reported, `bucket_index_shape_for_test` renders it as the comparison between a shard
    /// built by commands and one rebuilt from records, and the whole-scan address lookup in
    /// `bucket_store` takes the FIRST match. Keeping the list sorted by the same key the tree was
    /// keyed by makes every one of those readers see the identical sequence, which is why this is
    /// a container change and not a behaviour change.
    /// `an_unsorted_block_list_would_reorder_every_walk_of_this_index` is the control: it builds
    /// the same blocks in three different orders and asserts one walk, with a negative control
    /// showing that fill order and handle order genuinely differ.
    ///
    /// DUPLICATES ARE REPLACED, NOT APPENDED. A map deduplicated by construction; a list does
    /// not, so `insert_unaccounted` binary-searches and overwrites in place on a hit, returning
    /// the address it displaced so the live tally can discharge it.
    /// `a_second_insert_of_the_same_block_replaces_it_rather_than_adding_beside_it` is the guard.
    Many(Vec<(u64, BlockIndex)>),
}

/// AS WIDE AS THE BLOCK LIST IT HOLDS, AND NO WIDER -- three arms in the space of one vector header.
/// `Empty` and the boxed single-block arm both ride in pointer niches, so there is no discriminant
/// word and no inline payload to be the widest field of `BucketNode` any more.
///
/// 24, not 80, since the single-block case stopped being held INLINE and started being held behind a
/// pointer. That arm was `8 + size_of::<BlockIndex>()` -- a handle plus a whole entry -- and every
/// `BucketNode` in the `BucketMap` paid its width whether or not its bucket held exactly one block.
/// The 56 bytes leave the structure rather than moving into its tail, which is why they take
/// `BucketNode` with them.
///
/// 56 AND NOT 72, BECAUSE #1974 GOT THERE FIRST AND THE TWO CHANGES HARVEST THE SAME FIELD. It took
/// the entry from 88 to 72 by spending one byte on a model spelling drawn from a seventeen-element
/// set instead of a sixteen-byte fat pointer, which took this arm 96 -> 80 and the node 160 -> 144.
/// Whichever of the two landed second found the other's node saving already banked; the node is 88
/// either way. What #1974 keeps after this change is on the HEAP rather than in the node -- a
/// narrower entry makes every boxed arm and every list element smaller.
///
/// AND BOXING THE ARM RECOVERS EXACTLY WHAT REMOVING IT WOULD. Measured: both shapes are 24 bytes,
/// because the tags fit in the pointer niches either way. So the width is not what decides between
/// them -- the heap is, and the measurement is in `inline_arm_trade.rs`.
///
/// ASSERTED AGAINST THE LIST RATHER THAN A LITERAL ALONE. The literal says which number this is;
/// the derivation says WHY, and it is the derivation that fails if the niches ever stop being
/// available -- which is the failure a literal alone would report as "the width moved".
const _: () = assert!(std::mem::size_of::<BlockIndexMap>() == 24);
const _: () = assert!(
    std::mem::size_of::<BlockIndexMap>() == std::mem::size_of::<Vec<(u64, BlockIndex)>>()
);

/// Entries this process has examined looking a block up, counted under `cfg(test)` only.
///
/// THE READ PATH IS WHERE THIS CHANGE IS WON OR LOST -- it trades a tree descent for a walk --
/// and a duration cannot say so on a box that sits at load 40. This counts the entries the
/// lookup actually touched, which repeats to three significant figures and is what the two
/// strategies differ in. Incremented inside [`find_block`], which is the ONE door every lookup
/// goes through, so a test reading it is reading the copy production calls rather than a model
/// of it.
///
/// `cfg(test)` and not a feature: a counter on the read path is exactly what this campaign is
/// removing, and it must not exist in a shipped binary.
#[cfg(test)]
/// HOW MANY ENTRIES A MEMBERSHIP QUESTION IN AN `ObjectIndex` TOUCHES.
///
/// The number an ordinal proposal turns on. `stable_block_object_id` computes an object's identity
/// from its key with a hash; the alternative is to look the object up in the bucket's own short list,
/// and what that COSTS is this count against the hash's byte count -- the two things
/// `engine::hashing::ROUTING_BUCKET_KEY_BYTES` and this counter make comparable.
///
/// `#[cfg(test)]` for the reason given on the derivation counters: the work being counted is smaller
/// than the atomic that would count it in production.
#[cfg(test)]
pub(super) static OBJECT_INDEX_ENTRIES_EXAMINED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// A bisection of `len` touches `floor(log2(len)) + 1` entries, and zero for an empty list. Written
/// out rather than measured inside `binary_search`, which does not report.
#[cfg(test)]
pub(super) fn entries_a_bisection_examines(len: usize) -> u64 {
    if len == 0 {
        0
    } else {
        u64::from(usize::BITS - len.leading_zeros())
    }
}

#[cfg(not(test))]
fn entries_a_bisection_examines(_len: usize) -> u64 {
    0
}

/// How many stored slots named an id another slot in the same array already named.
///
/// PRODUCTION AND NOT `cfg(test)`, unlike the probe counter below: a stored array disagreeing with
/// itself is a thing that happens to a real store and a zero here is a fact worth being able to
/// read. It is a count rather than a refusal because the load path's job is to come up.
static STORED_SLOTS_REPEATED_AN_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

fn note_stored_slots_repeated_an_id(count: u64) {
    STORED_SLOTS_REPEATED_AN_ID.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    tracing::warn!(
        repeated = count,
        "a stored object list named the same object in more than one slot; the first slot wins and          the rest are left as placeholders so nothing above them renumbers"
    );
}

pub(super) fn stored_slots_repeated_an_id() -> u64 {
    STORED_SLOTS_REPEATED_AN_ID.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
fn note_object_index_entries_examined(count: u64) {
    OBJECT_INDEX_ENTRIES_EXAMINED.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(not(test))]
#[inline(always)]
fn note_object_index_entries_examined(_count: u64) {}

#[cfg(test)]
pub(super) fn reset_object_index_entries_examined() {
    OBJECT_INDEX_ENTRIES_EXAMINED.store(0, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
pub(super) fn object_index_entries_examined() -> u64 {
    OBJECT_INDEX_ENTRIES_EXAMINED.load(std::sync::atomic::Ordering::Relaxed)
}

pub(super) static BLOCK_LOOKUP_ENTRIES_EXAMINED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(super) fn block_lookup_entries_examined() -> u64 {
    BLOCK_LOOKUP_ENTRIES_EXAMINED.load(std::sync::atomic::Ordering::Relaxed)
}

#[cfg(test)]
pub(super) fn reset_block_lookup_entries_examined() {
    BLOCK_LOOKUP_ENTRIES_EXAMINED.store(0, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(test)]
fn note_entries_examined(count: usize) {
    BLOCK_LOOKUP_ENTRIES_EXAMINED.fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(not(test))]
#[inline(always)]
fn note_entries_examined(_count: usize) {}

/// Walk a sorted block list from the front. THE STRATEGY THE MEASUREMENT DECLINED, kept for the
/// measurement and compiled only into tests.
///
/// A walk was the obvious candidate here: a block list is short, its entries are contiguous, and a
/// walk has no unpredictable branch. `the_walk_and_the_bisection_cross_over_where_the_measurement_says`
/// counts the entries each strategy touches and the bisection becomes the cheaper of the two at a
/// list of THREE -- while the `Many` arm exists only from TWO, where the two tie exactly. So there
/// is no length this arm ever holds at which the walk is ahead, and the shipped lookup is the
/// bisection. At the measured p50 of 39 the walk touches 20.000 entries a hit against the
/// bisection's 4.538 -- 4.41x, far outside any correction contiguity could make to a count.
///
/// Sorted, so the walk stops at the first handle PAST the one being looked for -- and the
/// position it stops at is the insert position, which is what the bisection's `Err` arm hands
/// back too. Both therefore answer the identical `Result`, which is what lets
/// `the_walk_and_the_bisection_answer_identically_at_every_length` run one against the other.
#[cfg(test)]
pub(super) fn scan_block(blocks: &[(u64, BlockIndex)], key: &u64) -> Result<usize, usize> {
    for (at, (handle, _)) in blocks.iter().enumerate() {
        match handle.cmp(key) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => {
                note_entries_examined(at + 1);
                return Ok(at);
            }
            std::cmp::Ordering::Greater => {
                note_entries_examined(at + 1);
                return Err(at);
            }
        }
    }
    note_entries_examined(blocks.len());
    Err(blocks.len())
}

/// Bisect a sorted block list. THE SHIPPED LOOKUP.
///
/// Written out rather than delegated to `binary_search_by` so the entries it touches can be
/// COUNTED as they are touched. A count derived from `log2(len)` would be a model of the search
/// rather than the search, and this campaign has already had an instrument report a model.
///
/// It also has no cliff: the widest list measured holds 50 blocks, and ten times that is four more
/// probes. A walk at the same length would be five hundred entries, which is the reason a
/// fallback for the tail is not needed here -- the shipped strategy IS the tail's strategy.
pub(super) fn bisect_block(blocks: &[(u64, BlockIndex)], key: &u64) -> Result<usize, usize> {
    let (mut low, mut high) = (0usize, blocks.len());
    let mut examined = 0usize;
    while low < high {
        let mid = low + (high - low) / 2;
        examined += 1;
        match blocks[mid].0.cmp(key) {
            std::cmp::Ordering::Less => low = mid + 1,
            std::cmp::Ordering::Greater => high = mid,
            std::cmp::Ordering::Equal => {
                note_entries_examined(examined);
                return Ok(mid);
            }
        }
    }
    note_entries_examined(examined);
    Err(low)
}

/// Find a handle in a sorted block list.
///
/// THE ONE DOOR. Every lookup, removal and insert into the `Many` arm comes through here, which
/// is what makes the entry counter above a reading of the path production runs rather than of a
/// copy written for the test. It is the bisection because that is what the measurement said --
/// see [`scan_block`] for the strategy it declined and the counts that declined it.
pub(super) fn find_block(blocks: &[(u64, BlockIndex)], key: &u64) -> Result<usize, usize> {
    bisect_block(blocks, key)
}

/// Room for one more block, taken in fixed steps rather than by doubling.
///
/// `Vec`'s own growth doubles: a list of 39 entries takes a capacity of 64, which is a quarter of
/// its bytes unused -- almost exactly the slack the `BTreeMap` node left, so a change that
/// swapped a tree for a doubling vector would have spent the change and bought nothing. Stepping
/// by four caps the waste at three entries, at the price of a reallocation every four inserts on
/// a list that is tens of entries long.
/// `the_block_list_growth_step_is_what_keeps_the_slack_off_the_measurement` measures both sides.
///
/// FOUR AND NOT EIGHT, because most buckets are short. At 4,000 records on the configured range
/// the measured distribution is p50 4, p90 6, MAX 8, and a step of eight would have charged a
/// list of two the same as a list of eight. Four ties `Vec`'s own first block at every length up
/// to eight and wins from twelve upward, which is where the large corpus lives.
pub(super) const BLOCK_LIST_GROWTH_STEP: usize = 4;

fn reserve_one_more(blocks: &mut Vec<(u64, BlockIndex)>) {
    if blocks.len() == blocks.capacity() {
        let want = (blocks.len() + 1).div_ceil(BLOCK_LIST_GROWTH_STEP) * BLOCK_LIST_GROWTH_STEP;
        blocks.reserve_exact(want - blocks.len());
    }
}

/// Iterating a block index, whichever shape it is in.
pub(super) enum BlockIndexIter<'a> {
    Empty,
    One(std::iter::Once<(&'a u64, &'a BlockIndex)>),
    Many(std::slice::Iter<'a, (u64, BlockIndex)>),
}

impl<'a> Iterator for BlockIndexIter<'a> {
    type Item = (&'a u64, &'a BlockIndex);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BlockIndexIter::Empty => None,
            BlockIndexIter::One(once) => once.next(),
            // The list is kept sorted by handle, so this yields the same sequence the tree's
            // iterator did.
            BlockIndexIter::Many(iter) => iter.next().map(|(handle, block)| (handle, block)),
        }
    }
}

pub(super) enum BlockIndexValuesMut<'a> {
    Empty,
    One(std::iter::Once<&'a mut BlockIndex>),
    Many(std::slice::IterMut<'a, (u64, BlockIndex)>),
}

impl<'a> Iterator for BlockIndexValuesMut<'a> {
    type Item = &'a mut BlockIndex;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            BlockIndexValuesMut::Empty => None,
            BlockIndexValuesMut::One(once) => once.next(),
            BlockIndexValuesMut::Many(iter) => iter.next().map(|(_handle, block)| block),
        }
    }
}

/// Live blocks sitting on ONE slab: how many, and how many logical bytes of them.
///
/// `bytes` sums `BlockAddress::length`, which is what every existing per-slab live figure sums --
/// `storage_reclaim_slab_reports` fills `live_physical_bytes` from exactly that. Block refs and
/// bytes are both kept because they answer different questions and neither derives the other: the
/// compaction drain set asks whether ANY block is still there, a garbage fraction asks how MUCH.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct SlabLiveTally {
    pub(super) block_refs: u64,
    pub(super) bytes: u64,
}

/// WHY A BLOCK LEFT THE INDEX, which is the attribution an address cannot carry.
///
/// A FIELD OF THE RANGE AND NOT A TOTAL BESIDE IT. A range that knows why it became garbage is the
/// difference between a discard backlog that can be attributed and one that can only be totalled,
/// and the reason is known exactly where the entry is displaced -- so it is recorded there, with
/// the range, rather than reconstructed later from something that cannot answer it.
///
/// ONE BYTE, so the range is twelve rather than eight. The alternative -- keeping the range at
/// eight and aggregating the cause per index -- was written first and discarded: the aggregate
/// cannot answer "is the garbage on THIS slab rewrite or deletion", which is the question that
/// decides between compacting a slab and waiting for it to die.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum StaleCause {
    /// An index entry was REPLACED by one naming different bytes. The displacing write is already
    /// durable where it landed; these bytes are what the old entry named.
    Overwrite,
    /// An index entry was dropped outright -- a delete, or a block ref removed by name.
    Removed,
    /// An index entry was dropped by a predicate: an expiry pass, a tombstone sweep, a retain.
    Swept,
}

impl StaleCause {
    fn slot(self) -> usize {
        match self {
            StaleCause::Overwrite => 0,
            StaleCause::Removed => 1,
            StaleCause::Swept => 2,
        }
    }
}

/// ONE BYTE RANGE ON ONE SLAB THAT AN INDEX ENTRY STOPPED POINTING AT, AND WHY.
///
/// The offset and length are `u32` because both come out of the address: `BlockAddress` is one word
/// with the slab in the high half and the offset in the low, and `ElementEntry::length` is already
/// a `u32`. Widening either to `u64` would widen the bounded list below without being able to
/// describe anything an address can name.
///
/// THE SLAB IS NOT A FIELD. These live in a map keyed by slab, so carrying it here would store one
/// fact in two places -- the defect the `ElementEntry` notes above describe at length.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StaleRange {
    pub(super) offset: u32,
    pub(super) length: u32,
    pub(super) cause: StaleCause,
}

impl StaleRange {
    /// One past the last byte. Saturating, so a range at the top of the word cannot wrap into
    /// looking like a range at the bottom of it.
    fn end(&self) -> u32 {
        self.offset.saturating_add(self.length)
    }

    /// Too small to be worth discarding on its own, which is the FRAGMENTATION the counts below
    /// report. See [`MIN_DISCARD_RANGE_BYTES`].
    fn is_fragmented(&self) -> bool {
        u64::from(self.length) < MIN_DISCARD_RANGE_BYTES
    }
}

/// THE SMALLEST RANGE WORTH DISCARDING ON ITS OWN, and therefore what makes a range FRAGMENTED.
///
/// A range counts as fragmented when its `length` is below this. A CONSTANT rather than a setting,
/// because a setting read by nothing is a shape this tree has shipped before: it becomes a dial the
/// moment a caller wants a different one, and until then its value has to be defensible on its own.
///
/// FOUR KILOBYTES, which is the page the filesystem hands back. A hole smaller than that cannot
/// return space to the filesystem however it is punched, so a range below it is space that only a
/// COMPACTION -- relocating the live remainder of the slab -- can recover. That is exactly the
/// distinction `compaction_candidate_*` is for, and it is why the threshold is a property of the
/// storage layer and not of the workload.
pub(super) const MIN_DISCARD_RANGE_BYTES: u64 = 4_096;

/// HOW MANY MERGED RANGES ONE SLAB MAY HOLD.
///
/// THE BOUND IS PART OF THE FIRST VERSION AND NOT A FOLLOW-UP. An append-only list of every range
/// an overwrite ever retired grows with the WRITE RATE and never with the data, which is the shape
/// the changefeed in this tree already has; a list like that is worse than no list, because it
/// turns a reporting improvement into a memory leak on the write path.
///
/// THREE THINGS KEEP IT SMALL, and the first is the one that matters. Adjacent and overlapping
/// ranges of the same cause MERGE rather than accumulate, so the count grows with how FRAGMENTED a
/// slab's garbage is and not with how much of it there is: a slab overwritten front to back
/// collapses to one range however many blocks it held. Then the cap below. Then the prune, where a
/// discarded slab's list leaves with it.
///
/// Sixty-four ranges is 768 B of range per slab plus the vector's own block. AT THE CAP THE NEW
/// RANGE IS DROPPED AND COUNTED, NOT EVICTED OVER: every range held is then still exactly a range
/// that retired, and `dropped_ranges` says the detail is incomplete. The alternative -- extending a
/// neighbour to swallow the hole between them -- would claim LIVE bytes as stale, and a figure
/// that over-reports garbage is the one direction a collector must never be given.
pub(super) const STALE_RANGES_PER_SLAB: usize = 64;

/// HOW MANY SLABS MAY CARRY A RANGE LIST AT ONCE.
///
/// The map is pruned where a slab is discarded (`discard_stale_slabs`, called from the reclaim
/// round with the slabs the collector removed or quarantined) and emptied wholesale where the index
/// it describes is rebuilt. This cap is the BACKSTOP for neither of those having happened: a
/// process that never ran a reclaim round still cannot grow this map past 512 entries.
///
/// The victim is the lowest-numbered slab the live tally no longer points at -- the oldest slab
/// this shard has finished with, which is the one a collector takes first and therefore the one
/// whose detail is worth least. Its bytes leave `pending_discard_bytes` and are counted in
/// `discarded_bytes`, so a pruned figure is visibly pruned rather than quietly short.
pub(super) const STALE_SLABS_TRACKED: usize = 512;

/// WHAT ONE SLAB HAS HAD RETIRED OUT OF IT, recorded when the index entry was displaced.
///
/// TWO HALVES WITH DIFFERENT GUARANTEES, DELIBERATELY. `block_refs` and `bytes` are EXACT: every
/// discharge adds to them and nothing but a prune takes anything away, so a ratio standing on
/// `bytes` is standing on recorded fact. `ranges` is BOUNDED detail: capped, merged and droppable,
/// so a range list that overflowed still leaves the byte figure exact. Those are the two things a
/// caller asks for -- how much is dead, and where -- and only the second one can be given up.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct SlabStaleTally {
    /// Index entries retired off this slab. EXACT.
    pub(super) block_refs: u64,
    /// Bytes those entries named, summing `ElementEntry::length` exactly as the live tally does --
    /// so live and stale are in ONE unit and a ratio between them means something. EXACT.
    pub(super) bytes: u64,
    /// The retired ranges, sorted by offset, with adjacent and overlapping ones of the same cause
    /// merged. Bounded by [`STALE_RANGES_PER_SLAB`].
    ranges: Vec<StaleRange>,
    /// Ranges the cap refused. The bytes are still in `bytes` above; only the WHERE was lost.
    dropped_ranges: u32,
}

impl SlabStaleTally {
    /// Charge one retired block, and fold its range into the list.
    ///
    /// MERGING REQUIRES THE SAME CAUSE. Two touching ranges that retired for different reasons are
    /// kept apart, because the cause is a field of the range and a merged range would have to pick
    /// one and lie about the other. It costs ranges only where a slab is being both overwritten and
    /// deleted in the same neighbourhood, and it keeps every range's attribution exact.
    fn record(&mut self, range: StaleRange) {
        self.block_refs = self.block_refs.saturating_add(1);
        self.bytes = self.bytes.saturating_add(u64::from(range.length));
        if range.length == 0 {
            // A zero-length range covers nothing and would be a neighbour of every offset it sat
            // next to, which would make every neighbouring range merge through it. The ref is
            // still charged above, so a zero-length retirement is counted and not located.
            return;
        }
        let at = self.ranges.partition_point(|held| held.offset <= range.offset);
        if at > 0 {
            let left = self.ranges[at - 1];
            if left.cause == range.cause && left.end() >= range.offset {
                let end = left.end().max(range.end());
                self.ranges[at - 1].length = end.saturating_sub(left.offset);
                self.coalesce_from(at - 1);
                return;
            }
        }
        if at < self.ranges.len() {
            let right = self.ranges[at];
            if right.cause == range.cause && range.end() >= right.offset {
                let end = right.end().max(range.end());
                self.ranges[at] = StaleRange {
                    offset: range.offset,
                    length: end.saturating_sub(range.offset),
                    cause: range.cause,
                };
                self.coalesce_from(at);
                return;
            }
        }
        if self.ranges.len() >= STALE_RANGES_PER_SLAB {
            self.dropped_ranges = self.dropped_ranges.saturating_add(1);
            return;
        }
        self.ranges.insert(at, range);
    }

    /// Absorb every following range of the same cause that the one at `at` now reaches.
    ///
    /// A MERGE CAN CLOSE A HOLE. Retiring the block BETWEEN two recorded ranges makes all three one
    /// range, and extending only the left neighbour would leave the right one covering bytes the
    /// left one now also covers -- which would make `merged_discard_range_count` count the same
    /// garbage twice and report fragmentation that had just been removed.
    fn coalesce_from(&mut self, at: usize) {
        while at + 1 < self.ranges.len() {
            let next = self.ranges[at + 1];
            if next.cause != self.ranges[at].cause || self.ranges[at].end() < next.offset {
                break;
            }
            let end = self.ranges[at].end().max(next.end());
            self.ranges[at].length = end.saturating_sub(self.ranges[at].offset);
            self.ranges.remove(at + 1);
        }
    }

    pub(super) fn range_count(&self) -> usize {
        self.ranges.len()
    }

    pub(super) fn dropped_range_count(&self) -> u32 {
        self.dropped_ranges
    }

    pub(super) fn ranges(&self) -> &[StaleRange] {
        &self.ranges
    }
}

/// THE RETIRED SET AS ONE READING, which is what the published counters are made of.
///
/// Every field is derived from the range list in ONE pass, and every byte figure sums
/// `ElementEntry::length` -- the same unit the live tally is in. Nothing here is a file size, and
/// nothing here is derived from `total_known_physical_bytes`, which accumulates before the
/// slab-state match and therefore does not fall when a purge frees space.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct SlabStaleSummary {
    /// Every byte recorded as retired over the window: pending plus already discarded. The sum of
    /// the three cause figures below.
    pub(super) stale_bytes: u64,
    /// Bytes recorded as retired and NOT yet discarded. EXACT, and the term the block-granular
    /// space figure adds to live bytes.
    pub(super) pending_discard_bytes: u64,
    pub(super) pending_discard_block_refs: u64,
    /// Bytes that left this record because their slab was discarded or their detail pruned. The
    /// complement of `pending_discard_bytes` within `stale_bytes`.
    pub(super) discarded_bytes: u64,
    /// `pending_discard_bytes` over `stale_bytes`: how much of what has been retired is still
    /// waiting on the collector. Falls as the collector catches up.
    pub(super) discard_backlog_ppm: u64,
    /// Pending bytes on slabs the live tally no longer points at AT ALL -- a slab wholly dead from
    /// this shard's view, which is what a discard candidate is: the collector can drop it whole.
    pub(super) discard_candidate_bytes: u64,
    pub(super) discard_candidate_slabs: u64,
    /// Merged ranges held, over every tracked slab. Merging happens where the range is RECORDED
    /// rather than in a status pass, so this is also the pending range count.
    pub(super) merged_discard_range_count: u64,
    /// Ranges shorter than [`MIN_DISCARD_RANGE_BYTES`] -- holes too small for a discard to return
    /// space, wherever they are.
    pub(super) fragmented_range_count: u64,
    /// The fragmented ranges on slabs that are still partly LIVE, which are the ones only a
    /// compaction can recover: the slab cannot be dropped whole, and the holes cannot be punched.
    pub(super) compaction_candidate_bytes: u64,
    pub(super) compaction_candidate_range_count: u64,
    pub(super) compaction_candidate_slabs: u64,
    /// The single BEST compaction candidate: its recoverable bytes, and that as a share of
    /// everything on its slab.
    ///
    /// The aggregates above cannot say whether compacting is worth it, because that depends on
    /// CONCENTRATION: 600 KB spread over thirty slabs at 2% each returns almost nothing per
    /// relocation, while the same 600 KB on one slab at 60% returns most of it, and both report
    /// identical totals. This is the figure a minimum-ratio gate would read.
    ///
    /// `recoverable / (live bytes + pending dead bytes)` on the slab with the highest such share.
    /// Zero when there is no compaction candidate at all, which is the same convention the rest of
    /// this struct uses for "not established".
    pub(super) best_compaction_candidate_bytes: u64,
    pub(super) best_compaction_candidate_ratio_ppm: u64,
    /// Ranges the per-slab cap refused. Non-zero means the WHERE is incomplete; the bytes are not.
    pub(super) dropped_range_count: u64,
    pub(super) slabs_tracked: u64,
    pub(super) slabs_pruned: u64,
    /// Bytes retired over the window, by cause. The split is what separates rewrite pressure from a
    /// shrinking live set: the same total means opposite things depending on which it is.
    pub(super) overwrite_bytes: u64,
    pub(super) removed_bytes: u64,
    pub(super) swept_bytes: u64,
}

impl SlabStaleSummary {
    /// Bytes the index has ever pointed at on the slabs it is still tracking: live plus pending
    /// discard. The denominator's companion, and the numerator the space figure wants -- NOT a file
    /// size, so the two halves of the ratio are in one unit.
    pub(super) fn logical_image_bytes(&self, live_bytes: u64) -> u64 {
        live_bytes.saturating_add(self.pending_discard_bytes)
    }

    /// TRUE WHEN SPACE IS PENDING THAT ONLY A COMPACTION CAN RECOVER.
    ///
    /// `compaction_candidate_slabs > 0`. It says a slab is carrying holes too small to punch while
    /// still holding live blocks, so the live remainder has to be relocated before the space comes
    /// back.
    ///
    /// NOT A GATE, AND NOT AN ALERT. On a store that overwrites anything at all this goes true
    /// early and stays true, for the same reason `space_amplification_ppm` cannot be alerted on:
    /// it reports a CONDITION, not a threshold being crossed. What it is for is deciding whether a
    /// compaction round has anything to do -- it is false exactly when every pending byte is on a
    /// slab the collector could drop whole, and then a compaction is wasted work.
    pub(super) fn compaction_recommended(&self) -> bool {
        self.compaction_candidate_slabs > 0
    }
}

/// Per-slab live-block tally, MAINTAINED on every index mutation rather than recomputed by a
/// whole-shard walk.
///
/// WHAT IT COUNTS. Exactly the block set `collect_live_block_entries` returns: every block held in
/// the bucket index, plus -- for a RELEASED bucket -- the blocks that bucket held when it was
/// released. Delete-marked blocks are included, because that walk includes them; a block leaves
/// this tally when its index entry does, not when a flag on it changes.
///
/// WHY THE INDEX AND NOT THE BLOCK STORE. The block store never learns that a block died. It sees
/// appends, and it sees whole slabs arrive and leave; the fact that an index entry stopped
/// pointing at an offset reaches it nowhere. A live-byte figure maintained there could only be
/// recomputed from the index anyway, which is the walk this exists to remove.
///
/// RELEASE IS COUNTER-NEUTRAL, DELIBERATELY. `release_bucket_blocks` empties a bucket block index
/// while its blocks stay live -- they are still in the model maps, and
/// `collect_bucket_index_live_block_entries` supplements them back into the walk. So release does
/// NOT decrement, and `reload_released_bucket` does NOT increment: it re-files the same blocks
/// through `insert_released`. The pair cancels, which is why a released-then-reloaded bucket is
/// one of the workloads the drift check is required to cover rather than one it may assume.
///
/// ON DRIFT, RECOMPUTATION WINS. `reconcile_block_slab_live` compares this against the walk,
/// corrects THIS one, and reports the difference. There is no production assert: a counting bug
/// must not become an outage.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(super) struct BlockSlabLiveIndex {
    by_slab: BTreeMap<u64, SlabLiveTally>,
    /// WHAT LEFT, beside what is still there.
    ///
    /// LIVE AND STALE ARE PEERS, WHICH IS THE STRUCTURAL POINT. Staleness is RECORDED beside
    /// liveness rather than DERIVED from it. Derived is what it was, and `logical_ever - live` is
    /// only available after a storage-manager cycle has published the union tally -- which is why
    /// the space figure could not move at the moment of the overwrite that caused the waste. A
    /// structure that holds what is live and what retired side by side can answer both at once.
    ///
    /// THE SAME FUNNEL RECORDS BOTH, AND THAT IS WHY THEY CANNOT DISAGREE. `remove_address` is the
    /// one place a block leaves this tally, so a stale record written anywhere else could disagree
    /// with the live figure about what retired; written here, there is nothing to disagree with.
    /// The alternative shape -- a sibling field on `CoreIndex` -- would have put a third `&mut` on
    /// `insert_with_handle`, `remove` and `retain` and on every call site that already threads two
    /// disjoint field borrows, to buy nothing this does not have.
    ///
    /// EPHEMERAL, AND THE WINDOW IS PART OF THE READING. The live half is rederivable -- the walk
    /// in `recompute_block_slab_live` IS its definition -- and this half is not: the index knows
    /// only what it still points at, and a displaced extent is exactly what it stopped holding. So
    /// this covers the discharges made SINCE THE INDEX WAS LAST BUILT, and after a reload it is
    /// empty however much garbage the store is carrying. Persisting it would make it a format
    /// change; `control.rs` carries what that costs and what reading it has to allow for.
    stale: BTreeMap<u64, SlabStaleTally>,
    /// Retired bytes by [`StaleCause`], indexed by `StaleCause::slot`. Kept beside the ranges
    /// rather than summed out of them because it must survive a prune: the ranges leave when a slab
    /// is discarded and the window total must not leave with them.
    stale_bytes_by_cause: [u64; 3],
    stale_slabs_pruned: u64,
    stale_bytes_discarded: u64,
    /// False until something has derived this from the index it counts.
    ///
    /// A ShardState arrives from serde with this empty, and an empty tally is indistinguishable
    /// from a shard holding no live blocks at all. Every consumer checks this before believing a
    /// zero, and the fallback is the walk -- so a load path that forgets to seed it costs the old
    /// cost rather than reporting a store made entirely of garbage.
    ready: bool,
}

/// Every charge and discharge this process has made against a live tally.
///
/// THE COST ADDED, as a count rather than as a duration: one map lookup apiece, on the write path.
/// A count is what can be compared against the scan it replaces without a clock, and it is what
/// stays true on a box that is busy.
pub static BLOCK_SLAB_LIVE_CHARGES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

pub fn block_slab_live_charges() -> u64 {
    BLOCK_SLAB_LIVE_CHARGES.load(std::sync::atomic::Ordering::Relaxed)
}

pub fn reset_block_slab_live_charges() {
    BLOCK_SLAB_LIVE_CHARGES.store(0, std::sync::atomic::Ordering::Relaxed);
}

impl BlockSlabLiveIndex {
    pub(super) fn is_ready(&self) -> bool {
        self.ready
    }

    /// Make the tally read as never-derived, so a consumer takes its fallback.
    ///
    /// The A side of the measurement: the same process, the same shard, the same round, with the
    /// maintained tally withheld -- which is the only way to compare the walk against the tally
    /// without also comparing two different corpora.
    #[cfg(test)]
    pub(super) fn forget_for_test(&mut self) {
        self.ready = false;
    }

    pub(super) fn add_address(&mut self, address: &ElementEntry) {
        BLOCK_SLAB_LIVE_CHARGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tally = self.by_slab.entry(address.block_slab_id()).or_default();
        tally.block_refs = tally.block_refs.saturating_add(1);
        tally.bytes = tally.bytes.saturating_add(address.length());
    }

    /// Discharge a block from the live tally AND record the range it retired.
    ///
    /// THE TWO HALVES ARE ONE STATEMENT. A design where staleness is recorded at the call sites that
    /// already decrement this tally has two places that must agree about what left; a design where
    /// the decrement itself records it has one. `cause` is the only thing the call site knows that
    /// this does not, which is why it is the only thing it passes.
    ///
    /// RECORDED EVEN WHEN THE LIVE SIDE FINDS NOTHING TO DISCHARGE. The early return below is for a
    /// slab with no live tally -- an under-derived counter, or a double discharge -- and the block
    /// retired either way. Dropping the stale record with it would make the two disagree in exactly
    /// the case this funnel exists to prevent.
    pub(super) fn remove_address(&mut self, address: &ElementEntry, cause: StaleCause) {
        self.record_stale(address, cause);
        self.remove_address_unrecorded(address);
    }

    /// DISCHARGE WITHOUT RECORDING ANY STALENESS, because nothing retired.
    ///
    /// ONE CALLER, AND THE DISTINCTION IS NOT COSMETIC. A replacement that lands on the SAME entry
    /// -- same slab, same offset, same length -- gives up no bytes: the index still points at them.
    /// The stale record only ever ADDS, so counting that as pending discard would claim live bytes
    /// as garbage. The live tally is different: its charge below is unconditional, so a skipped
    /// discharge leaves the block counted TWICE and the shard believes it holds twice the blocks it
    /// does. The two halves want opposite things from the same event, which is why this is a
    /// separate door rather than an early return in the one above.
    ///
    /// I SHIPPED THE OTHER ARRANGEMENT FIRST and skipped both halves.
    /// `a_second_insert_of_the_same_block_replaces_it_rather_than_adding_beside_it` and
    /// `replacing_a_block_discharges_the_address_it_displaced` both went red and named it exactly:
    /// "a branch that reports none charges without discharging".
    pub(super) fn remove_address_unrecorded(&mut self, address: &ElementEntry) {
        BLOCK_SLAB_LIVE_CHARGES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let Some(tally) = self.by_slab.get_mut(&address.block_slab_id()) else {
            return;
        };
        tally.block_refs = tally.block_refs.saturating_sub(1);
        tally.bytes = tally.bytes.saturating_sub(address.length());
        if tally.block_refs == 0 && tally.bytes == 0 {
            // A slab nothing points at any more is ABSENT, not zero. Keeping the entry would grow
            // this map with every slab the store ever rolled, and the caller that wants a zero for
            // a slab it can name gets one from `tally` regardless.
            self.by_slab.remove(&address.block_slab_id());
        }
    }

    /// Append the retired range, taking the slab cap into account first.
    ///
    /// BOTH HALVES OF THE ADDRESS ARE NARROWED THROUGH `try_from` AND NOT `as`. The address word
    /// holds the slab in its high 32 bits and the offset in its low 32, so neither value can exceed
    /// `u32::MAX` today and the fallback is unreachable -- which is why it saturates to the ceiling
    /// rather than panicking, and why it is written at all: a later address with a wider offset
    /// would otherwise WRAP here and record a range at the wrong end of a slab.
    fn record_stale(&mut self, address: &ElementEntry, cause: StaleCause) {
        let length = u32::try_from(address.length()).unwrap_or(u32::MAX);
        self.stale_bytes_by_cause[cause.slot()] =
            self.stale_bytes_by_cause[cause.slot()].saturating_add(u64::from(length));
        let block_slab_id = address.block_slab_id();
        if self.stale.len() >= STALE_SLABS_TRACKED && !self.stale.contains_key(&block_slab_id) {
            self.prune_one_stale_slab();
        }
        let offset = u32::try_from(address.offset()).unwrap_or(u32::MAX);
        self.stale
            .entry(block_slab_id)
            .or_default()
            .record(StaleRange { offset, length, cause });
    }

    /// Drop the detail for the slab whose detail is worth least: the lowest-numbered one the live
    /// tally no longer points at, or the lowest-numbered one at all if every tracked slab is still
    /// partly live. The backstop for [`STALE_SLABS_TRACKED`], never the ordinary path.
    fn prune_one_stale_slab(&mut self) {
        let victim = self
            .stale
            .keys()
            .copied()
            .find(|block_slab_id| {
                self.by_slab
                    .get(block_slab_id)
                    .is_none_or(|tally| tally.block_refs == 0)
            })
            .or_else(|| self.stale.keys().next().copied());
        let Some(victim) = victim else {
            return;
        };
        self.drop_stale_slab(victim);
    }

    fn drop_stale_slab(&mut self, block_slab_id: u64) {
        if let Some(dropped) = self.stale.remove(&block_slab_id) {
            self.stale_slabs_pruned = self.stale_slabs_pruned.saturating_add(1);
            self.stale_bytes_discarded = self.stale_bytes_discarded.saturating_add(dropped.bytes);
        }
    }

    /// FORGET THE RETIRED SET FOR SLABS THAT HAVE BEEN DISCARDED. The prune, and the reason this map
    /// is bounded by something real rather than only by its cap.
    ///
    /// Called from the reclaim round with the slabs the collector removed or quarantined for
    /// destruction. Those bytes are no longer PENDING a discard -- the discard happened, or is
    /// scheduled and already counted as `reclaimable_physical_bytes` -- so leaving them in
    /// `pending_discard_bytes` would report the same garbage twice in two different fields.
    ///
    /// Counted into `discarded_bytes` rather than silently dropped, so a reader can tell a figure
    /// that fell because garbage was collected from one that fell because detail was discarded.
    pub(super) fn discard_stale_slabs(&mut self, block_slab_ids: impl IntoIterator<Item = u64>) {
        for block_slab_id in block_slab_ids {
            self.drop_stale_slab(block_slab_id);
        }
    }

    pub(super) fn stale_tally(&self, block_slab_id: u64) -> SlabStaleTally {
        self.stale.get(&block_slab_id).cloned().unwrap_or_default()
    }

    pub(super) fn stale_slab_count(&self) -> usize {
        self.stale.len()
    }

    /// The retired set as one reading. O(slabs tracked) and O(ranges held), the same cost class as
    /// every other consumer of this type, and no write-path work at all.
    pub(super) fn stale_summary(&self) -> SlabStaleSummary {
        let overwrite_bytes = self.stale_bytes_by_cause[StaleCause::Overwrite.slot()];
        let removed_bytes = self.stale_bytes_by_cause[StaleCause::Removed.slot()];
        let swept_bytes = self.stale_bytes_by_cause[StaleCause::Swept.slot()];
        let mut summary = SlabStaleSummary {
            stale_bytes: overwrite_bytes
                .saturating_add(removed_bytes)
                .saturating_add(swept_bytes),
            discarded_bytes: self.stale_bytes_discarded,
            slabs_tracked: self.stale.len() as u64,
            slabs_pruned: self.stale_slabs_pruned,
            overwrite_bytes,
            removed_bytes,
            swept_bytes,
            ..SlabStaleSummary::default()
        };
        for (block_slab_id, stale) in self.stale.iter() {
            summary.pending_discard_bytes =
                summary.pending_discard_bytes.saturating_add(stale.bytes);
            summary.pending_discard_block_refs = summary
                .pending_discard_block_refs
                .saturating_add(stale.block_refs);
            summary.merged_discard_range_count = summary
                .merged_discard_range_count
                .saturating_add(stale.range_count() as u64);
            summary.dropped_range_count = summary
                .dropped_range_count
                .saturating_add(u64::from(stale.dropped_range_count()));
            // A SLAB WITH NO LIVE REFS IS WHOLLY DEAD FROM THIS SHARD'S VIEW, which is what the
            // live tally's own "absent, not zero" rule makes an absent entry mean. Live REFS and
            // not live bytes: a delete-marked block is counted by the walk this tally is held to,
            // so its byte figure is not a reliable zero.
            let live_refs = self
                .by_slab
                .get(block_slab_id)
                .map(|tally| tally.block_refs)
                .unwrap_or_default();
            let mut fragmented_bytes = 0_u64;
            let mut fragmented_ranges = 0_u64;
            for range in stale.ranges() {
                if range.is_fragmented() {
                    fragmented_bytes = fragmented_bytes.saturating_add(u64::from(range.length));
                    fragmented_ranges = fragmented_ranges.saturating_add(1);
                }
            }
            summary.fragmented_range_count = summary
                .fragmented_range_count
                .saturating_add(fragmented_ranges);
            if live_refs == 0 {
                summary.discard_candidate_bytes =
                    summary.discard_candidate_bytes.saturating_add(stale.bytes);
                summary.discard_candidate_slabs =
                    summary.discard_candidate_slabs.saturating_add(1);
            } else if fragmented_ranges > 0 {
                // STILL PARTLY LIVE AND CARRYING HOLES TOO SMALL TO PUNCH. The collector cannot
                // drop the slab, and a discard cannot return a sub-page hole, so the live remainder
                // has to be relocated. That is a compaction, and this is its backlog.
                summary.compaction_candidate_bytes =
                    summary.compaction_candidate_bytes.saturating_add(fragmented_bytes);
                summary.compaction_candidate_range_count = summary
                    .compaction_candidate_range_count
                    .saturating_add(fragmented_ranges);
                summary.compaction_candidate_slabs =
                    summary.compaction_candidate_slabs.saturating_add(1);
                // A RUNNING MAXIMUM, not a second pass: both terms are already in hand here.
                //
                // The denominator is this slab's WHOLE content -- the bytes the index still points
                // at plus the bytes it has retired -- so the ratio reads as "how much of this slab
                // comes back if it is compacted". Dividing by the dead bytes alone would report
                // 1.0x for every candidate and rank nothing.
                let live_bytes = self
                    .by_slab
                    .get(block_slab_id)
                    .map(|tally| tally.bytes)
                    .unwrap_or_default();
                let slab_total = live_bytes.saturating_add(stale.bytes);
                if slab_total > 0 {
                    let ratio_ppm = fragmented_bytes.saturating_mul(1_000_000) / slab_total;
                    if ratio_ppm > summary.best_compaction_candidate_ratio_ppm {
                        summary.best_compaction_candidate_ratio_ppm = ratio_ppm;
                        summary.best_compaction_candidate_bytes = fragmented_bytes;
                    }
                }
            }
        }
        summary.discard_backlog_ppm = if summary.stale_bytes == 0 {
            0
        } else {
            summary
                .pending_discard_bytes
                .saturating_mul(1_000_000)
                / summary.stale_bytes
        };
        summary
    }

    pub(super) fn tally(&self, block_slab_id: u64) -> SlabLiveTally {
        self.by_slab.get(&block_slab_id).copied().unwrap_or_default()
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (u64, SlabLiveTally)> + '_ {
        self.by_slab.iter().map(|(id, tally)| (*id, *tally))
    }

    pub(super) fn slab_count(&self) -> usize {
        self.by_slab.len()
    }

    pub(super) fn total_block_refs(&self) -> u64 {
        self.by_slab
            .values()
            .fold(0_u64, |sum, tally| sum.saturating_add(tally.block_refs))
    }

    /// Declare the tally derived, without replacing it.
    ///
    /// For a rebuild that CHARGED every block as it filed it: the tally is already correct and a
    /// walk to confirm that would be the walk this type exists to remove. Callers must have
    /// emptied it first -- `rebuild_bucket_block_ownership` does, right where it clears the map it
    /// counts.
    pub(super) fn mark_ready(&mut self) {
        self.ready = true;
    }

    /// Empty the tally and un-derive it. Pairs with `mark_ready` around a rebuild.
    ///
    /// THE RETIRED SET GOES WITH IT, and the counts of what was pruned go with that. A wholesale
    /// rebuild replaces the index this describes: the retired ranges name offsets in slabs the
    /// rebuild is about to stop pointing at, and a compaction round -- which is what calls this --
    /// has just relocated the live set and handed the old slabs to the collector, so those bytes
    /// have been ACTED ON rather than are pending. Carrying them across would report the same
    /// garbage twice, once here and once as `reclaimable_physical_bytes`.
    ///
    /// On the rebuild paths that are NOT a compaction, this UNDER-reports: real pending discard is
    /// forgotten. That is the safe direction, for the same reason the `ready` flag above gives -- a
    /// figure that understates garbage under-recommends a compaction, where one that overstates it
    /// would recommend discarding bytes that are live.
    pub(super) fn clear(&mut self) {
        self.by_slab.clear();
        self.stale.clear();
        self.stale_bytes_by_cause = [0; 3];
        self.stale_slabs_pruned = 0;
        self.stale_bytes_discarded = 0;
        self.ready = false;
    }

    /// Replace the whole tally and declare it derived.
    ///
    /// For the paths that rebuild the index this counts -- a load, a manifest install, a
    /// bucket-ownership rebuild -- and for the drift check correcting itself.
    ///
    /// THE RETIRED SET IS LEFT ALONE, which is not an oversight. The walk that produces `tallies`
    /// reads the blocks the index STILL POINTS AT, so it has nothing to say about the ones it
    /// stopped pointing at -- a recomputed stale set would be empty by construction, and resetting
    /// to it would delete true records every time the drift check ran. `clear` is how the retired
    /// set is emptied, and it is called where the index is actually replaced.
    pub(super) fn reset_from(&mut self, tallies: BTreeMap<u64, SlabLiveTally>) {
        self.by_slab = tallies;
        self.ready = true;
    }
}

impl BlockIndexMap {
    pub(super) fn get(&self, key: &u64) -> Option<&BlockIndex> {
        match self {
            BlockIndexMap::Empty => None,
            // ONE COMPARISON AND ONE POINTER LOAD, not a bisection. The arm does not enter
            // `find_block`, which is why `BLOCK_LOOKUP_ENTRIES_EXAMINED` counts nothing at all for a
            // single-block bucket.
            BlockIndexMap::One(handle, block) => (handle == key).then_some(&**block),
            BlockIndexMap::Many(blocks) => find_block(blocks, key).ok().map(|at| &blocks[at].1),
        }
    }

    pub(super) fn get_mut(&mut self, key: &u64) -> Option<&mut BlockIndex> {
        match self {
            BlockIndexMap::Empty => None,
            BlockIndexMap::One(handle, block) => (&*handle == key).then_some(&mut **block),
            BlockIndexMap::Many(blocks) => {
                find_block(blocks, key).ok().map(|at| &mut blocks[at].1)
            }
        }
    }

    /// Drop a block, charge the removal to the live tally, and record the range it retired.
    ///
    /// THE CAUSE IS A PARAMETER BECAUSE THE MECHANISM DOES NOT DETERMINE IT, and that was measured
    /// rather than assumed. This door is reached by `drop_live_object_entries`, where the block is
    /// being DELETED, and by `upsert_bucket_index_block_inner`'s supersede, where it is being
    /// OVERWRITTEN -- the same call, two different reasons. A first version labelled the cause from
    /// the call it went through and attributed 2,144,000 B of pure rewrite to deletion;
    /// `the_space_figure_moves_at_the_overwrite` is what said so.
    pub(super) fn remove(
        &mut self,
        key: &u64,
        live: &mut BlockSlabLiveIndex,
        cause: StaleCause,
    ) -> Option<BlockIndex> {
        let removed = self.remove_unaccounted(key);
        if let Some(block) = removed.as_ref() {
            live.remove_address(&block.address, cause);
        }
        removed
    }

    fn remove_unaccounted(&mut self, key: &u64) -> Option<BlockIndex> {
        match self {
            BlockIndexMap::Empty => None,
            BlockIndexMap::One(handle, _) => {
                if handle != key {
                    return None;
                }
                match std::mem::replace(self, BlockIndexMap::Empty) {
                    // The box is UNWRAPPED rather than cloned out of: the caller is handed the entry
                    // it already owned, and the allocation goes back to the allocator with it.
                    BlockIndexMap::One(_, block) => Some(*block),
                    _ => unreachable!("just matched One"),
                }
            }
            BlockIndexMap::Many(blocks) => {
                let removed = find_block(blocks, key)
                    .ok()
                    .map(|at| blocks.remove(at).1);
                self.shrink();
                removed
            }
        }
    }

    /// Install a block, charge it to the live tally, and return its handle.
    ///
    /// A block with the same identity replaces the one already there rather than adding beside it,
    /// which is what the rendered string key used to do by being the key -- so an OVERWRITE both
    /// discharges the address it displaced and charges the new one. Those are different slabs
    /// whenever a rewrite rolled, which is the whole reason a counter has to see the displaced
    /// address rather than assume a replacement is byte-neutral.
    pub(super) fn insert_with_handle(
        &mut self,
        handle: u64,
        block: BlockIndex,
        live: &mut BlockSlabLiveIndex,
    ) -> u64 {
        let address = block.address.clone();
        let (handle, displaced) = self.insert_unaccounted(handle, block);
        if let Some(displaced) = displaced {
            // A REWRITE THAT LANDED ON THE SAME ENTRY RETIRED NOTHING, so it is discharged WITHOUT
            // a stale record. Recording it would make the pending-discard figure count bytes the
            // index still points at, because that record only ever adds; skipping the DISCHARGE as
            // well would leave the block charged twice, because `add_address` below is
            // unconditional. Two halves, opposite requirements, one event -- see
            // `remove_address_unrecorded`, which two existing arms wrote the reason for.
            //
            // Compared on the whole entry rather than on the slab and offset alone, so a block
            // rewritten in place at a DIFFERENT length still records the range it gave up.
            if displaced == address {
                live.remove_address_unrecorded(&displaced);
            } else {
                live.remove_address(&displaced, StaleCause::Overwrite);
            }
        }
        live.add_address(&address);
        handle
    }

    /// Install a block that is ALREADY counted.
    ///
    /// One caller, and it must stay that way: `reload_released_bucket` re-files the blocks a
    /// release took out of the index, and release never discharged them. Counting them here would
    /// double every released bucket the moment it was touched again.
    pub(super) fn insert_released_with_handle(&mut self, handle: u64, block: BlockIndex) -> u64 {
        self.insert_unaccounted(handle, block).0
    }

    /// TEST-ONLY DOOR, AND IT DELEGATES RATHER THAN DUPLICATING.
    ///
    /// The fixtures build a bare `BlockIndexMap` with no bucket around it, so they have nothing to
    /// compute a handle from the bucket level with. This keeps their signature and computes it the
    /// one way it is computed -- `block_index_handle` and then the same install -- so a fixture
    /// cannot exercise a handle derived differently from the one production writes. Two doors, one
    /// implementation; the other arrangement is the second path this change exists to delete.
    #[cfg(test)]
    pub(super) fn insert(
        &mut self,
        object_key: &str,
        block: BlockIndex,
        live: &mut BlockSlabLiveIndex,
    ) -> u64 {
        let handle = block_index_handle(object_key, &block);
        self.insert_with_handle(handle, block, live)
    }

    /// The other test-only door, delegating for the same reason.
    #[cfg(test)]
    pub(super) fn insert_released(&mut self, object_key: &str, block: BlockIndex) -> u64 {
        let handle = block_index_handle(object_key, &block);
        self.insert_released_with_handle(handle, block)
    }

    /// The mechanism, with no accounting: the handle it was GIVEN, and the address it displaced.
    ///
    /// THE HANDLE ARRIVES FROM ABOVE, and that is the whole of this change. It was
    /// `block_index_handle(&page.object_key, &page)` computed right here -- inside the map, from the entry and
    /// nothing else. A map cannot see the sibling fields of the bucket that owns it, so a term
    /// that leaves the entry for a per-bucket structure becomes unreachable from this line, and
    /// every such term was therefore pinned to the entry by this one call.
    ///
    /// Computing it one level up at [`BucketNode::insert_block`] changes WHERE, not WHAT: same
    /// inputs, same order, same hasher, byte-identical `u64`. That matters more than it sounds
    /// like, because handles are written to disk inside the lookup's refs -- two processes holding
    /// the same block must compute the same handle or those refs point at nothing, which is what a
    /// counter did silently until a reload lost an object.
    /// `the_hoisted_handle_is_byte_identical_to_the_one_the_map_used_to_compute` is the check.
    fn insert_unaccounted(&mut self, handle: u64, block: BlockIndex) -> (u64, Option<ElementEntry>) {
        let displaced = match self {
            BlockIndexMap::Empty => {
                // THE ONE ALLOCATION THE INLINE ENTRY DID NOT TAKE, and it is taken here. ONE ENTRY
                // and not a growth step: this is a box, so it is sized for exactly the block it holds.
                // That is the whole of boxing's advantage over dropping the arm, which would put the
                // same page in a list whose first block is four entries -- measured at 482.0 B a
                // bucket against 178.3 on the whole-keyspace range.
                *self = BlockIndexMap::One(handle, Box::new(block));
                None
            }
            BlockIndexMap::One(existing, held) => {
                if *existing == handle {
                    // A rewrite of the same block: the entry is replaced INSIDE the box it already
                    // has, so a rewrite of a single-block bucket allocates nothing at all.
                    Some(std::mem::replace(&mut **held, block).address)
                } else {
                    // A second block: this bucket has earned a list. Built SORTED, because every
                    // walk of this index reads it in handle order.
                    let (first_handle, first) = match std::mem::replace(self, BlockIndexMap::Empty) {
                        BlockIndexMap::One(first_handle, first) => (first_handle, *first),
                        _ => unreachable!("just matched One"),
                    };
                    // ONE STEP, not two. A spill takes the same first block `reserve_one_more`
                    // would have taken, so a list of two, three or four costs exactly what
                    // `Vec`'s own first block costs and the step policy is never behind at the
                    // short lengths where most buckets sit. Taking two steps here made a list of
                    // three cost twice what doubling would have;
                    // `the_block_list_growth_step_is_what_keeps_the_slack_off_the_measurement`
                    // is what said so.
                    let mut blocks = Vec::with_capacity(BLOCK_LIST_GROWTH_STEP);
                    if first_handle < handle {
                        blocks.push((first_handle, first));
                        blocks.push((handle, block));
                    } else {
                        blocks.push((handle, block));
                        blocks.push((first_handle, first));
                    }
                    *self = BlockIndexMap::Many(blocks);
                    None
                }
            }
            BlockIndexMap::Many(blocks) => match find_block(blocks, &handle) {
                // A block with this identity is already filed: overwrite it where it sits. A list
                // does not deduplicate by construction the way the tree did, so this is the only
                // thing standing between a rewrite and a bucket holding the same block twice.
                Ok(at) => Some(std::mem::replace(&mut blocks[at].1, block).address),
                Err(at) => {
                    reserve_one_more(blocks);
                    blocks.insert(at, (handle, block));
                    None
                }
            },
        };
        (handle, displaced)
    }

    /// Give the list back once a bucket no longer needs one.
    ///
    /// TWO COLLAPSES, AND BOTH GIVE AN ALLOCATION BACK. At one entry the list becomes a box, which
    /// trades a buffer sized for a whole growth step for one sized for the entry; at zero it becomes
    /// `Empty`, which gives the buffer back outright. Without the second, a bucket emptied by an
    /// expiry sweep would go on holding a list sized for the fifty blocks it used to have; without
    /// the first, a bucket that briefly held two blocks would keep a four-entry buffer for the rest
    /// of its life -- which is the cost this type exists to avoid.
    fn shrink(&mut self) {
        let len = match self {
            BlockIndexMap::Many(blocks) => blocks.len(),
            _ => return,
        };
        match len {
            0 => *self = BlockIndexMap::Empty,
            1 => {
                let blocks = match std::mem::replace(self, BlockIndexMap::Empty) {
                    BlockIndexMap::Many(blocks) => blocks,
                    _ => unreachable!("just matched Many"),
                };
                let (handle, block) = blocks.into_iter().next().expect("length is one");
                *self = BlockIndexMap::One(handle, Box::new(block));
            }
            _ => {}
        }
    }

    pub(super) fn len(&self) -> usize {
        match self {
            BlockIndexMap::Empty => 0,
            BlockIndexMap::One(..) => 1,
            BlockIndexMap::Many(blocks) => blocks.len(),
        }
    }

    /// What this block index owns on the heap, in REQUEST bytes.
    ///
    /// HERE, AND NOT AT THE CALL SITE, BECAUSE A CALL SITE CAN ONLY NAME A TYPE. The published
    /// index figure used to spell its own arithmetic as `pages * size_of::<BlockIndex>()`, which is
    /// a reading of the ENTRY where two of the three arms hold something else -- and naming a type
    /// at a call site compiles whatever the field became, so it went on answering a plausible
    /// number after the container underneath it changed shape twice. `blocks_per_bucket` charged
    /// `2 * size_of::<Arc<str>>()` for a group that had stopped being a pointer at all for exactly
    /// that reason. Written beside the arms, each term is the `size_of` of the type that arm
    /// actually holds, so a field that changes width moves the figure with it.
    ///
    /// ONE TERM PER ARM, AND THE ARMS GENUINELY DIFFER:
    ///
    ///   * `Empty` owns nothing -- `the_empty_arm_is_an_alias_for_no_blocks_and_costs_nothing_to_keep`
    ///     is the standing assertion that this is still true.
    ///   * `One` owns a `Box<BlockIndex>`. The handle rides in the enum, so this allocation is one
    ///     ENTRY wide and the old per-block charge was exactly right for it -- measured 1.0000
    ///     against the allocator at both corpus sizes on the whole-keyspace range, where every
    ///     bucket is single-block. A blanket move to the pair stride would have OVER-charged that
    ///     whole deployment by 12.5%.
    ///   * `Many` owns `Vec<(u64, BlockIndex)>`. Its element is the PAIR, eight bytes wider than
    ///     the entry, and the buffer is owned at CAPACITY -- `reserve_one_more` steps it by
    ///     `BLOCK_LIST_GROWTH_STEP` with `reserve_exact`, so a list of five owns a buffer of eight.
    ///
    /// WHAT THIS DELIBERATELY DOES NOT CHARGE, ON A MEASUREMENT AND NOT AN OVERSIGHT. `BlockIndex`
    /// holds `object_key: Arc<str>` and `component: Option<Arc<str>>`, whose bytes live off the
    /// entry. They are not added here because this index does not OWN them: over four fixtures --
    /// routed keys and hash fields, at two corpus sizes, at both routing ranges -- every distinct
    /// name allocation had a strong count higher than the number of index entries holding it, and
    /// dropping every block index in the shard returned the block buffers and not one byte of name.
    /// The model maps own them and a read resolves through those, not through here. Charging them
    /// would double-count, and charging them once per ENTRY rather than once per allocation would
    /// over-count a container store's key names by 100x.
    /// `the_resident_index_report_is_measured_against_the_allocator` is that measurement and fails
    /// if the ownership ever moves.
    ///
    /// AND IT IS A FLOOR, NOT AN EQUALITY -- BY A MEASURED AMOUNT, WHICH IS THE POINT. These are
    /// REQUEST bytes: what the container asked the allocator for. `ALLOC_CHUNK_BYTES` reads
    /// `malloc_usable_size`, which is a floor above the request and not an equality (#1969: a
    /// 104-byte request read 128). Modelling a rounding rule into a shipped figure would make it a
    /// claim about the platform's allocator, so this charges the request and the size of what it
    /// misses is MEASURED rather than left unknown. Replaying this index's own allocation size
    /// distribution through the counting allocator:
    ///
    /// ```text
    ///   population                      allocations   chunk/request   this figure / chunk
    ///   routed, whole keyspace, 4,000         4,000         1.2634x                0.7915
    ///   routed, whole keyspace, 40,000       40,000         1.2543x                0.7972
    ///   routed, 0..1023, 4,000                1,024         1.0449x                0.9571
    ///   routed, 0..1023, 40,000               1,024         1.0055x                0.9945
    ///   hash fields, either range, 4,000         40         1.0022x                0.9978
    ///   hash fields, either range, 40,000   350-400    1.0020-1.0022x         0.9978-0.9980
    /// ```
    ///
    /// SO THE ERROR IS SIGNED AND ITS SIZE IS KNOWN: this reads 0.2% to 4.3% below what the
    /// allocator holds wherever the index is list-shaped, and about 20% below on the whole-keyspace
    /// range, where every bucket is one boxed 64-byte entry and glibc's 16-byte rounding is paid
    /// once per block instead of once per list. It is never HIGH. An eviction gate reading this can
    /// treat it as a lower bound on the index's real footprint, which is the direction that makes
    /// it safe to act on.
    pub(super) fn resident_heap_bytes(&self) -> u64 {
        match self {
            BlockIndexMap::Empty => 0,
            // `Box<BlockIndex>`: one entry, and the handle is inline in the enum.
            BlockIndexMap::One(_handle, _block) => std::mem::size_of::<BlockIndex>() as u64,
            // `Vec<(u64, BlockIndex)>`: the element is the pair, and the buffer is owned at the
            // capacity it was reserved to rather than the length that is filled.
            BlockIndexMap::Many(blocks) => (blocks.capacity() as u64)
                .saturating_mul(std::mem::size_of::<(u64, BlockIndex)>() as u64),
        }
    }

    /// Whether this bucket holds no blocks.
    ///
    /// READS THE LENGTH, NOT THE DISCRIMINANT. `Empty` and `Many(vec![])` are both "no blocks", and
    /// `shrink` normalises the second into the first -- but a predicate that trusted the
    /// normalisation would answer `false` for an empty list if any path ever missed it, and the
    /// release lifecycle branches on this. Reading the length makes the two indistinguishable by
    /// construction instead of by convention.
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(super) fn iter(&self) -> BlockIndexIter<'_> {
        match self {
            BlockIndexMap::Empty => BlockIndexIter::Empty,
            BlockIndexMap::One(handle, block) => {
                BlockIndexIter::One(std::iter::once((handle, &**block)))
            }
            BlockIndexMap::Many(blocks) => BlockIndexIter::Many(blocks.iter()),
        }
    }

    pub(super) fn values(&self) -> impl Iterator<Item = &BlockIndex> {
        self.iter().map(|(_handle, block)| block)
    }

    /// Mutable blocks, UNACCOUNTED.
    ///
    /// Named for the contract rather than for the shape, because the shape cannot express it: a
    /// holder of `&mut BlockIndex` can rewrite `address`, and the live tally keys on
    /// `address.block_slab_id` and sums `address.length`. A mutation through here must change
    /// NEITHER. Everything else is fair game -- today's callers set `dirty`, and two tests
    /// deliberately corrupt `object_id` and `routing_bucket`, none of which the tally reads.
    ///
    /// A block that needs to MOVE goes through `insert`, which discharges the address it displaces
    /// and charges the new one.
    ///
    /// This is a NAMED boundary, not a compiler-enforced one: enforcing it would mean making
    /// `BlockIndex::address` private, and it is read in three figures of places.
    /// `the_maintained_slab_live_tally_matches_the_walk` is what fails if the name stops being
    /// obeyed.
    pub(super) fn blocks_mut_unaccounted(&mut self) -> BlockIndexValuesMut<'_> {
        match self {
            BlockIndexMap::Empty => BlockIndexValuesMut::Empty,
            BlockIndexMap::One(_, block) => BlockIndexValuesMut::One(std::iter::once(&mut **block)),
            BlockIndexMap::Many(blocks) => BlockIndexValuesMut::Many(blocks.iter_mut()),
        }
    }

    /// Drop the blocks a predicate rejects, discharging each from the live tally as it goes and
    /// recording the range each one retired.
    ///
    /// `live` comes first so the predicate stays the trailing argument it was, and `cause` follows
    /// it for the same reason. The cause is the PREDICATE'S subject and not this call's: the four
    /// engine-level `retain`s drop a supersede set, a delta's covered keys, an object being deleted
    /// and a block being marked deleted, which are not one reason. See
    /// [`BlockIndexMap::remove`] for the measurement that made the cause a parameter.
    pub(super) fn retain(
        &mut self,
        live: &mut BlockSlabLiveIndex,
        cause: StaleCause,
        mut keep: impl FnMut(&u64, &mut BlockIndex) -> bool,
    ) {
        match self {
            BlockIndexMap::Empty => {}
            BlockIndexMap::One(handle, block) => {
                let handle = *handle;
                if !keep(&handle, block) {
                    live.remove_address(&block.address, cause);
                    *self = BlockIndexMap::Empty;
                }
            }
            BlockIndexMap::Many(blocks) => {
                blocks.retain_mut(|(handle, block)| {
                    let kept = keep(&*handle, block);
                    if !kept {
                        live.remove_address(&block.address, cause);
                    }
                    kept
                });
                self.shrink();
            }
        }
    }
}

impl<'a> IntoIterator for &'a BlockIndexMap {
    type Item = (&'a u64, &'a BlockIndex);
    type IntoIter = BlockIndexIter<'a>;
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Collecting blocks assigns handles, the same as inserting them one at a time.
///
/// THE THIRD SITE WITH NO BUCKET IN SCOPE, AND THE ONE THE HOIST DOES NOT REACH.
///
/// The two mutation doors compute the handle at [`BucketNode::insert_block`], where the bucket's own
/// structures are available. This one cannot: it is how a `BlockIndexMap` arrives from serde, and a
/// `FromIterator` impl is handed pages and nothing else. The enclosing `BucketNode` is being
/// deserialized at the time, so its other fields may not have been read yet -- field order on the
/// wire decides, and the stored form is a map whose keys arrive in whatever order the encoder wrote.
///
/// So a term moved off the entry into a per-bucket structure is not reachable from THIS seam. The
/// handle is therefore still read off the entry on this line: same inputs, same order, same hasher,
/// byte-identical value.
///
/// BUT THIS IS NOT THE SEAM THE LOAD PATH HAS TO USE, AND THAT IS THE IMPORTANT HALF.
/// `BucketNode`'s own visitor accumulates every field as a SEPARATE LOCAL and builds
/// `Ok(BucketNode { .. })` at the end, so `object_index` and the page index are both in scope at
/// that closing construction, order-independently -- which is what a serde map visitor needs, since
/// it cannot assume one key arrives before another.
///
/// WHAT IT COSTS IS A CHANGE OF ACCUMULATION TYPE, not a change of construction site, and the
/// difference is worth stating because the same seam has already been used for a `u32` where moving
/// the construction was enough. A `u32` is STAMPED ONTO entries that already exist. A handle IS THE
/// MAP'S KEY: by the time this impl has run, every entry is already filed under one, and there is
/// nothing left to stamp. So the visitor would hold the STORED form -- a map of rendered key to
/// entry -- and build the page index at the closing construction, where the sibling it needs is in
/// hand. This impl then stays as the standalone door, for deserializing a bare map, which is the
/// same two-doors-one-body shape the install and the written key already have.
/// COLLECTED FROM (IDENTITY, PAGE) PAIRS, NOT FROM PAGES.
///
/// The handle is hashed from the identity and the page, and the identity is leaving the page --
/// so the pair is what this can be built from once it has. Every producer of these pairs has the
/// key in scope already: the decode has it in the stored map key it is iterating, and the
/// fixtures have it in the page they just built.
impl FromIterator<(Arc<str>, BlockIndex)> for BlockIndexMap {
    fn from_iter<I: IntoIterator<Item = (Arc<str>, BlockIndex)>>(blocks: I) -> Self {
        // Unaccounted: this is how a ShardState arrives from serde, and the tally is derived
        // AFTER a load by `reconcile_block_slab_live`. Charging blocks here would count a loaded
        // index twice over -- once on the way in, once when the load seeds the tally.
        let mut map = Self::default();
        for (object_key, block) in blocks {
            let handle = block_index_handle(&object_key, &block);
            map.insert_unaccounted(handle, block);
        }
        map
    }
}

/// THE PAGE INDEX A STORED BUCKET DECODES TO, AND BESIDE IT WHAT EACH PAGE'S IDENTITY IS.
///
/// The map is keyed by a HANDLE, and the handle is hashed from the identity and the page -- so a
/// decode that cannot see an identity cannot even assign the key it files a page under. The
/// identity is read here from the entry, which still carries one; what matters is that it is read
/// ONCE, in one place, and handed both to the handle and to the caller.
///
/// WHY THE CALLER NEEDS IT. `object_block_lookup` is keyed by (kind, object key) and valued by the
/// `(routing_bucket, handle)` of each of that object's pages. It is `skip_serializing` and was
/// rebuilt after every load by walking the decoded entries and reading the identity off each --
/// which is a walk that stops being possible the moment the entry has none. Handed up from here
/// it is filed as the index decodes, from the only pass that ever has both the identity and the
/// handle in scope.
///
/// Built through the same install as a live write, so a loaded index takes the shape a written one
/// does: a bucket that loads a single block must not come back holding a list.
///
/// UNACCOUNTED, as it always was: this is how a `ShardState` arrives from serde and the live tally
/// is derived AFTER a load by `reconcile_block_slab_live`. Charging here would count a loaded index
/// twice over.
fn block_index_and_identities(
    flat: BTreeMap<String, StoredBlockEntry>,
) -> Result<(BlockIndexMap, Vec<(u64, Arc<str>)>), String> {
    let mut map = BlockIndexMap::default();
    let mut identities = Vec::with_capacity(flat.len());
    // INTO_ITER AND NOT INTO_VALUES, WHICH IS THE WHOLE CHANGE ON THIS SIDE. The stored map's keys
    // were thrown away here, which is why #2350 could measure that the characters of a page key
    // are not read back: they reached this loop and were dropped. They are the SOURCE now.
    for (key, stored) in flat {
        let (object_key, block) = stored.into_parts_from_key(&key)?;
        let handle = block_index_handle(&object_key, &block);
        map.insert_unaccounted(handle, block);
        identities.push((handle, object_key));
    }
    Ok((map, identities))
}

/// THE FIXTURE DOOR onto the same builder, for the bare maps that decode with no node around them.
///
/// It discards the identities because it has nobody to hand them to. One implementation and two
/// doors; two builders that agree today is the hazard this file keeps a record of.
///
/// `TryFrom` AND NOT `From`, since the identity comes out of the key. A `From` would have to PANIC
/// on a key it cannot invert, and `#[serde(try_from = ..)]` is what turns the refusal into a decode
/// error instead -- the same `Err`-not-panic rule the rest of this load path follows.
impl TryFrom<BTreeMap<String, StoredBlockEntry>> for BlockIndexMap {
    type Error = String;

    fn try_from(flat: BTreeMap<String, StoredBlockEntry>) -> Result<Self, Self::Error> {
        Ok(block_index_and_identities(flat)?.0)
    }
}

/// THE PAGE INDEX, SERIALIZED AT THE BUCKET LEVEL.
///
/// Holds the whole node rather than just the map, which is the entire reason it exists:
/// `impl Serialize for BlockIndexMap` could only ever see the map, and serde's
/// `serialize(&self)` cannot be handed anything else -- so the written key was pinned to terms
/// living on the entry exactly as the handle was. Composed from the node, a key can draw on
/// anything the bucket owns.
///
/// THE MAP'S OWN `Serialize` IS DELETED RATHER THAN LEFT, and that is not tidying. It had one
/// caller; a second path that agrees with this one today is a second path to keep agreeing, and
/// this tree records what two copies that agree cost when one of them later does not.
///
/// It writes what it always wrote. Hand-written rather than `#[serde(into = ...)]`, which is
/// defined as `T::from(self.clone()).serialize(..)` and duplicates the whole index twice over
/// before a byte is written. And the sort is not incidental: the map this used to convert into was
/// keyed by the rendered string and emitted entries in string order, while this map is keyed by a
/// handle and iterates in hash order, so writing them as they come would reorder every dump.
struct BlockIndexAt<'a> {
    node: &'a BucketNode,
    keys: &'a BlockKeysByPosition<'a>,
}

impl Serialize for BlockIndexAt<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serialize_block_index(self.node, self.keys, serializer)
    }
}

/// THE ENTRY AS IT REACHES THE WIRE, AND IT NO LONGER RESTATES THE KEY IT IS FILED UNDER.
///
/// # THREE KEYS NOW, NOT FIVE, AND THE TWO THAT LEFT WERE THE KEY'S OWN FIELDS
///
/// `object_key` and `model_id` are the key's second and first slots. `block_index_written_key`
/// renders both into the name this entry is filed under, and #2350 measured that the characters of
/// the key were NOT READ BACK by the decoder -- every page key in a 2,000-key index was replaced
/// by the decimal of its handle, fed to the production decoder and re-encoded, and the
/// `slot_index` that came back was byte-identical. So the map held each fact twice and read it
/// once, from the copy that costs 4.4x more.
///
/// WHICH COPY GOES IS NOT A MATTER OF TASTE AND THE INTUITIVE ANSWER IS WRONG. #2350 priced both
/// directions on the shipped container at two key shapes and two store sizes: re-keying the map by
/// the live handle saves 19.6 B/object of STREAM and lands a container 5.1 to 6.8 B/object LARGER
/// at every point, because the composite is the most compressible structure in the index -- sorted,
/// long shared prefixes, repeating decimal runs. The entry side pays. The two are mutually
/// exclusive: exactly one copy of each fact may go.
///
/// # `address` STAYS, AND THAT IS A MEASUREMENT RATHER THAN A SCOPE DECISION
///
/// Its five values are in the key too, at 12.008 B/object against 53.008 as the named object. They
/// cannot come back out of it. `block_index_written_key` renders
/// `block_id().unwrap_or_default()` and `generation().unwrap_or_default()`, and
/// `ElementEntry::block_id()` is `None` EXACTLY while the block is log-resident -- so a staged page
/// and the first page of an object in a slab render ONE key and differ in `log_backed()`, which has
/// production readers.
/// `the_key_a_block_is_filed_under_inverts_exactly::the_presence_bits_of_an_address_are_not_in_the_key_it_renders`
/// builds both and asserts the keys identical. Trimming the address slot-wise instead is blocked
/// independently: `BlockAddressWire` is shared with the index log, which packs it POSITIONALLY.
///
/// # THE FIELD COUNT IS PART OF THE ENCODING
///
/// The served index is msgpack `with_struct_map`, so `serialize_struct` writes a map header of
/// exactly this length -- the same reason `BucketNode` counts its own optional key rather than
/// hard-coding one. Three is therefore a stored-format fact and
/// `SHARD_INDEX_FORMAT_VERSION` 27 is what pays for it.
///
/// THE KEY SET IS OBSERVED, NOT READ OFF THE ATTRIBUTES.
/// `block_entry_names::capture_the_stored_spelling_of_a_block_entry` prints a real encode of an
/// entry and of a whole index, and
/// `block_entry_names::the_stored_spelling_of_a_block_entry_moved_in_exactly_one_slot` pins the three
/// written forms byte for byte.
pub(super) struct BlockEntryAt<'a> {
    pub(super) block: &'a BlockIndex,
}

/// THE ENTRY AS IT ARRIVES FROM THE WIRE, AND THE IDENTITY IS NOT ON IT ANY MORE.
///
/// # IT STILL EXISTS, AND FOR A SHARPER REASON THAN BEFORE
///
/// A derived `Deserialize` on `BlockIndex` would keep COMPILING with the fields gone -- serde's
/// derive ignores a key it has no field for unless `deny_unknown_fields` is set -- so the removal
/// would have taken the identity off the load path without a single error, and
/// `object_block_lookup`, which is `skip_serializing` and rebuilt from what the decode reads,
/// would have come back empty on every load. That is the exact state the encoder refuses to write.
/// This type is what makes the key the ONLY place the decode can look.
///
/// # THE THREE FIELDS ARE THE THREE THE ENTRY WRITES
///
/// No `#[serde(default)]` and no `rename`, so all three keys are required and spelled as declared.
/// AND THE TWO THAT LEFT ARE DELIBERATELY NOT `#[serde(default)]` DECOYS: the derive drops a key it
/// has no field for, so an index written before this change arrives with its `object_key` and
/// `model_id` dropped and the reconstruction reads the SAME values out of the key beside them.
/// `block_entry_names::an_index_written_before_this_change_loads_block_for_block` drives that against
/// committed bytes, field by field.
#[derive(Debug, Clone, Deserialize)]
pub(super) struct StoredBlockEntry {
    pub(super) address: ElementEntry,
    pub(super) dirty: bool,
    pub(super) deleted: bool,
}

impl StoredBlockEntry {
    /// The identity read back out of the key this entry was filed under, and the page beside it.
    ///
    /// # THE KEY IS THE SOURCE NOW, AND IT IS EXACTLY INVERTIBLE FOR THESE TWO TERMS
    ///
    /// `index_log::block_ref_key_parts` is the inverse of the `block_ref_key_from_parts` that wrote
    /// the name: the kind is the leftmost field, the five numbers and the component slot are cut
    /// from the RIGHT, and everything between is the object key -- colons included, which is what
    /// makes an object key holding a colon come back whole.
    ///
    /// # AN `Err` AND NEVER A PANIC
    ///
    /// Every caller is a load path. A key that cannot be inverted, or a kind neither half of
    /// `model_kind_registry` declares, is a decode failure that `load_index` turns into a replay of
    /// the write-ahead log -- slower and correct. A panic here would make the store unopenable,
    /// which is the trade `ObjectRecord::try_from` and `StoredModelKind::deserialize` both state in
    /// as many words.
    pub(super) fn into_parts_from_key(self, key: &str) -> Result<(Arc<str>, BlockIndex), String> {
        let parts = crate::index_log::block_ref_key_parts(key).map_err(|error| error.to_string())?;
        let model_id =
            crate::engine::storage_bucket_internals::StoredModelKind::from_stored_name(parts.kind)
                .ok_or_else(|| {
                    // THE SAME WORDS `StoredModelKind::deserialize` REFUSES WITH, deliberately.
                    // The refusal MOVED when the spelling left the entry -- it used to be the
                    // field's own `Deserialize` -- and the guards that read the message are about
                    // the refusal and not about which door raised it, so the phrase they match on
                    // is kept rather than those guards being loosened.
                    format!(
                        "no stored model kind for model id {:?}, read back out of the stored page \
                         key {key:?}. The kind is that key's leftmost field and the page's own \
                         copy of it is gone, so there is nothing else to read it from -- and \
                         mapping it onto a kind it does not have would file the page under the \
                         wrong object. Declare it in `model_kind_registry`: under `live` if an arm \
                         of `visit_model_live_blocks` emits it, under `retired` -- with the reason \
                         an older index can carry it -- if not.",
                        parts.kind
                    )
                })?;
        let StoredBlockEntry { address, dirty, deleted } = self;
        Ok((
            // ONE ALLOCATION, WHICH IS THE ONE THE REMOVED FIELD COST. The parse borrows out of the
            // key; this is where the borrow becomes the `Arc<str>` the filing map and the
            // identities handed up to the decode share, exactly as the stored field's own
            // `Deserialize` produced one.
            Arc::from(parts.object_key),
            BlockIndex { model_id, address, dirty, deleted },
        ))
    }
}

impl Serialize for BlockEntryAt<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        // NAMED `BlockIndex`, which is what the derive named it. A `serialize_struct` name reaches
        // some formats and not others, and this one must not move either.
        //
        // THREE, NOT FIVE. `object_key` and `model_id` are the key's own second and first slots and
        // the decoder reads them back out of it now; the arity is part of the stored shape, which is
        // what the stamp step pays for.
        let mut entry = serializer.serialize_struct("BlockIndex", 3)?;
        entry.serialize_field("address", &self.block.address)?;
        entry.serialize_field("dirty", &self.block.dirty)?;
        entry.serialize_field("deleted", &self.block.deleted)?;
        entry.end()
    }
}

/// EVERY PAGE THIS BUCKET WRITES, NAMED BY THE IDENTITY HELD BESIDE IT.
///
/// # THE SOURCE IS BESIDE THE PAGE NOW, AND A MISSING ONE IS REFUSED
///
/// This was a CHECK (`check_page_keys_against_their_siblings`) walking the bucket a second time
/// and comparing the key the entry's own copy composes against the key the neighbour composes. It
/// is the SOURCE now: the written key below is composed from the neighbour, and the entry's copy
/// is only still compared because it still exists. One walk rather than two, because a check that
/// has become a source cannot usefully be a separate pass over the same pages.
///
