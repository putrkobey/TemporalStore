// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Storage physical-index / object-manager / feature-page layout report helpers, split from engine.rs.
use super::*;

pub(super) fn storage_object_lifecycle_report(
    shard_id: ShardId,
    shard: &ShardState,
) -> StorageObjectLifecycleReport {
    storage_object_lifecycle_report_for_buckets(shard_id, shard, &BTreeSet::new(), |_| 0)
}

pub(super) fn storage_object_lifecycle_report_for_buckets(
    shard_id: ShardId,
    shard: &ShardState,
    selected_buckets: &BTreeSet<u32>,
    routing_bucket_for_key: impl Fn(&str) -> u32,
) -> StorageObjectLifecycleReport {
    object_lifecycle_report_from_entries(
        shard_id,
        shard,
        collect_live_block_entries(shard),
        selected_buckets,
        routing_bucket_for_key,
    )
}

/// WHERE THE INDEX BESIDE THESE MAPS FILES EACH KEY, so a model-map walk can be scoped by bucket.
///
/// A model-map walk knows no filing, and the bucket a key's pages belong to is the one the bucket
/// index files them under -- NOT a hash over whatever range the reading process runs on. That
/// distinction is the whole of a cross-range manifest install: a restored dump image was written
/// under the SOURCE shard's range, so deriving the bucket from the reader's range partitions the same
/// store two different ways and the comparison finds nothing in common. The address used to carry the
/// source's bucket and answered this for free; the index that ships with the image answers it now.
fn filing_by_object_key(shard: &ShardState) -> HashMap<Arc<str>, u32> {
    let mut filed = HashMap::new();
    // THE IDENTITY COMES FROM BESIDE THE PAGE, not off it -- one inversion of the object lookup
    // for the whole walk, which is the same order as the walk. A page whose neighbours do not name
    // it is skipped: the one such page is a tombstone whose removal named no element, and a filing
    // map keyed by object cannot state an entry for an object it cannot name.
    let identities = shard.bucket_index.block_identities();
    for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
        for (handle, block) in bucket.block_index.iter() {
            let Some(beside) = identities.identity_of(bucket, *handle, block) else {
                continue;
            };
            filed.insert(Arc::clone(beside.object_key), *routing_bucket);
        }
    }
    filed
}

/// Same object-lifecycle report, but derived from the secondary model maps
/// (strings/hashes/feature series/...) instead of the bucket index. Used to detect
/// a bucket-dump manifest whose serialized model maps disagree with its bucket index
/// (e.g. a mutated object_id): the bucket-index-derived report alone stays valid.
pub(super) fn storage_object_lifecycle_report_for_buckets_from_model_maps(
    shard_id: ShardId,
    shard: &ShardState,
    selected_buckets: &BTreeSet<u32>,
    routing_bucket_for_key: impl Fn(&str) -> u32,
) -> StorageObjectLifecycleReport {
    let filed = filing_by_object_key(shard);
    object_lifecycle_report_from_entries(
        shard_id,
        shard,
        collect_model_live_block_entries(shard),
        selected_buckets,
        |key| {
            filed
                .get(key)
                .copied()
                .unwrap_or_else(|| routing_bucket_for_key(key))
        },
    )
}

pub(super) fn object_lifecycle_report_from_entries(
    shard_id: ShardId,
    shard: &ShardState,
    entries: Vec<LiveBlockEntry>,
    selected_buckets: &BTreeSet<u32>,
    routing_bucket_for_key: impl Fn(&str) -> u32,
) -> StorageObjectLifecycleReport {
    // THE FILING FIRST, THE DERIVATION AS THE FALLBACK, and `selected_buckets` is why. That set
    // comes from OUTSIDE this shard -- a dump manifest's bucket ids, an eviction round's victims --
    // so the bucket each entry is tested against has to be the bucket the entry's own CONTAINER
    // files it under. Deriving it from the key over the reader's range answers a different question
    // whenever the two ranges differ, which on a cross-range manifest install matched nothing at
    // all. The fallback is for an entry from a model-map walk, where there is no filing to read.
    let entries = entries
        .into_iter()
        .filter(|entry| {
            let routing_bucket = entry
                .filed_bucket()
                .unwrap_or_else(|| routing_bucket_for_key(&entry.object_key));
            selected_buckets.is_empty() || selected_buckets.contains(&routing_bucket)
        })
        .collect::<Vec<_>>();
    let mut expected_object_ids = BTreeSet::new();
    let mut actual_object_owners = BTreeMap::<u64, BTreeSet<u64>>::new();
    let mut missing_owner_block_refs = 0u64;
    let mut owner_mismatch_block_refs = 0u64;

    for entry in &entries {
        let expected_object_id = expected_live_block_object_id(shard_id, entry);
        expected_object_ids.insert(expected_object_id);
        // THE OBJECT ID IS THE WHOLE OF "OWNER" NOW. This counted a page whose address named
        // neither its object nor its bucket, and the bucket half has stopped being answerable HERE:
        // a bucket-index walk knows the filing for every page, so the term would be vacuous, and a
        // MODEL-MAP walk knows it for none, so the term would be universal. Either way it reports a
        // property of the walk rather than of the page -- and the two derivations this report is
        // compared against are one of each, so keeping it made a consistent dump refuse to install.
        //
        // AND THE OTHER HALF HAS STOPPED BEING ANSWERABLE HERE TOO, for the same reason one level
        // down: it asked whether the address NAMED its object, and an address names nothing now. Both
        // `missing_owner_block_refs` and `owner_mismatch_block_refs` had that as their only
        // remaining term, so both are structurally zero from THIS report. They are left in the
        // shape because the aggregate in `reports.rs` also sums the validation path, which still
        // has a term that fires -- `!bucket_block_present`. What is gone is this report's
        // contribution, and it is gone because it could only have compared a value with itself.
        //
        // REUSE IS STILL A REAL QUESTION AND IS NOW ASKED OF THE DERIVATION. Two DISTINCT (kind,
        // key) pairs hashing to one id is a collision the store would serve wrong, and it is
        // detectable without a second stored copy: derive per pair and look for an id reached by
        // more than one pair. That is a check that can fire, which the one it replaces could not.
        actual_object_owners
            .entry(expected_object_id)
            .or_default()
            .insert(stable_object_hash(&format!(
                "{}:{}",
                entry.kind.as_str(),
                entry.object_key
            )));
    }

    let reused_object_ids = actual_object_owners
        .into_iter()
        .filter_map(|(object_id, distinct_terms)| {
            (distinct_terms.len() > 1).then_some(object_id)
        })
        .collect::<Vec<_>>();
    let delete_marked_object_keys = shard
        .dirty_objects
        .iter()
        .filter(|key| {
            let routing_bucket = routing_bucket_for_key(key);
            (selected_buckets.is_empty() || selected_buckets.contains(&routing_bucket))
                && !record_exists(shard, key)
        })
        .map(String::from)
        .collect::<Vec<_>>();

    StorageObjectLifecycleReport {
        live_object_ids: expected_object_ids.len() as u64,
        live_block_refs: entries.len() as u64,
        stale_object_ids: 0,
        delete_marked_object_ids: delete_marked_object_keys.len() as u64,
        reused_object_id_conflicts: reused_object_ids.len() as u64,
        missing_owner_block_refs,
        owner_mismatch_block_refs,
        reused_object_ids,
        delete_marked_object_keys,
    }
}

pub(super) fn bucket_dump_entries_by_key(
    shard_id: ShardId,
    shard: &ShardState,
    selected_buckets: &BTreeSet<u32>,
    routing_bucket_for_key: impl Fn(&str) -> u32,
) -> BTreeMap<String, ElementEntry> {
    collect_live_block_entries(shard)
        .into_iter()
        .filter(|entry| {
            // The filing first -- see `object_lifecycle_report_from_entries` for why a set that came
            // from outside this shard has to be tested against the container's own answer.
            let routing_bucket = entry
                .filed_bucket()
                .unwrap_or_else(|| routing_bucket_for_key(&entry.object_key));
            selected_buckets.is_empty() || selected_buckets.contains(&routing_bucket)
        })
        .map(|entry| {
            let component = entry.component.unwrap_or_default();
            let block_id = entry.address.block_id().unwrap_or_else(|| {
                stable_block_object_id(
                    shard_id,
                    entry.kind.as_str(),
                    &entry.object_key,
                )
            });
            (
                format!(
                    "{}:{}:{}:{}",
                    entry.kind, entry.object_key, component, block_id
                ),
                entry.address,
            )
        })
        .collect()
}

/// How many entries the dirty half of [`bucket_storage_summaries`] looks at, across every call.
///
/// The cost of that half is not visible from outside the function, and deriving it as "one per
/// dirty object" is arithmetic about the code rather than a measurement of it. This counts what
/// actually happens, so the guard keeps being true after someone changes the loop.
pub(crate) static DIRTY_SUMMARY_VISITS: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// WHICH ROUTING BUCKETS RECORD DIRT ON THEIR OWN NODE -- the dirty-BUCKET question, answered
/// without the object-keyed set.
///
/// `BucketStorageSummary::dirty_object_count` is populated from one place, the
/// `dirty_objects.bucket_counts()` loop at the foot of [`bucket_storage_summaries`], and several
/// consumers only ever asked it `> 0`. That is a question about the BUCKET, and the bucket already
/// records the answer: `mark_object_dirty` CREATES the routing bucket's node when the key has none
/// and sets `dirty` on it, so even a key with no page at all leaves a node reading dirty.
///
/// The two records are independent, not one cached from the other, and that is measured rather
/// than argued: `whether_a_bucket_can_count_its_own_dirty_objects` neutralises each of the three
/// sites that maintain this flag in turn, and the arm with BOTH set-derived OR terms gone and the
/// mark kept still agrees with the set on every bucket -- including the ones carrying dirt no page
/// walk can see. So a consumer reading this survives the set being retired.
///
/// THE COUNT IS A DIFFERENT QUESTION AND IS NOT ANSWERED HERE. It is not derivable from the
/// bucket: the same measurement finds a page walk reporting 3 of 11 marks and reading 5 of 8 dirty
/// buckets as clean, at 17x the producer's work. `bucket_storage_summaries` goes on reading the
/// set for the count, and that pin is asserted there.
///
/// A bucket this set does not name reads the same either way: a summary can exist for a bucket
/// `bucket_map` does not hold, and such a bucket has no node to mark, so the set loop contributes
/// nothing to its count and `dirty_object_count > 0` was already false for it.
pub(super) fn dirty_bucket_nodes(shard: &ShardState) -> BTreeSet<u32> {
    shard
        .bucket_index
        .bucket_map
        .iter()
        .filter(|(_routing_bucket, bucket)| bucket.dirty())
        .map(|(routing_bucket, _bucket)| *routing_bucket)
        .collect()
}

pub(super) fn bucket_storage_summaries(
    shard: &ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> Vec<BucketStorageSummary> {
    let mut buckets = BTreeMap::<u32, BucketStorageSummary>::new();
    let mut block_slabs_by_bucket = BTreeMap::<u32, BTreeSet<u64>>::new();
    for entry in collect_live_block_entries(shard) {
        // The shard's OWN routing range, which is what every other consumer of this fallback
        // uses -- `rebuild_bucket_first_index`, `refresh_pending_bucket_runtime_flags` and the
        // dirty-key loop at the foot of this function all reach for
        // `block_routing_bucket(key, start, end)`. This site reached for `bucket_for_object(key,
        // 0, u32::MAX)` instead, which is the same answer only while the shard spans the whole
        // range. Narrow the range -- `TS_SHARD_END_ROUTING_SLOT=1023` is the setting that cuts
        // resident memory 45% -- and the two place the same page in different buckets: this one
        // in a bucket id above the shard's own end, no other component in agreement, and so a
        // summary for a bucket `bucket_map` does not hold while the bucket that does hold the
        // page reports no pages at all. A dump naming that bucket then carries no slabs for it.
        //
        // THE WALK'S OWN FILING FIRST, THE DERIVATION AS THE FALLBACK -- and this site is the one
        // that proves why the order matters.
        //
        // The address used to carry the bucket and this read was
        // `address.routing_bucket().unwrap_or_else(derive)`. Replacing it with the derivation ALONE
        // drops the container's answer rather than moving to it, and for a MANIFEST INSTALL that is
        // the wrong question: the embedded index is the SOURCE shard's `bucket_map`, written under the
        // SOURCE's routing range, so deriving over the reader's range names a bucket the manifest
        // never mentions. The object-count loop below reads `bucket_map` directly, so it kept
        // answering the container's question -- which is how a summary came to read `object_count: 1`
        // beside `block_ref_count: 0` and refuse every cross-range install with
        // `slot_dump_slot_summary_mismatch`. One half of one summary asking a different question from
        // the other half.
        //
        // The fallback is for an entry from a MODEL-MAP walk, where there is no filing to read: the
        // map holds pages by key and kind and knows nothing about buckets. That is the only shape the
        // derivation is right for here.
        let routing_bucket = entry.filed_bucket().unwrap_or_else(|| {
            block_routing_bucket(&entry.object_key, start_routing_bucket, end_routing_bucket)
        });
        let summary = buckets.entry(routing_bucket).or_insert(BucketStorageSummary {
            routing_bucket,
            ..BucketStorageSummary::default()
        });
        summary.block_ref_count = summary.block_ref_count.saturating_add(1);
        summary.physical_bytes = summary.physical_bytes.saturating_add(entry.address.length());
        summary.logical_bytes = summary.logical_bytes.saturating_add(entry.address.length());
        // Record which page slab backs each bucket so bucket-dump manifests carry the
        // live slab set (used by manifest validation and the dump/copy path).
        // Without this the map stayed empty and every summary reported no slabs.
        block_slabs_by_bucket
            .entry(routing_bucket)
            .or_default()
            .insert(entry.address.block_slab_id());
        if let Some(stored_slab_id) = entry.address.slab_id() {
            summary.last_compacted_slab = Some(
                summary
                    .last_compacted_slab
                    .map_or(stored_slab_id, |current| current.max(stored_slab_id)),
            );
        }
    }
    for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
        // object_count and the base dirty_generation are durable-generation IDENTITY
        // fields (compared by bucket_dump_summary_matches_current_generation to anchor
        // WAL/index reclaim). They must reflect durable bucket content, not the
        // transient `dirty` flag -- so a bucket that was dumped and then had its dirty
        // flag cleared but still owns live pages keeps its
        // reclaim fingerprint. Populate whenever the bucket is dirty OR already has a
        // live-page summary; behaviour is unchanged for every pre-clear state (a bucket
        // with content is dirty today, so the branch was always taken).
        let has_live_summary = buckets.contains_key(routing_bucket);
        if !bucket.dirty() && !has_live_summary {
            continue;
        }
        let summary = buckets.entry(*routing_bucket).or_insert(BucketStorageSummary {
            routing_bucket: *routing_bucket,
            ..BucketStorageSummary::default()
        });
        // object_index.len() is the bucket's total object count, not its dirty count;
        // assigning it to dirty_object_count double-counted (the dirty_objects loop
        // below is the authoritative per-key dirty tally).
        summary.object_count = bucket.object_index.object_count() as u64;
        summary.dirty_generation = bucket.dirty_generation;
    }
    // ONE ITERATION PER DIRTY BUCKET, where this was one per dirty OBJECT.
    //
    // The loop this replaces walked every dirty object key and hashed it to recompute a routing
    // bucket, to add 1 to a per-bucket counter. That is a fold whose group key was known when the
    // object was marked dirty and thrown away; `DirtyObjectIndex` keeps it, so the fold is
    // already done. Same arithmetic -- adding 1 N times and adding N are the same number well
    // below saturation -- over a loop bounded by the bucket count instead of the corpus.
    //
    // This function runs three times in one `apply_storage_lifecycle` and again on each metrics
    // scrape, so the per-key hash was paid four times a round over a set that grows with ingest.
    for (routing_bucket, dirty_object_count) in shard.dirty_objects.bucket_counts() {
        DIRTY_SUMMARY_VISITS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let summary = buckets.entry(routing_bucket).or_insert(BucketStorageSummary {
            routing_bucket,
            ..BucketStorageSummary::default()
        });
        summary.dirty_object_count = summary
            .dirty_object_count
            .saturating_add(dirty_object_count);
        summary.dirty_generation = summary.dirty_generation.saturating_add(dirty_object_count);
    }
    for (routing_bucket, summary) in &mut buckets {
        summary.block_slab_ids = block_slabs_by_bucket
            .get(routing_bucket)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();
    }
    buckets.into_values().collect()
}

const NATIVE_PACKED_BLOCK_INDEX_SIZE: usize = 17;
const NATIVE_PACKED_BUCKET_NODE_SIZE: usize = 24;

pub(super) fn physical_address_word(address: &ElementEntry) -> u64 {
    address.block_slab_id().wrapping_shl(32) | (address.offset() & u32::MAX as u64)
}

pub(super) fn native_packed_block_index_bytes(
    block: &StoragePhysicalBlockIndex,
) -> [u8; NATIVE_PACKED_BLOCK_INDEX_SIZE] {
    let mut bytes = [0u8; NATIVE_PACKED_BLOCK_INDEX_SIZE];
    bytes[0] = block.object_id.unwrap_or_default() as u8;
    bytes[1] = model_report_code(block.model_id.as_str());
    bytes[2..4].copy_from_slice(&(block.block_id.unwrap_or_default() as u16).to_le_bytes());
    bytes[4] = u8::from(block.dirty) | (u8::from(block.log_backed) << 1);
    let page_size = if block.deleted { 0 } else { block.length as u32 };
    bytes[5..9].copy_from_slice(&page_size.to_le_bytes());
    let address = physical_address_word(&ElementEntry::from_parts(block.block_slab_id, block.offset, block.length, block.block_id, block.object_id));
    bytes[9..17].copy_from_slice(&address.to_le_bytes());
    bytes
}

pub(super) fn native_packed_bucket_node_bytes(bucket: &StoragePhysicalBucketNode) -> [u8; NATIVE_PACKED_BUCKET_NODE_SIZE] {
    let mut bytes = [0u8; NATIVE_PACKED_BUCKET_NODE_SIZE];
    let block_in_log = bucket.block_indexes.iter().any(|block| block.log_backed);
    let trivial_block = bucket.block_ref_count <= 1;
    let block_deleted = bucket.block_ref_count == 0;
    let mut flags = 0u32;
    flags |= (bucket.ttl_ms.is_some() as u32) << 1;
    flags |= (bucket.dirty as u32) << 2;
    flags |= (bucket.loading as u32) << 4;
    flags |= (bucket.in_memory as u32) << 5;
    flags |= (bucket.dirty as u32) << 6;
    flags |= (block_deleted as u32) << 7;
    flags |= (block_in_log as u32) << 8;
    flags |= (trivial_block as u32) << 9;
    let flag_bytes = flags.to_le_bytes();
    bytes[0..3].copy_from_slice(&flag_bytes[0..3]);
    bytes[3..7].copy_from_slice(&(bucket.physical_bytes as u32).to_le_bytes());
    // 0 HERE MEANS THE BUCKET NAMES NO PAGE, and now it means only that. Every declared kind
    // packs as a non-zero code (`model_kind_registry` asserts it), and a kind the registry does
    // not declare refuses rather than landing on this value -- so a reader can tell an empty
    // bucket from one holding a zset, which it could not before.
    let model_code = bucket
        .block_indexes
        .first()
        .map(|block| model_report_code(block.model_id.as_str()))
        .unwrap_or_default();
    bytes[7] = model_code;
    bytes[8..16].copy_from_slice(&bucket.ttl_ms.unwrap_or_default().to_le_bytes());
    let address = bucket
        .block_indexes
        .first()
        .map(|block| block.block_slab_id.wrapping_shl(32) | (block.offset & u32::MAX as u64))
        .unwrap_or_default();
    bytes[16..24].copy_from_slice(&address.to_le_bytes());
    bytes
}

pub(super) fn storage_physical_index_report(
    shard_id: ShardId,
    shard: &ShardState,
    summaries: Vec<BucketStorageSummary>,
) -> StoragePhysicalIndexReport {
    let summary_by_bucket = summaries
        .into_iter()
        .map(|summary| (summary.routing_bucket, summary))
        .collect::<BTreeMap<_, _>>();
    let mut buckets = summary_by_bucket
        .iter()
        .map(|(routing_bucket, summary)| {
            (
                *routing_bucket,
                StoragePhysicalBucketNode {
                    routing_bucket: *routing_bucket,
                    layout: "empty".to_string(),
                    // THE BUCKET'S OWN FLAG, not `dirty_object_count > 0`. This report field is a
                    // dirty-BUCKET flag, and it was deriving one from a count of objects. The node
                    // records it directly -- see `dirty_bucket_nodes` for why the two are
                    // independent records of the same fact rather than one read off the other.
                    dirty: shard
                        .bucket_index
                        .bucket_map
                        .get(routing_bucket)
                        .map(|bucket| bucket.dirty())
                        .unwrap_or(false),
                    meta_loaded: true,
                    loading: false,
                    in_memory: summary.block_ref_count > 0,
                    ttl_ms: None,
                    object_count: summary.object_count,
                    block_ref_count: summary.block_ref_count,
                    logical_bytes: summary.logical_bytes,
                    physical_bytes: summary.physical_bytes,
                    dirty_generation: summary.dirty_generation,
                    last_dump_sequence: summary.last_dump_sequence,
                    native_packed_bucket_node_len: NATIVE_PACKED_BUCKET_NODE_SIZE,
                    native_packed_bucket_node_hex: String::new(),
                    block_indexes: Vec::new(),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();

    // WHAT "MISSING" MEANS NOW. It used to count pages whose ADDRESS carried no routing bucket --
    // a page rebuilt from a source that did not stamp one. An address carries none by construction
    // today, so counting that would count every page. What is still answerable, and is the same
    // question the count was asked for, is a page the INDEX cannot say the filing of: an entry from
    // a model-map walk, where there is no filing to report.
    let mut missing_routing_bucket_count = 0usize;
    for entry in collect_live_block_entries(shard) {
        if entry.filed_bucket().is_none() {
            missing_routing_bucket_count = missing_routing_bucket_count.saturating_add(1);
        }
        let routing_bucket = entry
            .filed_bucket()
            .unwrap_or_else(|| bucket_for_object(&entry.object_key, 0, u32::MAX));
        let bucket = buckets
            .entry(routing_bucket)
            .or_insert(StoragePhysicalBucketNode {
                routing_bucket,
                layout: "empty".to_string(),
                meta_loaded: true,
                in_memory: true,
                native_packed_bucket_node_len: NATIVE_PACKED_BUCKET_NODE_SIZE,
                ..StoragePhysicalBucketNode::default()
            });
        let mut block_index = StoragePhysicalBlockIndex {
            object_key: entry.object_key.clone().to_string(),
            model_id: entry.kind.clone().to_string(),
            component: entry.component.clone().map(|value| value.to_string()),
            routing_bucket,
            block_slab_id: entry.address.block_slab_id(),
            offset: entry.address.offset(),
            length: entry.address.length(),
            block_id: entry.address.block_id(),
            object_id: Some(expected_live_block_object_id(shard_id, &entry)),
            stored_slab_id: entry.address.slab_id(),
            // The index does not hold a digest; a caller wanting one reads the page.
            checksum: None,
            dirty: entry.dirty,
            deleted: entry.deleted,
            log_backed: entry.log_backed,
            native_packed_block_index_len: NATIVE_PACKED_BLOCK_INDEX_SIZE,
            native_packed_block_index_hex: String::new(),
        };
        block_index.native_packed_block_index_hex =
            hex::encode(native_packed_block_index_bytes(&block_index));
        bucket.block_indexes.push(block_index);
    }
    for (routing_bucket, runtime_bucket) in &shard.bucket_index.bucket_map {
        let bucket = buckets
            .entry(*routing_bucket)
            .or_insert(StoragePhysicalBucketNode {
                routing_bucket: *routing_bucket,
                native_packed_bucket_node_len: NATIVE_PACKED_BUCKET_NODE_SIZE,
                ..StoragePhysicalBucketNode::default()
            });
        bucket.layout = bucket_layout_name(runtime_bucket.layout).to_string();
        bucket.dirty = runtime_bucket.dirty();
        bucket.meta_loaded = runtime_bucket.meta_loaded();
        bucket.loading = runtime_bucket.loading();
        bucket.in_memory = runtime_bucket.in_memory();
        bucket.ttl_ms = runtime_bucket.ttl_ms.ms();
        bucket.object_count = runtime_bucket.object_index.object_count() as u64;
        bucket.block_ref_count = runtime_bucket.block_index.len() as u64;
        bucket.dirty_generation = runtime_bucket.dirty_generation;
        // `last_dump_sequence` IS NOT OVERWRITTEN FROM THE NODE HERE, and that is the whole of
        // this report's change. The value the row already carries came from the summary above,
        // which `merge_last_dump_sequence` fills from the NEWEST dump manifest -- so the report
        // now answers "is this bucket covered by the newest dump, and at what index-log
        // sequence", the one figure a decision in this engine actually reads (as the dump
        // ordering's tiebreaker). The node's own figure was a different number under the same
        // name, `max(manifest.wal_sequence)` over every manifest that ever named the bucket, and
        // nothing read it except this line and the bucket-store runtime report.
        //
        // WHAT IS LOST, stated rather than glossed: a bucket named by an older manifest and not
        // by the newest one reported the older manifest's WAL sequence and now reports 0. That
        // figure is not reconstructible from the newest manifest alone, and it was not readable
        // by anything that decided anything.
        let identities = shard.bucket_index.block_identities();
        for (handle, block) in runtime_bucket.block_index.iter() {
            // THE ROW NAMES AN OBJECT, so a page whose neighbours do not name one cannot produce a
            // row. The one such page is a tombstone whose removal named no element.
            let Some(beside) = identities.identity_of(runtime_bucket, *handle, block) else {
                continue;
            };
            let already_present = bucket.block_indexes.iter().any(|existing| {
                existing.object_key.as_str() == beside.object_key.as_ref()
                    && existing.model_id.as_str() == block.model_id.as_str()
                    // THE COMPONENT TERM IS DROPPED AS A TAUTOLOGY. It compared
                    // `existing.component` against `page.component`; the report row's component
                    // is filled from an index entry and an index entry has no element name, so
                    // both sides were `None` for every row and the term decided nothing. What
                    // identifies a row here is (key, kind, slab, offset) and that has not moved.
                    && existing.block_slab_id == block.address.block_slab_id()
                    && existing.offset == block.address.offset()
            });
            if already_present {
                continue;
            }
            let mut block_index = StoragePhysicalBlockIndex {
                object_key: beside.object_key.to_string(),
                model_id: block.model_id.clone().to_string(),
                // `None`, AND THE REPORT'S SLOT IS KEPT. `StoragePhysicalBlockIndex::component` is
                // a PUBLIC field of a serialized report, so retiring it is a schema change and not
                // part of a width step; it is filled from an index entry, and an index entry names
                // a page rather than an element, so the value it carried was already `None` for
                // every row this arm produced.
                component: None,
                routing_bucket: *routing_bucket,
                block_slab_id: block.address.block_slab_id(),
                offset: block.address.offset(),
                length: block.address.length(),
                block_id: block.address.block_id(),
                object_id: Some(block.object_id(shard_id, beside.object_key)),
                stored_slab_id: block.address.slab_id(),
                checksum: None,
                dirty: block.dirty,
                deleted: block.deleted,
                log_backed: block.log_backed(),
                native_packed_block_index_len: NATIVE_PACKED_BLOCK_INDEX_SIZE,
                native_packed_block_index_hex: String::new(),
            };
            block_index.native_packed_block_index_hex =
                hex::encode(native_packed_block_index_bytes(&block_index));
            bucket.block_indexes.push(block_index);
        }
    }
    for bucket in buckets.values_mut() {
        bucket.block_indexes.sort_by(|left, right| {
            left.object_key
                .cmp(&right.object_key)
                .then(left.model_id.cmp(&right.model_id))
                .then(left.component.cmp(&right.component))
                .then(left.block_slab_id.cmp(&right.block_slab_id))
                .then(left.offset.cmp(&right.offset))
        });
        if !shard.bucket_index.bucket_map.contains_key(&bucket.routing_bucket) {
            let object_count = bucket
                .block_indexes
                .iter()
                .filter_map(|block| block.object_id)
                .collect::<BTreeSet<_>>()
                .len();
            bucket.layout =
                bucket_layout_name(classify_bucket_layout(object_count, bucket.block_indexes.len()))
                    .to_string();
        }
        bucket.native_packed_bucket_node_len = NATIVE_PACKED_BUCKET_NODE_SIZE;
        bucket.native_packed_bucket_node_hex = hex::encode(native_packed_bucket_node_bytes(bucket));
    }
    let block_index_count = buckets
        .values()
        .map(|bucket| bucket.block_indexes.len())
        .sum::<usize>();
    let block_indexes = buckets
        .values()
        .flat_map(|bucket| bucket.block_indexes.iter())
        .collect::<Vec<_>>();
    let missing_object_id_count = block_indexes
        .iter()
        .filter(|block| block.object_id.is_none())
        .count();
    let missing_block_id_count = block_indexes
        .iter()
        .filter(|block| block.block_id.is_none())
        .count();
    let missing_checksum_count = block_indexes
        .iter()
        .filter(|block| block.checksum.is_none())
        .count();
    StoragePhysicalIndexReport {
        shard_id,
        bucket_first: true,
        bucket_index_authority: !shard.bucket_index.bucket_map.is_empty(),
        secondary_views_reconciled_from_bucket_index: !shard.bucket_index.bucket_map.is_empty(),
        bucket_count: buckets.len(),
        block_index_count,
        dirty_bucket_count: buckets.values().filter(|bucket| bucket.dirty).count(),
        missing_object_id_count,
        missing_routing_bucket_count,
        missing_block_id_count,
        missing_checksum_count,
        native_packed_block_index_size: NATIVE_PACKED_BLOCK_INDEX_SIZE,
        native_packed_bucket_node_size: NATIVE_PACKED_BUCKET_NODE_SIZE,
        native_packed_layout_compatible: true,
        bucket_nodes: buckets.into_values().collect(),
    }
}

pub(super) fn object_manager_runtime_report(
    shard_id: ShardId,
    shard: &ShardState,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
) -> ObjectManagerRuntimeReport {
    object_manager_runtime_report_from_entries(
        shard_id,
        shard,
        &collect_live_block_entries(shard),
        start_routing_bucket,
        end_routing_bucket,
    )
}

/// The same report, from live-page entries the caller ALREADY has.
///
/// This walked the shard TWICE: once for the ownership report below, and once more at the very end
/// purely to COUNT entries of eight timestamped kinds. Measured at 2.0x the shard
/// (`what_the_compaction_preamble_walks`), which is why the compaction preamble could not get
/// below 7.0x while calling the wrapper form.
pub(super) fn object_manager_runtime_report_from_entries(
    shard_id: ShardId,
    shard: &ShardState,
    entries: &[LiveBlockEntry],
    start_routing_bucket: u32,
    end_routing_bucket: u32,
