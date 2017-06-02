// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Standalone per-shard command execution extracted from engine.rs.
use crate::engine::timestamped_series::TimestampedSeriesWrite as _;
use crate::engine::timestamped_series::TimestampedSeries as _;
use super::*;

/// THE SERVED-PATH DIVERGENCE COUNTER FOR THE HASH WHOLE-OBJECT READ.
///
/// WHAT IT COUNTS. One increment for each field a served `HashGetAll` found NAMED BY THE PAGE
/// INDEX and NOT HELD BY the durable container `shard.hashes`. Per (read, field), so one read of
/// an object whose container lacks three of its fields counts three.
///
/// WHY ONLY THAT DIRECTION. It is the direction that loses a read once the index stops being
/// consulted -- which is the whole point of measuring it, because taking `component` off the page
/// entry is what makes the container load-bearing. The other direction, a field the container
/// holds that the index does not name, is not a loss: the union in `Command::HashGetAll` already
/// answers it from the container, and counting it would mix a rescued read in with a threatened
/// one.
///
/// WHAT A ZERO MEANS, AND WHAT IT DOES NOT. A zero means NO DIVERGENCE ON THE ROUTES THE TRAFFIC
/// TOOK. IT IS NOT PROOF OF COMPLETENESS. A field installed by a route that no request read is
/// never sampled here and cannot be, because this counter only ever sees reads; an object nobody
/// got is an object nobody checked. Reading a zero from here as "the container holds everything
/// the fold installs" would repeat exactly the substitution #2078 made -- it read a fixture's
/// green as a statement about the engine -- and that is the mistake a correction comment on #2078
/// was written for.
///
/// `reads_served` is reported BESIDE the count for that reason, and it is the figure to read
/// first: a zero over a zero denominator says nothing whatever, and the two numbers are the only
/// way to tell "no divergence" from "nothing was asked".
///
/// WHAT IT IS STRICTLY BETTER AT THAN THE FIXTURE IT REPLACES. It samples whatever route the
/// traffic actually took, including routes no fixture drives, and it cannot be satisfied by one
/// source being derived from the other -- the comparison happens at read time against both
/// sources as they are, with no load path in between to fill one from the other.
///
/// WHY IT IS PROCESS-WIDE AND NOT PER-SHARD. The figure is a property of the engine's read path,
/// not of a shard's contents, and it is read as a trend rather than as an inventory. Carrying a
/// `shard_id` on it would be a label describing something it did not measure, so the report
/// surface does not carry one.
///
/// THE RATE LIMIT ON THE LOG. The first [`LOG_BURST`] divergences are logged, and after that one
/// in every [`LOG_EVERY`]. An unbounded log would be one line per field per read for as long as
/// the divergence lasts, which on a hot object is the loudest possible way to say one thing; a
/// log of only the first would go quiet exactly when the condition spread. `divergences_logged`
/// is reported so the log and the count can be told apart.
pub(super) mod hash_read_divergence {
    use std::sync::atomic::{AtomicU64, Ordering};

    static READS_SERVED: AtomicU64 = AtomicU64::new(0);
    static INDEX_NAMED_FIELDS_THE_CONTAINER_LACKED: AtomicU64 = AtomicU64::new(0);
    static DIVERGENCES_LOGGED: AtomicU64 = AtomicU64::new(0);
    static LAST: std::sync::Mutex<Option<(String, String)>> = std::sync::Mutex::new(None);

    /// Log every one of the first this many, so a divergence that happens once is still named.
    const LOG_BURST: u64 = 8;
    /// After the burst, log one in this many.
    const LOG_EVERY: u64 = 1_024;

    /// One served whole-object hash read that resolved through both sources.
    ///
    /// Counted after the expiry door, because a read that removed an expired key never consulted
    /// either source and including it would inflate the denominator with reads that could not
    /// have diverged.
    pub(crate) fn note_read() {
        READS_SERVED.fetch_add(1, Ordering::Relaxed);
    }

    /// One field the page index named and the container did not hold.
    pub(crate) fn note(object_key: &str, field: &str) {
        let seen = INDEX_NAMED_FIELDS_THE_CONTAINER_LACKED.fetch_add(1, Ordering::Relaxed) + 1;
        if let Ok(mut last) = LAST.lock() {
            *last = Some((object_key.to_string(), field.to_string()));
        }
        if seen <= LOG_BURST || seen % LOG_EVERY == 0 {
            DIVERGENCES_LOGGED.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                object_key,
                field,
                divergences = seen,
                "the page index named a hash field the durable container does not hold; the \
                 union served it from the index"
            );
        }
    }

    /// The four figures, read together so a reader cannot take the count without its denominator.
    pub(crate) fn snapshot() -> (u64, u64, u64, Option<(String, String)>) {
        (
            READS_SERVED.load(Ordering::Relaxed),
            INDEX_NAMED_FIELDS_THE_CONTAINER_LACKED.load(Ordering::Relaxed),
            DIVERGENCES_LOGGED.load(Ordering::Relaxed),
            LAST.lock().ok().and_then(|last| last.clone()),
        )
    }
}

/// Put aside an outcome that no block backs: a deletion, a deadline, or state that lives only
/// in the index snapshot.
///
/// The block path stages its outcome inside `upsert_bucket_index_block`, because that is where a
/// block outcome is produced. These have no such moment, so they say it here.
#[allow(clippy::too_many_arguments)]
/// Record a block or a value that belongs to one COMPONENT of an object.
///
/// `stage_meta_outcome` states something about the object as a whole -- it is gone, its deadline
/// is this. This states what one part of it became: which block backs it, or the bytes for state no
/// block backs at all.
#[allow(clippy::too_many_arguments)]
fn stage_component_outcome(
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    component: Option<String>,
    routing_bucket: u32,
    address: Option<crate::block_store::ElementEntry>,
    value: Option<Vec<u8>>,
) {
    super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
        kind: kind.to_string(),
        object_key: object_key.to_string(),
        // MOVED, NOT CLONED. `component` is taken BY VALUE -- it is already the caller's
        // allocation handed over -- and this function ends here, so nothing reads it after.
        // Cloning it allocated a second copy of the same characters on every staged component
        // outcome: every hash field, set member, zset member, list element and context entity
        // write. `stage_meta_outcome` next door already moves its owned arguments.
        component,
        object_id: stable_block_object_id(shard_id, kind, object_key),
        routing_bucket,
        address,
        value,
        ttl: None,
        deleted: false,
        meta: false,
    });
}

fn stage_meta_outcome(
    shard_id: ShardId,
    kind: &str,
    object_key: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    value: Option<Vec<u8>>,
    ttl: Option<u64>,
    deleted: bool,
) {
    super::block_in_wal::stage_outcome(crate::wal::WalOutcomeItem {
        kind: kind.to_string(),
        object_key: object_key.to_string(),
        component: None,
        object_id: stable_block_object_id(shard_id, kind, object_key),
        routing_bucket: block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket),
        address: None,
        value,
        ttl,
        deleted,
        meta: true,
    });
}

/// Read a node record back, by the same two lookups every node reader here uses.
fn load_context_node(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &ShardState,
    object_key: &str,
) -> Option<ContextNode> {
    // THE BUCKET THE PAGE CACHE IS KEYED BY, derived the way the write path derives it.
    let (start_routing_bucket, end_routing_bucket) = shard.routing_range();
    let routing_bucket = Some(block_routing_bucket(
        object_key,
        start_routing_bucket,
        end_routing_bucket,
    ));
    // EACH ARM NAMES ITS OWN OBJECT, which is what the identity forced this lookup to say.
    //
    // The two arms are not two places to find one block: a CURRENT node is the `CONTEXT_NODE_FIELD`
    // field of a `hash`, and a pre-hashes node is a whole `context_node` block --
    // `model_map_block_address` spells the second as `("context_node", None)` and is the authority
    // on it. One identity for both is wrong for whichever arm it does not describe, and the comment
    // that used to sit at the read asserted they were the same. Under the old shape only the
    // ELEMENT was wrong, because the object came off the address: the in-log fallback built the block
    // key `(id, Some("meta"))` for a block registered as `(id, None)` and missed it, which reads as
    // MISSING for a durably acknowledged write on that path. The identity cannot be built without
    // naming a kind, so the arms had to separate.
    shard
        .hashes
        .get(object_key)
        .and_then(|fields| fields.get(CONTEXT_NODE_FIELD))
        .map(|address| (address, BlockIdentity::of(shard_id, "hash", object_key, Some(CONTEXT_NODE_FIELD))))
        .or_else(|| {
            shard
                .context_nodes()
                .get(object_key)
                .map(|address| (address, BlockIdentity::of(shard_id, "context_node", object_key, None)))
        })
        .and_then(|(address, identity)| {
            // Owning, not shared: the summary upsert calls this while writing, where the block
            // is as likely to be a miss as a hit -- and on a miss the shared read wraps an owned
            // buffer in a fresh Arc, which copies a second time. The query commands, which are
            // hit-heavy, do share.
            read_block_bytes(
                cache,
                block_store,
                shard_id,
                address,
                identity,
                routing_bucket,
            )
            .and_then(|bytes| context_from_bytes::<ContextNode>(&bytes))
        })
}

/// Persist a node record: the one producer of a node block.
///
/// Three commands now write a node -- the upsert, the embedding attach, and the summary upsert
/// keeping the node's copy of its summary vector current -- and each needs the block appended,
/// the outcome staged, the hash field pointed at the new address and the record invalidated, in
/// that order. Held apart, the third copy is where one of those four steps goes missing.
///
/// Returns whether a block was written, which is what the caller records as `mutated`.
#[allow(clippy::too_many_arguments)]
fn write_context_node(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard_id: ShardId,
    shard: &mut ShardState,
    object_key: &str,
    node: &ContextNode,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> bool {
    let object_id = stable_block_object_id(shard_id, "hash", object_key);
    let routing_bucket = block_routing_bucket(object_key, start_routing_bucket, end_routing_bucket);
    let mut wrote = false;
    // FRAMED AS A HASH BLOCK, because that is what it is. The outcome below records it under
    // `context_node` for a reason of its own -- it is never filed in the block index -- but the block
    // itself lands in `shard.hashes` under `CONTEXT_NODE_FIELD` and is read back through the hash
    // door with that field as its component. Leaving this one site unframed would leave the node
    // the single hash element a later load could not recover from its block, which is the element
    // #2016 already recorded as the one whose absence reads as the node not existing at all.
    let block = crate::engine::container_blocks::single_element_block(
        "hash",
        CONTEXT_NODE_FIELD,
        &context_bytes(node),
    );
    if let Ok(address) = append_value(
        cache,
        block_store,
        shard_id,
        &block,
        Some(object_id),
        // The field this node is filed under -- the same argument the object id above was
        // derived with. A context node is a hash FIELD, not a whole object.
        Some(CONTEXT_NODE_FIELD),
        Some(routing_bucket),
        async_storage,
    ) {
        // Its own kind, deliberately: this writes a hash block and records the OUTCOME under
        // `context_node` rather than `hash`, so a rebuild cannot add an entry naming a block the
        // outcome never described.
        //
        // CORRECTED: this said the write "never registers it in the bucket index". It does -- the
        // `append_value` above files the block from the object id, `CONTEXT_NODE_FIELD` and the
        // routing bucket, and `context_node_survives_reload` reads that entry back as
        // `("hash", Some("meta"), deleted=false)`. The distinction is between the index entry,
        // which exists, and the outcome record's kind, which is not `hash` on purpose.
        // THE CONSTANT, INTERNED ONCE PER SHARD INSTEAD OF COPIED PER NODE. This is
        // the write path the narrow arm of the measurement is about: every context
        // node in a store files its page under this one four-character name.
        let field_handle =
            super::state::intern_shared(&mut shard.bucket_index.kind_pool, CONTEXT_NODE_FIELD);
        super::recorded_map::install_element_staged_only::<super::recorded_map::HashKind>(
            shard,
            shard_id,
            "context_node",
            object_key,
            CONTEXT_NODE_FIELD,
            routing_bucket,
            field_handle,
            address.clone(),
            address,
        );
        wrote = true;
    }
    invalidate_context_record(cache, shard_id, object_key);
    wrote
}

/// The read-time half of a deadline, at the one spelling every arm uses.
///
/// A deadline is honoured in two places and both have to agree: the background sweep, which
/// collects the key eventually, and this, which makes the key read as gone the instant its
/// deadline passes. Without the second, a key stays visible -- and answers with real data --
/// for the whole interval between its deadline and whichever sweep round collects it.
///
/// It is a PER-CALL-SITE obligation: `execute_on_shard` has one arm per command and each one
/// has to remember. So it is spelled once here and called, and
/// `execute_on_shard_guards_every_arm_that_can_hold_a_deadline` fails when an arm appears
/// that calls neither this nor `remove_if_expired` and is not listed as exempt with a reason.
///
/// COST. `remove_if_expired` returns on an `is_empty` when the shard holds no deadlines at
/// all, and otherwise examines only the key it is given plus the three control-state families
/// derived from it. It never walks the keyspace, so the O(due) cost of an expiry round is
/// untouched. The cache sweep below runs only when a record was actually dropped -- once per
/// expired key, not once per read.
///
/// THE CACHE GOES WITH THE RECORD. Dropping the record and leaving its cached copy would let
/// the very next read answer from the copy, which is the bug this exists to close.
fn drop_if_expired(
    cache: &MultiLayerCache,
    shard_id: ShardId,
    shard: &mut ShardState,
    key: &str,
) -> bool {
    if remove_if_expired(shard, key) {
        invalidate_record_all(cache, shard_id, key, &CACHE_SWEEP_COUNTS);
        true
    } else {
        false
    }
}

/// Remove one element of a container, RECORDING THE REMOVAL IN THE PAGES as well as the index.
///
/// # WHY THERE IS A FUNCTION HERE AT ALL
///
/// Seven call sites removed a container element by calling `mark_bucket_index_block_deleted`, which
/// drops the index entry and stages the outcome. That left the PAGES saying nothing: #2028 drove
/// what it costs -- a folded page holding twelve members the index named eleven of -- and the reason
/// is that the page is never rewritten by a removal. So a removal now APPENDS a page that states it,
/// and the seven sites differ in their kind and their component and in nothing else. Spelling the
/// append at each of them is how the fourth site gets the ordinal wrong.
///
/// Two of the seven are not obviously removals and are the ones this must not miss: a `ZSetAdd` or
/// `ZSetIncrement` that changes a member's score REMOVES the old `(score, member)` element key and
/// writes a new one. Leave the old key untombstoned and a page-derived membership holds the member
/// TWICE, at both scores. `a_rescore_sweeps_its_own_tombstone_because_the_component_no_longer_spells_the_score`
/// drives that -- and, since the component the removal tombstones and the component the new write
/// files under are now the same string, also drives the sweep that cleans the tombstone up again
/// in the same write.
///
/// # WHAT IT DOES WHEN IT CANNOT WRITE THE PAGE
///
/// It removes the element anyway and leaves the pages silent, with the miss counted by
/// `container_blocks::unframed_container_removal_count` or by [`TOMBSTONE_APPEND_FAILURES`]. That is
/// the only honest choice: refusing the removal would keep serving an element the caller deleted,
/// which is worse than a page set that a derivation will decline to install. Both counters are
/// floored at zero by the guards, so neither is a tolerance -- they are how a hole announces itself
/// instead of appearing later as a resurrected member.
#[allow(clippy::too_many_arguments)]
pub(super) fn remove_container_element(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    shard: &mut ShardState,
    shard_id: ShardId,
    model_id: &str,
    key: &str,
    component: &str,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> bool {
    let routing_bucket = block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
    let tombstone = crate::engine::container_blocks::tombstone_block(model_id, component).and_then(
        |block| {
            // THE ORDINAL IS THE ELEMENT'S OWN, exactly as it is on the write path: the element
            // already has a position in this object and the tombstone is another page AT that
            // position, not a new one. Computed before the append because the append stamps it into
            // the record header and onto the address from one value -- `decode_block_record` refuses
            // a page whose two copies disagree, which is what #2008 threaded this for.
            let block_ordinal = crate::engine::state::container_block_ordinal(
                shard,
                model_id,
                key,
                component,
            );
            append_value_of_object(
                cache,
                block_store,
                shard_id,
                &block,
                Some(stable_block_object_id(shard_id, model_id, key)),
                Some(component),
                Some(routing_bucket),
                async_storage,
                block_ordinal,
            )
            .ok()
        },
    );
    if tombstone.is_none() {
        TOMBSTONE_APPEND_FAILURES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    crate::engine::mark_bucket_index_block_deleted_recording(
        shard,
        shard_id,
        model_id,
        key,
        Some(component),
        true,
        tombstone,
    )
}

/// Gated listings answered from a fold of the pages, and gated listings that DECLINED to.
///
/// Two counters because a test must be able to tell which path produced an answer. A gated
/// listing that quietly fell back to the durable map would return the right members and prove
/// nothing about the fold -- the shape that made an earlier measurement in this series report a
/// clean result over a branch nothing reached.
static GATED_LISTING_DERIVED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static GATED_LISTING_DECLINED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// (derived, declined) for gated container listings.
pub fn gated_listing_counts() -> (u64, u64) {
    (
        GATED_LISTING_DERIVED.load(std::sync::atomic::Ordering::Relaxed),
        GATED_LISTING_DECLINED.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// Forget them, so a test measures its own exercise.
pub fn reset_gated_listing_counts() {
    GATED_LISTING_DERIVED.store(0, std::sync::atomic::Ordering::Relaxed);
    GATED_LISTING_DECLINED.store(0, std::sync::atomic::Ordering::Relaxed);
}

/// Removals whose tombstone page could not be APPENDED, as distinct from could not be FRAMED.
///
/// Two counters and not one, because the two say different things about the store: an unframable
/// removal is a bug in this code's spelling of a kind, and a failed append is the block store
/// refusing a write. A guard that floored their sum would pass while one of them was firing.
static TOMBSTONE_APPEND_FAILURES: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// How many removals could not append the page that would have recorded them.
pub fn tombstone_append_failure_count() -> u64 {
    TOMBSTONE_APPEND_FAILURES.load(std::sync::atomic::Ordering::Relaxed)
}

/// Forget the count, so a test measures its own exercise.
pub fn reset_tombstone_append_failure_count() {
    TOMBSTONE_APPEND_FAILURES.store(0, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn execute_on_shard(
    cache: &MultiLayerCache,
    block_store: &BlockStore,
    feature_max_size: usize,
    async_storage: bool,
    control_rollup_enabled: bool,
    control_coalesce_persist: bool,
    control_distinct_sketch: bool,
    shard_id: ShardId,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    shard: &mut ShardState,
    command: Command,
) -> ExecuteOutcome {
    // Publish per-execute hints so the write helpers don't need the flags threaded through
    // every control-state write arm.
    shard.control_coalesce_persist = control_coalesce_persist;
    shard.control_distinct_sketch = control_distinct_sketch;
    let mut mutated = false;
    let response = match command {
        // Applied as nothing: `mutated` stays false, so it neither dirties a shard nor
        // invalidates a cache entry.
        Command::LeaderEstablish => CommandResponse::Empty,
        // Blob commands are dispatched before the shard lock (they live beside the engine,
        // not in shard record state); reaching this arm means the early dispatch was skipped.
        Command::ContextResourceBlobBegin { .. }
        | Command::ContextResourceBlobAppend { .. }
        | Command::ContextResourceBlobCommit { .. }
        | Command::ContextResourceBlobPut { .. }
        | Command::ContextResourceBlobFetch { .. }
        | Command::ContextResourceBlobSweep { .. } => CommandResponse::Empty,
        Command::CommonDelete { key } => {
            // An outcome with no block: the object is gone. Their log item states the same thing
            // with `object_deleted`, and replay applies it without re-running a delete.
            stage_meta_outcome(
                shard_id,
                "object",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                None,
                None,
                true,
            );
            mutated = delete_record(shard, &key);
            invalidate_record_all(cache, shard_id, &key, &CACHE_SWEEP_COUNTS);
            CommandResponse::Empty
        }
        Command::CommonExpire { key, ttl_ms } => {
            // Expired-but-unswept is "missing" here, exactly as `CommonPersist` treats it
            // and exactly as reads do. Without this, `record_exists_exact` below still sees
            // the unswept record and the deadline is RE-ARMED, bringing back a key that
            // every read already answers is gone.
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let expires_at = resolve_now_ms().saturating_add(ttl_ms);
            for record_key in associated_record_keys(&key) {
                if record_exists_exact(shard, &record_key) {
                    crate::engine::set_expiry(shard, record_key, expires_at);
                }
            }
            mutated = true;
            // The deadline is recorded ALREADY RESOLVED. That is the point: a replay applying
            // this outcome needs no clock of its own, where re-running the command would resolve
            // the TTL against the restart clock and extend every recently-expiring key.
            stage_meta_outcome(
                shard_id,
                "object",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                None,
                Some(expires_at),
                false,
            );
            invalidate_record_all(cache, shard_id, &key, &CACHE_SWEEP_COUNTS);
            CommandResponse::Empty
        }
        Command::CommonPersist { key } => {
            // Expired-but-unswept is "missing" here, exactly as reads treat it: removing the
            // sweep's pending work must not resurrect a value whose deadline already passed.
            if remove_if_expired(shard, &key) {
                mutated = true;
                // The collection is a DELETION, and it has to be recorded as one. This branch
                // reported a mutation and staged nothing, so `PERSIST` on a key whose deadline
                // had passed but which the sweep had not yet reached hit the standing
                // "changed the shard and recorded nothing" check in every debug build --
                // `SET k v PX 5`, wait, `PERSIST k` panicked the shard on an unmodified tree.
                // In a release build it is quieter and worse: the WAL entry for the command
                // carries no item describing the removal, so recovery has to re-run the
                // command against the restart clock instead of installing what it did.
                // Staged exactly as `CommonDelete` stages its own removal, which is the same
                // event reached by a different route.
                stage_meta_outcome(
                    shard_id,
                    "object",
                    &key,
                    start_routing_bucket,
                    end_routing_bucket,
                    None,
                    None,
                    true,
                );
                invalidate_record_all(cache, shard_id, &key, &CACHE_SWEEP_COUNTS);
                CommandResponse::Integer { value: 0 }
            } else {
                let mut removed = false;
                for record_key in associated_record_keys(&key) {
                    if crate::engine::clear_expiry(shard, &record_key)
                        && record_exists_exact(shard, &record_key)
                    {
                        removed = true;
                    }
                }
                if removed {
                    mutated = true;
                    stage_meta_outcome(
                        shard_id,
                        "object",
                        &key,
                        start_routing_bucket,
                        end_routing_bucket,
                        None,
                        None,
                        false,
                    );
                    invalidate_record_all(cache, shard_id, &key, &CACHE_SWEEP_COUNTS);
                }
                CommandResponse::Integer {
                    value: i64::from(removed),
                }
            }
        }
        Command::CommonTtl { key } => {
            // Take the answer from the COLLECTION, the way the other thirty-six arms do, rather
            // than from a second and narrower predicate beside it. `ttl_ms` calls
            // `remove_if_expired`, which collects the key AND its three control-state families
            // (`associated_record_keys`). Asking `expires_at_ms` about the bare key alone
            // answered a narrower question than the collection actually performed, so a TTL that
            // collected an expired `control_state:<family>:<key>` record reported no mutation:
            // the record left memory and no WAL tombstone was ever appended for it, leaving it to
            // come back on the next recovery after the caller had been told it was gone. An
            // expiry deletion is a LOGGED deletion on this path as much as in the sweep.
            //
            // `drop_if_expired` is the one spelling the other arms use. It also drops the cached
            // copy of the record it removes -- which the inline predicate never did, so the next
            // read could answer from the cache of a record just collected -- and it reads the
            // same replay-aware clock as every other deadline check, where the inline test read
            // the live clock instead.
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            let value = ttl_ms(shard, &key);
            CommandResponse::Integer { value }
        }
        Command::CommonExists { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                invalidate_record_all(cache, shard_id, &key, &CACHE_SWEEP_COUNTS);
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: if record_exists(shard, &key) { 1 } else { 0 },
            }
        }
        Command::StringSet { key, value } => {
            remove_if_expired(shard, &key);
            let object_id = stable_block_object_id(shard_id, "string", &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            if let Ok(address) = append_value(
                cache,
                block_store,
                shard_id,
                &value,
                Some(object_id),
                // A string block IS its whole object: the `None` the object id above carries.
                None,
                Some(routing_bucket),
                async_storage,
            ) {
                upsert_bucket_index_block(
                    shard,
                    shard_id,
                    "string",
                    &key,
                    None,
                    address.clone(),
                    true,
                );
                // ONE COPY OF THE CHARACTERS. The page was filed on the line above, so the
                    // lookup holds a handle for this object now -- take it rather than allocating a
                    // second copy of the same text. Allocate only when it has none, which is the
                    // shape `stream_batch_methods` already uses for this very map.
                    let stored_key = shard
                        .bucket_index
                        .object_block_lookup
                        .any_shared_object_key(key.as_str())
                        .unwrap_or_else(|| std::sync::Arc::from(key.as_str()));
                    shard.set_string_address(stored_key, &address);
                    // THE WRITER'S OWN LENGTH, which is the only place it is known. `address`
                    // carries the STORED length, so a denominator taken from the index would be
                    // compressed bytes; this is what the caller handed us, before the codec.
                    shard.set_string_logical_size(key.as_str(), value.len() as u64);
                mutated = true;
            }
            invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            CommandResponse::Empty
        }
        Command::StringSetEx { key, value, ttl_ms } => {
            remove_if_expired(shard, &key);
            let object_id = stable_block_object_id(shard_id, "string", &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            if let Ok(address) = append_value(
                cache,
                block_store,
                shard_id,
                &value,
                Some(object_id),
                // A string block IS its whole object.
                None,
                Some(routing_bucket),
                async_storage,
            ) {
                upsert_bucket_index_block(
                    shard,
                    shard_id,
                    "string",
                    &key,
                    None,
                    address.clone(),
                    true,
                );
                // ONE COPY OF THE CHARACTERS. The page was filed on the line above, so the
                    // lookup holds a handle for this object now -- take it rather than allocating a
                    // second copy of the same text. Allocate only when it has none, which is the
                    // shape `stream_batch_methods` already uses for this very map.
                    let stored_key = shard
                        .bucket_index
                        .object_block_lookup
                        .any_shared_object_key(key.as_str())
                        .unwrap_or_else(|| std::sync::Arc::from(key.as_str()));
                    shard.set_string_address(stored_key, &address);
                    // THE WRITER'S OWN LENGTH, which is the only place it is known. `address`
                    // carries the STORED length, so a denominator taken from the index would be
                    // compressed bytes; this is what the caller handed us, before the codec.
                    shard.set_string_logical_size(key.as_str(), value.len() as u64);
                let expires_at = resolve_now_ms().saturating_add(ttl_ms);
                crate::engine::set_expiry(shard, key.clone(), expires_at);
                // This write sets a value AND a deadline. Recording only the block passes a probe
                // that asks whether the record said anything, and produces a recovered key that
                // never expires -- so the deadline is recorded too, already resolved, exactly as
                // CommonExpire records its own.
                stage_meta_outcome(
                    shard_id,
                    "object",
                    &key,
                    start_routing_bucket,
                    end_routing_bucket,
                    None,
                    Some(expires_at),
                    false,
                );
                mutated = true;
            }
            invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            CommandResponse::Empty
        }
        Command::StringSetConditional {
            key,
            value,
            ttl_ms,
            condition,
            return_old,
            keep_ttl,
        } => {
            remove_if_expired(shard, &key);
            let old_value = shard.string_address(key.as_str()).and_then(|address| {
                read_block_bytes(
                    cache,
                    block_store,
                    shard_id,
                    &address,
                    // `shard.strings`: a whole-object block, so the element is `None` and the
                    // object is the key itself.
                    BlockIdentity::of(shard_id, "string", &key, None),
                    Some(block_routing_bucket(
                        &key,
                        start_routing_bucket,
                        end_routing_bucket,
                    )),
                )
            });
            let exists = old_value.is_some();
            let should_set = match condition {
                StringSetCondition::Always => true,
                StringSetCondition::IfExists => exists,
                StringSetCondition::IfNotExists => !exists,
            };
            if should_set {
                let object_id = stable_block_object_id(shard_id, "string", &key);
                let routing_bucket =
                    block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
                if let Ok(address) = append_value(
                    cache,
                    block_store,
                    shard_id,
                    &value,
                    Some(object_id),
                    // A string block IS its whole object.
                    None,
                    Some(routing_bucket),
                    async_storage,
                ) {
                    upsert_bucket_index_block(
                        shard,
                        shard_id,
                        "string",
                        &key,
                        None,
                        address.clone(),
                        true,
                    );
                    // ONE COPY OF THE CHARACTERS. The page was filed on the line above, so the
                    // lookup holds a handle for this object now -- take it rather than allocating a
                    // second copy of the same text. Allocate only when it has none, which is the
                    // shape `stream_batch_methods` already uses for this very map.
                    let stored_key = shard
                        .bucket_index
                        .object_block_lookup
                        .any_shared_object_key(key.as_str())
                        .unwrap_or_else(|| std::sync::Arc::from(key.as_str()));
                    shard.set_string_address(stored_key, &address);
                    // THE WRITER'S OWN LENGTH, which is the only place it is known. `address`
                    // carries the STORED length, so a denominator taken from the index would be
                    // compressed bytes; this is what the caller handed us, before the codec.
                    shard.set_string_logical_size(key.as_str(), value.len() as u64);
                    if let Some(ttl_ms) = ttl_ms {
                        let expires_at = resolve_now_ms().saturating_add(ttl_ms);
                        crate::engine::set_expiry(shard, key.clone(), expires_at);
                        // A conditional write that refreshes a deadline records the refreshed
                        // one. Recording only the block leaves a replay installing the value over
                        // a LAPSED deadline from an earlier record, and the key reads as expired
                        // even though the leader kept it alive.
                        stage_meta_outcome(
                            shard_id,
                            "object",
                            &key,
                            start_routing_bucket,
                            end_routing_bucket,
                            None,
                            Some(expires_at),
                            false,
                        );
                    } else if keep_ttl {
                        // KEEPTTL: THE DEADLINE IS NOT TOUCHED, AND THAT IS A RESULT TOO.
                        //
                        // `remove_if_expired` at the top of this arm has already taken the key
                        // away if its deadline had lapsed, so whatever is in `expires_at_ms`
                        // here is a LIVE deadline -- this cannot preserve a dead one.
                        //
                        // What is staged is the deadline the key ALREADY HOLDS, not `None`.
                        // Staging `None` would spell "this key has no deadline" to a replay,
                        // and the replay would install the new value over a cleared deadline --
                        // turning the one option whose whole purpose is to keep the countdown
                        // into the one that loses it, but only after a restart.
                        let kept = shard.expires_at_ms().get(key.as_str()).copied();
                        stage_meta_outcome(
                            shard_id,
                            "object",
                            &key,
                            start_routing_bucket,
                            end_routing_bucket,
                            None,
                            kept,
                            false,
                        );
                    } else {
                        crate::engine::clear_expiry(shard, &key);
                        // No deadline is equally a result. An object outcome carrying neither a
                        // deadline nor a deletion says exactly that.
                        stage_meta_outcome(
                            shard_id,
                            "object",
                            &key,
                            start_routing_bucket,
                            end_routing_bucket,
                            None,
                            None,
                            false,
                        );
                    }
                    mutated = true;
                }
                invalidate_cache_key(cache, CacheKey::string(shard_id, &key), async_storage);
            }
            if return_old {
                CommandResponse::Bytes { value: old_value }
            } else {
                CommandResponse::Integer {
                    value: if mutated { 1 } else { 0 },
                }
            }
        }
        Command::StringGet { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate(&CacheKey::string(shard_id, &key));
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            // THE PAYLOAD IS NOT CACHED A SECOND TIME UNDER THE LOGICAL KEY.
            //
            // A point read used to fill two cache layers that both hold the payload: this response
            // under `CacheKey::string(shard, key)`, and the block frame under
            // `CacheKey::page_with_slot(...)` inside the read below. That measured
            // `2 * value + envelope` bytes an entry at exactly 2.00 fills per key.
            //
            // THE FRAME IS THE COPY WORTH KEEPING, on three counts. `cached_response` stores with
            // `put_memory_only`, so this copy was memory-only, while the frame goes through
            // `cache.put` and is tiered and persistent -- `part2` asserts a tiny memory cache
            // refills from that tier without a block-store read. The frame key is read by the 23
            // callers of `read_block_bytes` and the two of `read_block_frame_bytes`; this key, for
            // a string, had exactly one reader. And this copy was the LARGER of the two, by the
            // msgpack response envelope.
            //
            // WHAT IT COSTS: a repeat read no longer short-circuits here, so it pays the
            // resident-map lookup and the element selection again. It must pay no extra
            // block-store read -- the frame entry is what it finds -- and that is asserted rather
            // than assumed by the cache arms that measure this path.
            CommandResponse::Bytes {
                value: read_bucket_index_value(
                    cache, block_store, shard_id, shard, "string", &key, None,
                ),
            }
        }
        Command::StringDelete { key } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            stage_meta_outcome(
                shard_id,
                "string",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                None,
                None,
                true,
            );
            mutated |= mark_bucket_index_object_deleted(shard, &key);
            mutated |= shard.remove_string_row(key.as_str()).is_some();
            let _ = cache.invalidate(&CacheKey::string(shard_id, &key));
            CommandResponse::Empty
        }
        Command::HashSet { key, field, value } => {
            remove_if_expired(shard, &key);
            let object_id = stable_block_object_id(shard_id, "hash", &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // WHICH block of this object this element is. Computed BEFORE the append, because
            // the append stamps the ordinal into the record header AND onto the address it
            // returns from the same value -- assigning it afterwards would leave the two
            // disagreeing, which `decode_block_record` refuses as a page id mismatch.
            //
            // AND THE QUESTION GOES TO THE MAP THAT IS KEYED BY THE FIELD, inside the function.
            // `container_block_ordinal` used to find an existing field's position by looking for an
            // entry filed under that field's component; a page-named entry carries none, so the
            // walk fell through to `highest + 1` on every overwrite -- a fresh ordinal, a fresh
            // address, a fresh page, for a write that should land on the same one. Hash is the one
            // container kind that overwrites a field IN PLACE this way.
            //
            // THAT IS ONE BRANCH, NOT TWO, NOW. This arm used to read `shard.hashes` itself under
            // the gate and call the function without it; the function reads the resident map
            // itself, so the two arms of that `if` computed the same value and the gate decided
            // nothing. The rule lives in one place -- see `state::container_block_ordinal`, which
            // also records why the resident map answers the high-water half as well.
            let block_ordinal = crate::engine::state::container_block_ordinal(
                shard,
                "hash",
                &key,
                &field,
            );
            // The block STATES which field it is, rather than leaving that to the entry that names
            // it. Same value, one frame around it -- see `container_blocks`.
            let block = crate::engine::container_blocks::single_element_block("hash", &field, &value);
            if let Ok(address) = append_value_of_object(
                cache,
                block_store,
                shard_id,
                &block,
                Some(object_id),
                // The element this block holds -- the SAME expression that derived the ordinal
                // above and the object id beside it. All three name one block, and since the frame
                // above it is also the key INSIDE the block.
                Some(field.as_str()),
                Some(routing_bucket),
                async_storage,
                block_ordinal,
            ) {
                // ONE HANDLE FROM THE POOL, for the name this map keeps. The component
                // argument below still carries its own owned copy; that position is the
                // bucket index's and is converted separately.
                let field_handle =
                    super::state::intern_shared(&mut shard.bucket_index.kind_pool, &field);
                super::recorded_map::install_element::<super::recorded_map::HashKind>(
                    shard,
                    shard_id,
                    &key,
                    Some(field.clone()),
                    field_handle,
                    address.clone(),
                    true,
                    address,
                );
                mutated = true;
            }
            invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
            CommandResponse::Empty
        }
        Command::HashGet { key, field } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            cached_response(cache, CacheKey::hash(shard_id, &key, &field), || {
                CommandResponse::Bytes {
                    // UNDER ONE ENTRY A PAGE THE INDEX CANNOT NAME THIS FIELD, so the index-only
                    // read would answer `None` for a field that is there. `shard.hashes` is
                    // durable (`#[serde(default)]`, see `RecordedMap`'s unconditional
                    // `Serialize`) and already the address `HashMultiGet` reads a field from --
                    // same lookup, used here as the point read's source of truth under the gate.
                    value: shard
                        .hashes
                        .get(&key)
                        .and_then(|fields| fields.get(&field))
                        .and_then(|address| {
                            read_block_bytes(
                                cache,
                                block_store,
                                shard_id,
                                address,
                                BlockIdentity::of(shard_id, "hash", &key, Some(field.as_str())),
                                Some(block_routing_bucket(
                                    &key,
                                    start_routing_bucket,
                                    end_routing_bucket,
                                )),
                            )
                        }),
                }
            })
        }
        Command::HashMultiGet { key, fields } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "hash", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Values {
                        values: vec![None; fields.len()],
                    },
                    mutated,
                };
            }
            let hash_fields = shard.hashes.get(&key);
            let values = fields
                .iter()
                .map(|field| {
                    hash_fields
                        .and_then(|entries| entries.get(field))
                        .and_then(|address| {
                            read_block_bytes(
                                cache,
                                block_store,
                                shard_id,
                                address,
                                // The hash the key names, and the field being read as its element.
                                BlockIdentity::of(shard_id, "hash", &key, Some(field.as_str())),
                                Some(block_routing_bucket(
                                    &key,
                                    start_routing_bucket,
                                    end_routing_bucket,
                                )),
                            )
                        })
                })
                .collect();
            CommandResponse::Values { values }
        }
        Command::HashMultiSet { key, entries } => {
            remove_if_expired(shard, &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // A COUNT, not a list of pending proofs. The install happens inline now, so nothing
            // is carried past the loop except whether anything landed.
            let mut applied = 0usize;
            for (field, value) in entries {
                let object_id = stable_block_object_id(shard_id, "hash", &key);
                // WHICH block of this object this element is. Computed BEFORE the append, because
                // the append stamps the ordinal into the record header AND onto the address it
                // returns from the same value -- assigning it afterwards would leave the two
                // disagreeing, which `decode_block_record` refuses as a page id mismatch.
                //
                // AND THE POSITION COMES FROM THE RESIDENT MAP, inside the function -- see
                // `HashSet`'s identical site and `state::container_block_ordinal`.
                let block_ordinal = crate::engine::state::container_block_ordinal(
                        shard,
                        "hash",
                        &key,
                        &field,
                );
                // The block STATES which field it is -- see `container_blocks`. Built per entry
                // because the frame carries the field, so one frame cannot stand for two.
                let block =
                    crate::engine::container_blocks::single_element_block("hash", &field, &value);
                if let Ok(address) = append_value_of_object(
                    cache,
                    block_store,
                    shard_id,
                    &block,
                    Some(object_id),
                    // The element this block holds -- the SAME expression that derived the ordinal
                    // above and the object id beside it. All three name one block.
                    Some(field.as_str()),
                    Some(routing_bucket),
                    async_storage,
                    block_ordinal,
                ) {
                    // ONE HANDLE FROM THE POOL, for the name this map keeps. The component
                    // argument below still carries its own owned copy; that position is the
                    // bucket index's and is converted separately.
                    let field_handle =
                        super::state::intern_shared(&mut shard.bucket_index.kind_pool, &field);
                    super::recorded_map::install_element::<super::recorded_map::HashKind>(
                        shard,
                        shard_id,
                        &key,
                        Some(field.clone()),
                        field_handle,
                        address.clone(),
                        true,
                        address,
                    );
                    invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
                    applied += 1;
                }
            }
            if applied > 0 {
                mutated = true;
            }
            CommandResponse::Empty
        }
        Command::HashIncrBy {
            key,
            field,
            increment,
        } => {
            remove_if_expired(shard, &key);
            // UNDER ONE ENTRY A PAGE THE INDEX CANNOT NAME THIS FIELD, so the index-only read
            // would miss a present counter through `unwrap_or_default()` and silently restart it
            // at zero -- this is the one HASH surface among the four that WRITES on a miss, so
            // the wrong answer is not just served, it is persisted. `shard.hashes` is durable and
            // already the address `HashMultiGet`/the gated `HashGet` read a field from.
            let current = shard
                .hashes
                .get(&key)
                .and_then(|fields| fields.get(&field))
                .and_then(|address| {
                    read_block_bytes(
                        cache,
                        block_store,
                        shard_id,
                        address,
                        BlockIdentity::of(shard_id, "hash", &key, Some(field.as_str())),
                        Some(block_routing_bucket(
                            &key,
                            start_routing_bucket,
                            end_routing_bucket,
                        )),
                    )
                })
                .and_then(|bytes| parse_i64(&bytes))
                .unwrap_or_default();
            let value = current.saturating_add(increment);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // WHICH block of this object this element is. Computed BEFORE the append, because
            // the append stamps the ordinal into the record header AND onto the address it
            // returns from the same value -- assigning it afterwards would leave the two
            // disagreeing, which `decode_block_record` refuses as a page id mismatch.
            //
            // AND THE POSITION COMES FROM THE RESIDENT MAP, inside the function -- see `HashSet`'s
            // identical site and `state::container_block_ordinal` for why the index cannot say.
            let block_ordinal = crate::engine::state::container_block_ordinal(
                shard,
                "hash",
                &key,
                &field,
            );
            // The block STATES which field it is -- see `container_blocks`.
            let block = crate::engine::container_blocks::single_element_block(
                "hash",
                &field,
                value.to_string().as_bytes(),
            );
            if let Ok(address) = append_value_of_object(
                cache,
                block_store,
                shard_id,
                &block,
                Some(stable_block_object_id(shard_id, "hash", &key)),
                // The element this block holds -- the same expression that derived the ordinal
                // above, and now also the key INSIDE the block. The object id beside it no longer
                // names the element: #2019 made it the object's, so the ordinal, the component and
                // the frame are the three things that say which element this is.
                Some(field.as_str()),
                Some(routing_bucket),
                async_storage,
                block_ordinal,
            ) {
                // ONE HANDLE FROM THE POOL, for the name this map keeps. The component
                // argument below still carries its own owned copy; that position is the
                // bucket index's and is converted separately.
                let field_handle =
                    super::state::intern_shared(&mut shard.bucket_index.kind_pool, &field);
                super::recorded_map::install_element::<super::recorded_map::HashKind>(
                    shard,
                    shard_id,
                    &key,
                    Some(field.clone()),
                    field_handle,
                    address.clone(),
                    true,
                    address,
                );
                invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
                mutated = true;
            }
            CommandResponse::Integer { value }
        }
        Command::HashGetAll { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "hash", &key);
                return ExecuteOutcome {
                    response: CommandResponse::HashEntries {
                        entries: Vec::new(),
                    },
                    mutated,
                };
            }
            // THE TWO SOURCES THAT NAME THIS OBJECT'S FIELDS, AND WHY THIS READ NOW ASKS BOTH.
            //
            // The page index has always been the source this read resolves field names from, and
            // the durable container `shard.hashes` names the same fields from the other side.
            // #2078 set out to prove the container holds every field the fold installs, and it
            // does not prove that: BOTH load-path functions in `storage_bucket_internals` derive a
            // hash field map by WALKING THE PAGE INDEX and merge it into the container through
            // `RecordedHashContainer::reconcile_from_durable` --
            // `rebuild_unserialized_model_maps_from_bucket_index` and
            // `reconcile_secondary_views_from_bucket_index`. So after a reload the container names
            // what the index names BY CONSTRUCTION, the fixture compares the index against a
            // structure derived from it, and disabling the container carry leaves it green.
            //
            // CITED BY FUNCTION AND NOT BY LINE, because this change watched a line citation go
            // stale while it was being written: the second site sat at `:5229` on the commit this
            // was authored against and at `:5248` two commits later, moved by #2085.
            //
            // So the precondition is established HERE instead, at run time against whatever
            // traffic the process serves. That samples every route a request takes rather than
            // the ones a fixture drives, and no load path sits between the two sources to fill
            // one from the other.
            //
            // SERVING THE UNION IS WHAT MAKES THAT SAFE. A field only one source names is still
            // answered, so a divergence is OBSERVED rather than SUFFERED and no read is lost
            // while the measurement is being taken.
            let indexed = bucket_index_component_block_addresses(shard, "hash", &key);
            // Copied out before the block reads: those borrow the cache and the block store while
            // this borrows the shard, and the addresses are what both halves of the union need.
            let contained: Vec<(String, crate::block_store::ElementEntry)> = shard
                .hashes
                .get(&key)
                .map(|fields| {
                    fields
                        .iter()
                        .map(|(name, address)| (name.to_string(), address.clone()))
                        .collect()
                })
                .unwrap_or_default();

            // THE UNION, KEYED ON THE FIELD NAME, AND WHICH SIDE WINS A TIE.
            //
            // The INDEX wins. That makes this a no-op for every field the index names -- which
            // today is every field -- and leaves the container contributing exactly the fields
            // the index does not name. A union that preferred the container would change the
            // address served for every read in the store, and a change whose purpose is to
            // measure is not entitled to do that.
            let mut resolved: Vec<(
                String,
                Option<std::sync::Arc<str>>,
                crate::block_store::ElementEntry,
            )> = Vec::with_capacity(indexed.len() + contained.len());
            let mut named_by_index: std::collections::HashSet<String> =
                std::collections::HashSet::with_capacity(indexed.len());
            for (component, address) in indexed {
                // A `None` COMPONENT NAMES NO FIELD, SO IT IS NOT SERVED AS ONE.
                //
                // THIS USED TO DEFAULT IT TO `""` AND SERVE IT. The paragraph here said a nameless
                // entry "is the whole-object door's answer for a block that belongs to the object
                // rather than to an element of it ... It is served exactly as it always was", and
                // that was survivable only while every hash entry carried a field name. Under one
                // entry a page NO hash entry carries one, so every page of the object was served as
                // a field called `""` whose value was the raw page FRAME.
                //
                // MEASURED, and it is the defect that refused this collapse twice: a two-field hash
                // answered FOUR entries -- `("f1", ..), ("f3", ..)` and two `("", b"TSCPG2\n...")`
                // -- which `conformance_oracle_matches_reference_model` reports against the
                // reference model and `a_command_of_one_type_answers_empty_for_a_key_of_another`
                // reports as a hash that is not the hash that was written. It also served `hash: 41`
                // of 40 on a forty-field corpus, which is one phantom per folded page.
                //
                // AN EMPTY FIELD NAME IS LEGAL, which is exactly why the absent one must not spell
                // it: defaulting here collides with a genuine `""` field and takes its value. This
                // is the same `unwrap_or_default()` correction the four derived-view arms already
                // carry, arriving on the READ path. The field a nameless page holds is named inside
                // the page's own frame and in `shard.hashes`, and the union below serves it from
                // there -- so nothing is lost by skipping, only a name that was never written.
                let Some(name) = component.as_deref().map(str::to_string) else {
                    continue;
                };
                named_by_index.insert(name.clone());
                resolved.push((name, component, address));
            }

            // THE MEASUREMENT. See `hash_read_divergence` for what a zero from it does and does
            // not mean; the short form is that a zero is a statement about the routes traffic
            // took and NOT a proof of completeness on the routes it did not.
            {
                let held: std::collections::HashSet<&str> =
                    contained.iter().map(|(name, _)| name.as_str()).collect();
                for name in named_by_index.iter().filter(|name| !held.contains(name.as_str())) {
                    hash_read_divergence::note(&key, name);
                }
                hash_read_divergence::note_read();
            }

            // The container's own fields that the index does not name. These are the reads the
            // union ADDS: without it they are not served at all, which is why the union is the
            // part that makes observing a divergence safe rather than merely informative.
            for (name, address) in contained {
                if !named_by_index.contains(&name) {
                    let component: std::sync::Arc<str> = std::sync::Arc::from(name.as_str());
                    resolved.push((name, Some(component), address));
                }
            }

            let entries = resolved
                .into_iter()
                .filter_map(|(name, component, address)| {
                    read_block_bytes(
                        cache,
                        block_store,
                        shard_id,
                        &address,
                        // The element the whole-object door handed back beside this address.
                        BlockIdentity::of(shard_id, "hash", &key, component.as_deref()),
                        Some(block_routing_bucket(
                            &key,
                            start_routing_bucket,
                            end_routing_bucket,
                        )),
                    )
                    .map(|value| (name, value))
                })
                .collect();
            CommandResponse::HashEntries { entries }
        }
        Command::HashLen { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                let _ = cache.invalidate_record(shard_id, "hash", &key);
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                // UNDER ONE ENTRY A PAGE THE INDEX NAMES NO FIELD, so a count of index entries is
                // THE INDEX NAMES NO FIELD, so a count of index entries is a count of PAGES, not
                // fields -- a wrong number rather than an empty answer. The resident map is durable
                // and keyed by field, so it is the count. The entry-counting arm this replaces is
                // deleted rather than left behind a condition: there is no state in which an entry
                // names a field for it to count.
                value: shard.hashes.get(&key).map(|fields| fields.len()).unwrap_or(0) as i64,
            }
        }
        Command::HashDelete { key, field } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            // Mirror hash2::Del: deleting the last field removes the whole key (DeleteObject
            // on empty). Leaving an empty field map behind makes the key still report as existing
            // (EXISTS=1, TYPE=hash) -- a phantom hash. That cleanup now lives inside
            // `RecordedHashContainer::remove_field`, so no arm can forget it.
            //
            // AND "THE MAP FORGOT IT" IS NOT "THE KEYSPACE FORGOT IT". The sentence above is true
            // and it is about ONE SOURCE: `record_exists_exact` ORs the bucket index in beside the
            // resident map, so the resident map going empty does not stop the key answering. A
            // gated removal keeps the object's live page entry -- correctly, while the page still
            // holds other members -- and for the LAST member that left the key enumerable with
            // nothing in it across KEYS, SCAN, DBSIZE, EXISTS, TYPE and EXPIRE. The last-element
            // arm in `recorded_map::remove_element` is what empties the second source.
            //
            // THE GENERAL FORM, because this asymmetry has now decided two fixes: a change that
            // touches "the index entry for this object" WITHOUT SAYING WHICH KIND will keep
            // getting it wrong. A LIVE entry claims a page with members on it; a TOMBSTONE records
            // that a named element was removed. The first becomes false when the last member goes;
            // the second stays true, and it is what makes the removal win a fold by append
            // position -- so taking it along undoes the removal it records. The gate itself got
            // this right by naming tombstones and not live entries; the first attempt at the
            // cleanup above got it wrong by marking the whole object deleted, and a churn test
            // caught it at zero tombstones where one was required.
            //
            // THE PARENTHESIS THAT USED TO END THIS NOTE SAID SETS DID NOT NEED THE CLEANUP,
            // and it was wrong about this engine: `record_exists_exact` reads
            // `shard.sets.contains_key(key)` on the line after the one it reads
            // `shard.hashes.contains_key(key)` on, so a set left holding an empty member map is
            // the same phantom by the same reader. `SetRemove` does the cleanup now too.
            // FROM THE POOL RATHER THAN MINTED, even though this name is about to be
            // dropped: the write already interned it, so this is a lookup, and minting
            // here would allocate a copy whose only use is to be compared and discarded.
            let field_handle =
                super::state::intern_shared(&mut shard.bucket_index.kind_pool, &field);
            mutated |= super::recorded_map::remove_element::<super::recorded_map::HashKind>(
                cache,
                block_store,
                shard,
                shard_id,
                &key,
                field.as_str(),
                field_handle,
                start_routing_bucket,
                end_routing_bucket,
                async_storage,
            );
            invalidate_if_cached(cache, CacheKey::hash(shard_id, &key, &field));
            CommandResponse::Empty
        }
        Command::SetAdd { key, member } => {
            remove_if_expired(shard, &key);
            let member_component = hex::encode(&member);
            let object_id = stable_block_object_id(shard_id, "set", &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // WHICH block of this object this element is. Computed BEFORE the append because the
            // append stamps it into the record header, and read off the block index -- which is
            // where the element's own previous block, if it has one, already states its position.
            //
            // AND THE QUESTION GOES TO THE MAP THAT IS KEYED BY THE MEMBER, inside the function.
            // `container_block_ordinal` used to pick an element's existing position out of the INDEX
            // by component, which a page-named entry does not carry -- so every rewrite fell
            // through to `highest + 1` and a member written five times left five pages instead of
            // overwriting one (measured: two pages and ordinals [0, 1] where one and [0] are
            // right). This arm therefore carried its own `shard.sets` lookup under the gate, with
            // the function called only when the map had no answer.
            //
            // THAT IS ONE BRANCH, NOT TWO, NOW. The function asks the resident map itself, so the
            // gated arm here and the ungated one computed the same value and the `if` decided
            // nothing -- a branch whose arms cannot differ, which no suite can report. The lookup
            // lives in the one place that owns the rule; see `state::container_block_ordinal`.
            let block_ordinal = crate::engine::state::container_block_ordinal(
                shard,
                "set",
                &key,
                &member_component,
            );
            // The block STATES which member it is -- see `container_blocks`. For a set the element
            // key IS the member and so is the value, which the frame stores once: #2017 measured
            // that redundancy and this is the stage that stops paying it twice.
            let block = crate::engine::container_blocks::single_element_block(
                "set",
                &member_component,
                &member,
            );
            if let Ok(address) = append_value_of_object(
                cache,
                block_store,
                shard_id,
                &block,
                Some(object_id),
                // The element this block holds -- the SAME expression that derived the ordinal
                // above and the object id beside it. All three name one block.
                Some(member_component.as_str()),
                Some(routing_bucket),
                async_storage,
                block_ordinal,
            ) {
                super::recorded_map::install_element::<super::recorded_map::SetKind>(
                    shard,
                    shard_id,
                    &key,
                    Some(member_component.clone()),
                    member.clone(),
                    address.clone(),
                    true,
                    address,
                );
                mutated = true;
            }
            let _ = cache.invalidate(&CacheKey::set_members(shard_id, &key));
            CommandResponse::Empty
        }
        Command::ZSetAdd { key, member, score } => {
            remove_if_expired(shard, &key);
            let biased = zset_score_bits(score);
            let existed = shard
                .zsets
                .get(&key)
                .and_then(|members| members.get(&member))
                .map(|(old_biased, _)| *old_biased);
            if existed == Some(biased) {
                // Same score: nothing to rewrite, and the answer is still "not new".
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            if let Some(old_biased) = existed {
                let old_component = zset_component(&member);
                remove_container_element(
                    cache,
                    block_store,
                    shard,
                    shard_id,
                    "zset",
                    &key,
                    &old_component,
                    start_routing_bucket,
                    end_routing_bucket,
                    async_storage,
                );
            }
            let component = zset_component(&member);
            let object_id = stable_block_object_id(shard_id, "zset", &key);
            let routing_bucket =
                block_routing_bucket(&key, start_routing_bucket, end_routing_bucket);
            // WHICH block of this object this element is. Computed BEFORE the append, because
            // the append stamps the ordinal into the record header AND onto the address it
            // returns from the same value -- assigning it afterwards would leave the two
            // disagreeing, which `decode_block_record` refuses as a page id mismatch.
            let block_ordinal = crate::engine::state::container_block_ordinal(
                shard,
                "zset",
                &key,
                &component,
            );
            // The block STATES which element it is -- see `container_blocks`. A zset's element key is
            // its biased score followed by its member, and the value IS that member, so the frame
            // stores the member once and says where inside the key it starts.
            let block =
                crate::engine::container_blocks::single_element_block("zset", &component, &member);
            if let Ok(address) = append_value_of_object(
                cache,
                block_store,
                shard_id,
                &block,
                Some(object_id),
                // The element this block holds -- the SAME expression that derived the ordinal
                // above and the object id beside it. All three name one block.
                Some(component.as_str()),
                Some(routing_bucket),
                async_storage,
                block_ordinal,
            ) {
                // THE STANDALONE UPSERT IS GONE BECAUSE THE OPERATION DOES IT -- the same
                // correction `ZSetIncrBy` and `ListPush` already carry (see their own notes). It
                // stood here filing the SAME block under the SAME component a second time, which
                // staged a second WAL outcome for one write: harmless while the component carried
                // the score twice over identically, and no longer harmless now that only
                // `install_element`'s own filing carries the score (through
                // `RecordedKind::outcome_value`) -- a replay of the OTHER, standalone-filed
                // outcome would have found no score to install and refused the whole shard load.
                // One call now does both, record first.
                super::recorded_map::install_element::<super::recorded_map::ZSetKind>(
                    shard,
                    shard_id,
                    &key,
                    Some(component.clone()),
                    member.clone(),
                    (biased, address.clone()),
                    true,
                    address,
                );
                mutated = true;
            }
            CommandResponse::Integer {
                value: i64::from(existed.is_none()),
            }
        }
        Command::ZSetScore { key, member } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Bytes { value: None },
                    mutated,
                };
            }
            CommandResponse::Bytes {
                value: shard
                    .zsets
                    .get(&key)
                    .and_then(|members| members.get(&member))
                    .map(|(biased, _)| zset_score_string(*biased).into_bytes()),
            }
        }
        Command::ZSetRemove { key, member } => {
            mutated |= drop_if_expired(cache, shard_id, shard, &key);
            // THE SCORE IS READ BEFORE THE REMOVAL RATHER THAN TAKEN FROM IT, and that is the
            // one ordering this conversion had to change rather than preserve.
            //
            // This arm used to drop the member from the resident map FIRST, because the removal's
            // own return value carried the biased score that the component is rendered from -- so
            // the resident mutation preceded the durable record, which is the order a recorded
            // container exists to make unrepresentable. Reading the score with `get` costs one
            // lookup and lets the record go first.
            let biased = shard
                .zsets
                .get(&key)
                .and_then(|members| members.get(&member))
                .map(|(biased, _)| *biased);
            match biased {
                None => CommandResponse::Integer { value: 0 },
                Some(biased) => {
                    let component = zset_component(&member);
                    mutated |= super::recorded_map::remove_element::<super::recorded_map::ZSetKind>(
                        cache,
                        block_store,
                        shard,
                        shard_id,
                        &key,
                        &component,
                        member.clone(),
                        start_routing_bucket,
                        end_routing_bucket,
                        async_storage,
                    );
                    CommandResponse::Integer { value: 1 }
                }
            }
        }
        Command::ZSetCard { key } => {
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: shard.zsets.get(&key).map_or(0, BTreeMap::len) as i64,
            }
        }
        Command::ZSetRange {
            key,
            start,
            stop,
            rev,
        } => {
            remove_if_expired(shard, &key);
            let mut ordered = zset_ordered_members(shard, &key);
            if rev {
                ordered.reverse();
            }
            let length = ordered.len() as i64;
            let resolve = |index: i64| if index < 0 { length + index } else { index };
            let from = resolve(start).max(0);
            let to = resolve(stop).min(length - 1);
            let members = if from > to || length == 0 {
                Vec::new()
            } else {
                ordered[from as usize..=to as usize]
                    .iter()
                    .flat_map(|(member, biased)| {
                        [member.clone(), zset_score_string(*biased).into_bytes()]
                    })
                    .collect()
            };
            CommandResponse::Members { members }
        }
        Command::ZSetRangeByScore {
            key,
            min,
            max,
            min_exclusive,
            max_exclusive,
            rev,
        } => {
            remove_if_expired(shard, &key);
            let min_bits = zset_score_bits(min);
            let max_bits = zset_score_bits(max);
            // The score test runs before the member bytes are copied, so a narrow window copies
            // a narrow window rather than the whole set.
            let mut ordered: Vec<(Vec<u8>, u64)> = zset_members_in_score_range(
                shard,
                &key,
                min_bits,
                max_bits,
                min_exclusive,
                max_exclusive,
            );
            if rev {
                ordered.reverse();
            }
            let members = ordered
                .into_iter()
                .flat_map(|(member, biased)| [member, zset_score_string(biased).into_bytes()])
                .collect();
            CommandResponse::Members { members }
        }
        Command::SeenCheck {
            key,
            member,
            window_ms,
        } => {
            // Lazy expiry, as every other kind's arms do. Without it a key reads as
            // live for the whole interval between its deadline and whichever sweep
            // round happens to collect it -- and these two fail CLOSED while it lasts:
            // a lapsed seen-set still answers "duplicate" and suppresses work that
            // should run, a lapsed bucket still answers "denied" and keeps rejecting a
            // caller whose limit was meant to have been discarded.
            remove_if_expired(shard, &key);
            let now = resolve_now_ms();
            let floor = now.saturating_sub(window_ms);
            // No block backs a seen-set; it lives in the index snapshot. So the outcome carries
            // the member and the moment, which is everything an apply needs.
            stage_meta_outcome(
                shard_id,
                "seen",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                Some(member.clone()),
                Some(now),
                false,
            );
            let seen = shard.seen.entry(std::sync::Arc::from(&key[..])).or_default();
            // Bounded sweep from the time-ordered front: enough to keep pace with any
            // sustained rate, never enough to stall a hot call on a huge backlog.
            for _ in 0..128 {
                match seen.by_time.first_key_value() {
                    Some(((seen_at, _), ())) if *seen_at < floor => {
                        let ((seen_at, expired), ()) =
                            seen.by_time.pop_first().expect("front exists");
                        if seen.by_member.get(&expired) == Some(&seen_at) {
                            seen.by_member.remove(&expired);
                        }
                    }
                    _ => break,
                }
            }
            let duplicate = seen
                .by_member
                .get(&member)
                .is_some_and(|seen_at| *seen_at >= floor);
            if !duplicate {
                if let Some(previous) = seen.by_member.insert(member.clone(), now) {
                    seen.by_time.remove(&(previous, member.clone()));
                }
                seen.by_time.insert((now, member), ());
            }
            mutated = true;
            CommandResponse::Integer {
                value: i64::from(duplicate),
            }
        }
        Command::SeenCard { key } => {
            // A count is a read like any other: a set whose deadline has passed counts zero,
            // not whatever it held when the deadline arrived. The removal is a real change to
            // the shard, so it is reported as one -- the same shape `StringGet` and
            // `CommonExists` use.
            if remove_if_expired(shard, &key) {
                mutated = true;
                return ExecuteOutcome {
                    response: CommandResponse::Integer { value: 0 },
                    mutated,
                };
            }
            CommandResponse::Integer {
                value: shard.seen.get(&key[..]).map_or(0, |seen| seen.by_member.len()) as i64,
            }
        }
        Command::BucketTake {
            key,
            tokens,
            capacity,
            refill_per_sec,
        } => {
            // Lazy expiry, as every other kind's arms do. Without it a key reads as
            // live for the whole interval between its deadline and whichever sweep
            // round happens to collect it -- and these two fail CLOSED while it lasts:
            // a lapsed seen-set still answers "duplicate" and suppresses work that
            // should run, a lapsed bucket still answers "denied" and keeps rejecting a
            // caller whose limit was meant to have been discarded.
            remove_if_expired(shard, &key);
            let now = resolve_now_ms();
            let current = shard.buckets.get(&key[..]).copied();
            let (allowed, remaining, retry_after_ms, next) =
                bucket_take(current, now, tokens, capacity, refill_per_sec);
            // The outcome is the bucket the take LEFT BEHIND -- tokens and the refill moment --
            // not the take itself. Re-running a take would recompute against a different clock;
            // installing the resulting bucket cannot.
            let mut bucket_state = Vec::with_capacity(16);
            bucket_state.extend_from_slice(&next.0.to_le_bytes());
            bucket_state.extend_from_slice(&next.1.to_le_bytes());
            stage_meta_outcome(
                shard_id,
                "bucket",
                &key,
                start_routing_bucket,
                end_routing_bucket,
                Some(bucket_state),
                None,
                false,
            );
            // The outcome is the bucket the take LEFT BEHIND -- tokens and the refill moment --
            // not the take itself. Re-running a take would recompute against a different clock;
            // installing the resulting bucket cannot.
            let mut bucket_state = Vec::with_capacity(16);
            bucket_state.extend_from_slice(&next.0.to_le_bytes());
