// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE RECONCILE DOES TO AN ELEMENT THE BUCKET INDEX DOES NOT NAME.
//!
//! Every test here was written to CONFIRM a hazard and every one refuted it. They are kept because
//! the refutation is the result: a later change that makes the load path name-sensitive breaks
//! these, loudly, instead of quietly emptying the resident maps.
//!
//! THE PREDICTION THAT FAILED. A context node is installed through
//! `install_element_staged_only::<HashKind>`, whose emitter files a staged outcome and no outcome
//! record of kind `hash`; the recovery arm installs through `replay_install_element`, the exception
//! only `HashKind` declares. Both were documented as writing a block that "is never registered in
//! the bucket index". If that were true the element would be container-only, the index-derived base
//! could not name it, and `fill_absent_elements` would refuse it as a resurrection.
//!
//! IT IS NOT TRUE, AND THE INDEX SAYS SO. Asked directly, `bucket_index` holds
//! `("hash", Some("meta"), deleted=false)` for a node written through the command path --
//! `append_value` files the block from the object id, `CONTEXT_NODE_FIELD` and the routing bucket.
//! What is deliberately absent is the OUTCOME record under kind `hash`, which is a different thing
//! and was being conflated with the index entry.
//!
//! AND A GENUINELY CONTAINER-ONLY ELEMENT SURVIVES ANYWAY, which is the finding the collapse work
//! depends on. `LiveBlockKey` is `(slab_id, offset, length)`: a PAGE identity carrying no element,
//! component or object key. So `fill_absent_elements` asks only whether the page is still there,
//! never whether the index names this element -- and an element the index has never heard of is
//! kept as long as its page is live.
//!
//! WHY EACH CONTROL IS HERE, because a survival test without them asserts nothing:
//!
//!   * AN INDEX-REGISTERED FIELD, in every arm. Without it a reload that lost EVERYTHING would look
//!     identical to the defect being probed.
//!   * A DEAD-PAGE ADDRESS, in the arms that claim survival. An inert filter and a permissive one
//!     are indistinguishable from a single passing case; the dead-page element must be REFUSED, and
//!     that is what proves the filter discriminates rather than waving everything through.
//!
//! ONE ASSERTION HERE WAS INVERTED RATHER THAN RE-GOLDENED, and the distinction is the point.
//! `a_container_only_element_survives_the_reconcile_when_its_block_is_still_live` was first written
//! as `..._is_refused_by_the_reconciles_live_address_filter`, asserting refusal. It failed. The
//! assertion was changed to the measured behaviour AND THE NAME WAS CHANGED WITH IT. A test that
//! keeps its name through a semantic inversion is how `wal.rs` once ended up asserting that one
//! equals two: the name went on describing the old belief while the body asserted the opposite, and
//! nothing in the suite could see the contradiction. Restate, and rename; do not re-golden.
//!
//! SCOPE, LABELLED. The end-to-end path is measured for `hashes` only, because
//! `replay_install_element` is gated on `ReplaysInstallsUnrecorded` and `HashKind` is the only kind
//! that declares it. For the other three kinds the DECIDING FUNCTION is measured directly, with
//! each kind's own element and value type. That the whole load path then behaves the same for them
//! follows by inference from shared generic code, and is left as an inference rather than claimed
//! as a measurement.
//!
//! STILL OPEN, AND DELIBERATELY NOT TESTED HERE. Whether the collision rule in
//! `fill_absent_elements` -- `insert_element_if_absent`, under which the index-derived address wins
//! -- is reachable at all. It needs the same `(key, element)` in both sources at DIFFERENT
//! addresses, and no path has been shown to produce that. A test asserting today's collision
//! outcome would pass because the state is unreachable, which is a guard over an unreachable defect
//! wearing the costume of a correctness test. Unproven is written down as unproven.

use crate::engine::TemporalEngine;
use crate::types::Command;

const NARROW_END: u32 = 1023;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "cnsr".to_string(),
        shard_uri: "local://cnsr/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: NARROW_END,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
        response.status
    );
}

fn node_at(node_hash: u64) -> crate::types::ContextNode {
    crate::types::ContextNode {
        node_hash,
        parent_hash: 0,
        kind: 1,
        canonical_name: format!("node_{node_hash:08}"),
        l0: "a node".to_string(),
        status: 0,
        last_event_time_ms: 0,
        l1_ref: String::new(),
        raw_metadata_ref: String::new(),
        vector: Vec::new(),
        embedding_model_hash: 0,
        embedding_updated_at_ms: 0,
        summary_vector: Vec::new(),
        summary_vector_valid_from_ms: 0,
        summary_vector_model_hash: 0,
    }
}

fn run(engine: &TemporalEngine, commands: Vec<Command>) {
    let response = engine.batch_execute(crate::types::BatchExecuteRequest {
        shard_id: 1,
        commands,
    });
    assert!(response.status.ok, "the batch must ack: {:?}", response.status);
}

/// Does `ContextGetNode` answer for this node?
fn node_is_served(engine: &TemporalEngine, node_hash: u64) -> bool {
    let response = engine.batch_execute(crate::types::BatchExecuteRequest {
        shard_id: 1,
        commands: vec![Command::ContextGetNode {
            tenant_hash: 7,
            node_hash,
        }],
    });
