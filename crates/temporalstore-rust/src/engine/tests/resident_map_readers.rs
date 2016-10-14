// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHO ELSE READS THE RESIDENT CONTAINER MAP, AND WHICH OF THEM ANSWERS WRONG.
//!
//! # WHERE THIS STARTS
//!
//! #2017 drove that a resident container map can hold an element the live page index says is gone:
//! after a removal and a reconcile against a snapshot that predates it, `shard.sets` held 2 members
//! and the live page index held 1. It refused to serve `SMEMBERS` from that map for exactly that
//! reason, and enumerated the map's OTHER readers without asking any of them anything:
//!
//! ```text
//!     record_exists_exact                 EXISTS / TTL / EXPIRE / PERSIST
//!     collect_live_block_slab_ids          the reclaim live-slab set
//!     compact_shard_blocks_relocating      rewrites page addresses in place
//!     collect_upsert_index_items           resolves a member to build its WAL index item
//!     visit_model_live_blocks              re-renders the component; bucket release and rebuild
//! ```
//!
//! This module asks each of them, on a driven over-complete state rather than from the source. Two
//! of the five turn out to read the map at a granularity the over-completeness cannot reach; two
//! read it and are held off by something else, asserted here so a change that removes it fails the
//! build; and the FIRST one answers wrong today -- but not for the reason #2017's enumeration
//! suggests, and not through the state #2017 built.
//!
//! # THE LIVE ONE, AND IT IS NOT A MEMBER-LEVEL DEFECT
//!
//! `record_exists_exact` reads `shard.sets` at KEY granularity -- `shard.sets.contains_key(key)` --
//! so an extra MEMBER in the map cannot move its answer. The key is in both populations and EXISTS
//! is 1 either way. Asked on #2017's state it is correct, and this module asserts that rather than
//! implying it.
//!
//! What moves it is an EMPTY member map left under a live key, and `SetRemove` left one on every
//! removal:
//!
//! ```text
//!     if let Some(set) = shard.sets.get_mut(&key) {
//!         mutated |= set.remove(&member).is_some();
//!     }
//! ```
//!
//! The member goes, the key stays, and `record_exists_exact`'s `contains_key` then answers 1 for a
//! set with no members -- a key every listing reports as empty, `SCARD` as 0, and `EXISTS` as
//! present. `CommonExpire` gates on the same function, so a deadline is accepted for it too, and
//! `ttl_ms` reports that deadline rather than the -2 of a missing key.
//!
//! THREE OF FOUR KINDS ALREADY DID THIS, which is what makes it an omission rather than a design.
//! `HashDelete` removes the key when its last field goes, `ZSetRemove` when its last member goes,
//! `ListPop` when its last element goes. Only `SetRemove` did not -- and the note that explains the
//! cleanup at `HashDelete` said, in a parenthesis, that sets did not need one. They do: the reader
//! that makes a phantom hash observable is `record_exists_exact`, and it reads `shard.sets` in the
//! very next line of the same expression.
//!
//! THE DISCRIMINATOR IS A COUNT ACROSS THE FOUR KINDS, not one key's answer. A single-kind fixture
//! cannot tell "sets are the outlier" from "this is how containers behave", which is the shape
//! #2016's first mutation run was defeated by. `an_emptied_container_key_does_not_report_as_present`
//! drives all four, asserts each one was genuinely emptied, and fails on the COUNT of kinds that
//! still answer 1. Before the fix that count is 1 of 4; it is 0 of 4 now, with the four-kind
//! denominator asserted so a fixture that silently wrote nothing cannot pass.
//!
//! # THE OTHER FOUR, ASKED ON #2017's STATE
//!
//! `the_resident_map_readers_asked_in_the_over_complete_state` builds the divergence on TWO keys so
//! the states differ by a COUNT and not by which address wins a walk, and asks every reader.
//!
//! `visit_model_live_blocks` EMITTED THE GHOST. It walks `shard.sets` and re-renders each member's
//! component, so a resurrected member is emitted as a live page entry with a component naming it.
//! That feeds `collect_model_live_block_entries`, bucket release and the bucket-index rebuild.
//!
//! `collect_live_block_slab_ids` ABSORBED THE GHOST'S SLAB. It walks the model maps directly -- the
//! comment in `compact_shard_blocks_relocating` says so, as the reason it cannot share the
//! compaction preamble's walk -- so the ghost's `block_slab_id` joins the reclaim live set and a
//! slab is held against reclamation for a page nothing can reach. Conservative, so not data loss,
//! and real.
//!
//! `compact_shard_blocks_relocating` RELOCATED IT, and this is the one the source reads the other
//! way round until you find the right loop. Its preamble builds `collect_live_block_entries` reports,
//! and that function dispatches to the BUCKET-INDEX walk whenever the index is non-empty -- which is
//! what makes it look page-index-bounded. The relocation itself is not those reports. It is
//! `for (key, members) in shard.sets.iter_mut()`, one arm per model, handing every address the map
//! names to `compact_block_addresses` to be read off the old slab, appended to the fresh one and
//! rewritten in place. So the RESIDENT MAP is compaction's work list: a ghost is a page compaction
//! reads, copies onto every slab it ever rolls, and keeps alive for good. The test asserts the
//! property that makes the loop safe -- every address the maps name is a live page -- over all five
//! element-bearing models, because the loop has an arm for each.
//!
//! `collect_upsert_index_items` WOULD build an item for the ghost -- asked directly with the ghost's
//! component it resolves the address straight out of `shard.sets` and emits a page item, which on
//! replay would re-install an index entry for a page that was removed. Nothing asks it: the
//! component list it is driven from comes from `command_upsert_components`, which has arms for the
//! six ADD/SET commands and none for any removal. `SetRemove`'s component is produced by
//! `command_removed_component` instead, on the `collect_command_index_items_for` path. LATENT, and
//! the test asserts the exact condition -- `command_upsert_components` answers `None` for
//! `SetRemove` -- so an arm added there fails this module.
//!
//! # WHETHER THE MERGE CAN BE MADE UNABLE TO RESURRECT
//!
//! It can, and #2005 already wrote the question. #2005 fixed a resurrection on this same function
//! by applying the delta fold's carried elements ONCE, after the page index had settled, asking
//! "is there still a page at the address this element was carried with?" -- `live_block_key` over the
//! finished bucket index. That fix was complete FOR THE CARRY. It left the other input to the same
//! merge unasked.
//!
//! SO THIS IS A DIFFERENT MECHANISM, NOT AN INCOMPLETE FIX, and the distinction is the whole reason
//! the question was worth asking. #2005's resurrection came from `fold_carried_container_elements`
//! putting an element into the durable map that a later record's page removal then contradicted --
//! an input BUILT during the load. #2017's comes from the PERSISTED map itself,
//! `set_index_serde`'s `(member bytes, address)` pairs, being older than the page index it is merged
//! with -- an input READ off disk. One is a fold ordering bug and it is fixed; the other is a
//! snapshot-age property of the durable map and `fill_absent_elements` had no filter for it at all.
//! `both_inputs_to_the_merge_now_answer_the_same_live_block_question` drives the two side by side.
//!
//! `fill_absent_elements` now asks #2005's question of the persisted map too. That KEEPS #1989's
//! element and DROPS #2017's, which is what makes it the right question rather than a narrowing:
//! #1989's case is a page whose component cannot be decoded, so the derived view cannot name the
//! element while the page is RIGHT THERE in the index -- live at its address, kept. #2017's case is
//! a page that is gone from the index -- not live at its address, dropped. The function's stated job,
//! "keep every durable element the derived view could not produce", becomes "keep every durable
//! element the derived view could not produce AND whose page is still there".
//!
//! A PERSISTED KEY WITH NO SURVIVING ELEMENT NOW GETS NO ENTRY AT ALL. The old body ran
//! `derived.entry(key).or_default()` before looking at a single element, so a persisted key whose
//! every page was gone -- and a persisted key holding an empty map -- installed an EMPTY member map
//! under a live key: the same phantom `SetRemove` was leaving, arriving by reload instead. The entry
//! is created only when an element survives now.
//!
//! # THE BRANCH THAT DID NOT REACH THE MERGE AT ALL
//!
//! The three merges were gated on a `saw_*` flag, and the flag was not the protection it looks like:
//!
//! ```text
//!     if saw_sets {
//!         let persisted = std::mem::take(&mut shard.sets);
//!         shard.sets = fill_absent_elements(sets, persisted);
//!     }
//! ```
//!
//! `saw_sets` is assigned at the TOP of the set arm, BEFORE the component decode. So it never meant
//! "at least one set entry decoded" -- `saw_hashes` means that, because #2016 moved it inside its
//! match for a reason that applies to the one arm with no durable map behind it. `saw_sets == false`
//! means the settled page index holds NO SET PAGE AT ALL, and the skip then left the deserialized
//! persisted map standing WHOLE: unfiltered, every stale entry intact. A store whose set pages were
//! all removed after its last base-index write reloaded with a full resident map and an empty page
//! index, which is the over-complete state by the one route the merge never saw.
//!
//! The three merges are UNCONDITIONAL now, so the filter answers that arm too. Running them with an
//! empty derived view is safe for the same reason the filter is safe at all: it drops only an element
//! whose address matches no live page in the finished index, which trusts the index exactly as far as
//! this function already trusts it two arms up, where `shard.strings = strings` and
//! `shard.hashes = hashes` assign the derived view WHOLESALE.
//! `the_merge_runs_even_when_no_set_block_survived` drives both arms.
//!
//! # AND STILL NO SCAN SHIPS, FOR A SMALLER REASON THAN THE ONE THIS OPENED WITH
//!
//! The map can no longer hold a member the page index does not, which was #2017's whole objection,
//! and every reader above now agrees with the index. It does not follow that `SMEMBERS` can be served
//! from it, and the residual is in the other direction: a live page whose component cannot be decoded
//! is in the index and, unless the persisted map happens to hold it, in NO map -- #1989's case, where
//! the derived view cannot name the element. A listing served from `shard.sets` would MISS it, and a
//! listing that under-reports is worse than the reads it saves. Nothing in this engine writes a
//! non-hex set component, so that is latent, and it is latent on the write path rather than on this
//! function.
//!
//! So the saving #2017 measured -- `SMEMBERS` at 1.00 page reads per member, flat across a 32x width
//! change, against a `zset` control at 0.00 -- is now UNBLOCKED rather than taken. It is a
//! serving-path change with its own measurement and its own gate, and it does not belong in a commit
//! that moves a recovery path. Stated here so the next reader does not have to re-derive whether the
//! map is ready: it is, on the over-reporting side, which is the side that made it unsafe.
//!
//! # NOT COVERED, stated so the cover is not read as total
//!
//! The `zset` and `list` merges take the same new filter and the same two tests exercise the set
//! arm only for the reader sweep; `both_inputs_to_the_merge_now_answer_the_same_live_block_question`
//! covers all three arms for the filter itself. `hashes` has no durable map and no merge, so it has
//! no persisted input to filter. The page-read saving is quoted from #2017 and not re-derived here.
#![allow(clippy::all)]
use super::*;
use std::collections::{BTreeMap, BTreeSet};

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
        table_name: "resident-map-readers".to_string(),
        shard_uri: "local://resident-map-readers/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
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

fn run(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(
        response.status.ok,
        "the fixture command failed: {response:?}"
    );
    response.response
}

/// The same, WITHOUT asserting the status, for the one call whose refusal is the finding. Returns
/// `Ok(response)` or `Err(status code)` so a refusal can be asserted by name rather than by absence.
fn try_run(engine: &TemporalEngine, command: Command) -> Result<crate::types::CommandResponse, String> {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    if response.status.ok {
        Ok(response.response)
    } else {
        Err(response.status.code)
    }
}

/// The integer a command answered, or a panic naming what came back instead -- an `unwrap_or(0)`
/// here would read a shape change as a zero and every count below would quietly agree with it.
fn integer_of(response: &crate::types::CommandResponse) -> i64 {
    match response {
        crate::types::CommandResponse::Integer { value } => *value,
        other => panic!("expected an integer response, got {other:?}"),
    }
}

/// How many members a listing returned, for the four container kinds.
fn listed_len(engine: &TemporalEngine, kind: &str, key: &str) -> i64 {
    match kind {
        "hash" => integer_of(&run(
            engine,
            Command::HashLen {
                key: key.to_string(),
            },
        )),
        "zset" => integer_of(&run(
            engine,
            Command::ZSetCard {
                key: key.to_string(),
            },
        )),
        "list" => integer_of(&run(
            engine,
            Command::ListLen {
                key: key.to_string(),
            },
        )),
        "set" => match run(
            engine,
            Command::SetMembers {
                key: key.to_string(),
            },
        ) {
            crate::types::CommandResponse::Members { members } => members.len() as i64,
            other => panic!("SetMembers answered {other:?}"),
        },
        other => panic!("no listing for kind {other}"),
    }
}

fn exists(engine: &TemporalEngine, key: &str) -> i64 {
    integer_of(&run(
        engine,
        Command::CommonExists {
            key: key.to_string(),
        },
    ))
}

/// The members `shard.sets` holds for one key, straight off the resident map.
fn resident_members(engine: &TemporalEngine, key: &str) -> BTreeSet<Vec<u8>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    shard
        .sets
        .get(key)
        .map(|members| members.keys().cloned().collect())
        .unwrap_or_default()
}

/// The members the SERVING PATH answers for one set key -- the population a listing hands back.
///
/// IT REPLACED `page_index_members`, WHICH CANNOT EXIST ANY MORE, and that is recorded here because
/// two arms in this module turn on it. The old helper decoded members out of the component each page
/// was filed under; the index entry stopped naming its element, so
/// `bucket_index_component_block_addresses` hardcodes `None` in the pair's first slot on all three
/// of its return paths and the walk now hands back addresses and nothing else. The helper returned
/// an empty set for every key, which is what fired the per-row controls.
///
/// AND THE DERIVED SIDE OF THE MERGE IS EMPTY, measured by handing `fill_absent_elements` an empty
/// persisted map and finding `shard.sets` empty afterwards. Rebuilding a set's members from the live
/// page index needs the component the entry no longer carries, so nothing arrives from that side:
/// the resident set map is wholly the durable snapshot's, and the address test is the only filter
/// there is. That is why these arms compare the map against the SERVING path and not against a
/// second derivation of the same thing.
///
/// AND THE PAGE PAYLOAD IS NOT THE SUBSTITUTE, measured rather than argued: a removal leaves the
/// element's page live and still stating the element, holding the removal in a SEPARATE tombstone
/// page, so a payload-derived population OVER-REPORTS and would agree with the resident map -- which
/// would make the controls pass and the finding disappear. The serving path is the authority on
/// which members a key holds, so it is the one asked. Asking it means the shard lock must be free.
fn served_members(engine: &TemporalEngine, key: &str) -> BTreeSet<Vec<u8>> {
    match run(
        engine,
        Command::SetMembers {
            key: key.to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members.into_iter().collect(),
        other => panic!("SetMembers answered {other:?} rather than members"),
    }
}

// =================================================================================================
// 1. THE LIVE READER: A KEY THAT HOLDS NOTHING AND STILL REPORTS AS PRESENT
// =================================================================================================

/// AN EMPTIED CONTAINER KEY MUST NOT REPORT AS PRESENT, AND A SET'S DID.
///
/// `record_exists_exact` -- behind `EXISTS`, `TTL`, `EXPIRE` and `PERSIST` -- ORs
/// `shard.sets.contains_key(key)` in beside its bucket-index answer. `SetRemove` removed the member
/// from the inner map and left the outer key, so after the last member went the key was still in
/// `shard.sets` with an empty member map and `EXISTS` answered 1 for it.
///
/// ALL FOUR KINDS, AND THE ASSERTION IS ON THE COUNT. One kind cannot distinguish "sets are the
/// outlier" from "this is what containers do here", and `HashDelete`'s own note asserted the former
/// in a parenthesis. Each row asserts its container was genuinely emptied first -- a fixture whose
/// removal did nothing would report `EXISTS = 1` for the honest reason and pass a weaker check.
///
/// rust-internal: command surface only
#[test]
fn an_emptied_container_key_does_not_report_as_present() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    // (kind, key, add, remove)
    let kinds: [(&str, &str, Command, Command); 4] = [
        (
            "hash",
            "phantom-hash",
            Command::HashSet {
                key: "phantom-hash".to_string(),
                field: "f".to_string(),
                value: b"v".to_vec(),
            },
            Command::HashDelete {
                key: "phantom-hash".to_string(),
                field: "f".to_string(),
            },
        ),
        (
            "zset",
            "phantom-zset",
            Command::ZSetAdd {
                key: "phantom-zset".to_string(),
                member: b"m".to_vec(),
                score: 1.5,
            },
            Command::ZSetRemove {
                key: "phantom-zset".to_string(),
                member: b"m".to_vec(),
            },
        ),
        (
            "list",
            "phantom-list",
            Command::ListPush {
