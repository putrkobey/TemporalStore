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
                key: "phantom-list".to_string(),
                member: b"m".to_vec(),
                left: false,
            },
            Command::ListPop {
                key: "phantom-list".to_string(),
                left: false,
            },
        ),
        (
            "set",
            "phantom-set",
            Command::SetAdd {
                key: "phantom-set".to_string(),
                member: b"m".to_vec(),
            },
            Command::SetRemove {
                key: "phantom-set".to_string(),
                member: b"m".to_vec(),
            },
        ),
    ];

    let mut rows: Vec<(&str, i64, i64, i64, i64)> = Vec::new();
    let mut phantom_kinds: Vec<&str> = Vec::new();

    for (kind, key, add, remove) in kinds {
        run(&engine, add);
        // THE CONTROL SIDE OF EVERY ROW: the fixture wrote something and the key is present for the
        // honest reason.
        let len_before = listed_len(&engine, kind, key);
        let exists_before = exists(&engine, key);
        assert_eq!(
            1, len_before,
            "{kind}: the fixture wrote {len_before} element(s), not 1, so the removal below has \
             nothing to prove"
        );
        assert_eq!(
            1, exists_before,
            "{kind}: EXISTS answered {exists_before} for a key holding one element, so this \
             fixture's EXISTS says nothing"
        );

        run(&engine, remove);
        let len_after = listed_len(&engine, kind, key);
        assert_eq!(
            0, len_after,
            "{kind}: the listing still reports {len_after} element(s) after the removal, so this \
             row never emptied its container"
        );
        let exists_after = exists(&engine, key);
        if exists_after != 0 {
            phantom_kinds.push(kind);
        }
        rows.push((kind, len_before, exists_before, len_after, exists_after));
    }

    println!("\n=== EXISTS after the last element of a container is removed ===");
    println!("  kind  len before  EXISTS before  len after  EXISTS after");
    for (kind, len_before, exists_before, len_after, exists_after) in &rows {
        println!(
            "  {kind:<5} {len_before:>11} {exists_before:>14} {len_after:>10} {exists_after:>13}"
        );
    }
    println!(
        "  {} of {} kind(s) report a key with no elements as present",
        phantom_kinds.len(),
        rows.len()
    );

    // THE DENOMINATOR. Four kinds exercised, each proven to have emptied.
    assert_eq!(
        4,
        rows.len(),
        "DENOMINATOR: {} kind(s) exercised, not the 4 this claim is about",
        rows.len()
    );
    assert!(
        phantom_kinds.is_empty(),
        "{:?} report a container key with no elements as present. `record_exists_exact` reads \
         `shard.<kind>.contains_key(key)`, so a removal that leaves an empty inner map behind \
         leaves a key that EXISTS answers 1 for and every listing answers empty for",
        phantom_kinds
    );
}

/// AND THE SAME PHANTOM ACCEPTS A DEADLINE, which is the reader behind `EXPIRE` rather than
/// `EXISTS`.
///
/// `CommonExpire` records a deadline only `for record_key in associated_record_keys(&key)` where
/// `record_exists_exact(shard, &record_key)`, and `ttl_ms` gates on the same function before it
/// reads `expires_at_ms`. So the phantom is not one reader answering oddly: it puts a live deadline
/// into `shard.expires_at_ms` for a key with nothing under it, and `TTL` reports that deadline
/// instead of the -2 that means "no such key".
///
/// THE CONTROL IS THE SAME KEY WITH A MEMBER STILL IN IT: `EXPIRE` must be accepted there, or this
/// test would pass against an `EXPIRE` that had simply stopped working.
///
/// rust-internal: command surface only
#[test]
fn an_emptied_set_does_not_accept_a_deadline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    // THE CONTROL: a set that still holds a member takes a deadline.
    run(
        &engine,
        Command::SetAdd {
            key: "ttl-control".to_string(),
            member: b"stays".to_vec(),
        },
    );
    run(
        &engine,
        Command::CommonExpire {
            key: "ttl-control".to_string(),
            ttl_ms: 600_000,
        },
    );
    let control_ttl = integer_of(&run(
        &engine,
        Command::CommonTtl {
            key: "ttl-control".to_string(),
        },
    ));
    assert!(
        control_ttl > 0,
        "the control set with a live member reports TTL {control_ttl}, so EXPIRE is not working \
         here and the subject below would pass for the wrong reason"
    );

    // THE SUBJECT: the same shape, emptied first.
    run(
        &engine,
        Command::SetAdd {
            key: "ttl-phantom".to_string(),
            member: b"goes".to_vec(),
        },
    );
    run(
        &engine,
        Command::SetRemove {
            key: "ttl-phantom".to_string(),
            member: b"goes".to_vec(),
        },
    );
    assert_eq!(
        0,
        listed_len(&engine, "set", "ttl-phantom"),
        "the subject set is not empty, so this test never built a phantom"
    );
    // NOT asserted ok: the refusal IS the fixed behaviour, and it is stronger than "no deadline was
    // recorded". Before the cleanup this answered ok and `TTL` then reported the deadline it had
    // recorded; now the key is genuinely absent and `CommonExpire` refuses it by name.
    let phantom_expire = try_run(
        &engine,
        Command::CommonExpire {
            key: "ttl-phantom".to_string(),
            ttl_ms: 600_000,
        },
    );
    let phantom_ttl = match try_run(
        &engine,
        Command::CommonTtl {
            key: "ttl-phantom".to_string(),
        },
    ) {
        Ok(response) => integer_of(&response),
        // A refusal here says the same thing -2 says, and more loudly.
        Err(_) => -2,
    };

    println!(
        "\n=== EXPIRE then TTL ===\n  control (one member) EXPIRE ok, TTL {control_ttl} ms\n  \
         emptied set         EXPIRE {:?}, TTL {phantom_ttl} (-2 means no such key)",
        phantom_expire.as_ref().map(|_| "ok")
    );

    assert!(
        phantom_expire.is_err(),
        "EXPIRE was ACCEPTED for a set with no members, so `record_exists_exact` can still see the \
         key in `shard.sets` and a deadline is now recorded against nothing"
    );
    assert_eq!(
        -2, phantom_ttl,
        "TTL answered {phantom_ttl} for a set with no members, so a deadline stands against a key \
         every listing reports as empty"
    );
}

// =================================================================================================
// 2. THE OTHER FOUR READERS, ASKED ON #2017's OVER-COMPLETE STATE
// =================================================================================================

/// One key's divergence fixture: `population` members written, `removed` of them removed, and the
/// pre-removal durable map captured so it can be put back the way a stale snapshot would.
struct Divergence {
    key: String,
    members: Vec<Vec<u8>>,
    removed: Vec<Vec<u8>>,
    persisted_before_removal: BTreeMap<Vec<u8>, ElementEntry>,
}

fn build_divergence(
    engine: &TemporalEngine,
    key: &str,
    population: usize,
    remove_count: usize,
) -> Divergence {
    let members: Vec<Vec<u8>> = (0..population)
        .map(|index| format!("{key}-member-{index:03}").into_bytes())
        .collect();
    for member in &members {
        run(
            engine,
            Command::SetAdd {
                key: key.to_string(),
                member: member.clone(),
            },
        );
    }
    // What `set_index_serde` would have serialized at this moment: the map as it stands, before any
    // removal. Captured off the live map, which is exactly what it serializes.
    let persisted_before_removal = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        shard
            .sets
            .get(key)
            .expect("the seeded key is in the resident map")
            .clone()
    };
    assert_eq!(
        population,
        persisted_before_removal.len(),
        "{key}: the pre-removal snapshot holds {} member(s), not the {population} written",
        persisted_before_removal.len()
    );
    let removed: Vec<Vec<u8>> = members.iter().take(remove_count).cloned().collect();
    for member in &removed {
        run(
            engine,
            Command::SetRemove {
                key: key.to_string(),
                member: member.clone(),
            },
        );
    }
    let live_now = resident_members(engine, key).len();
    assert_eq!(
        population - remove_count,
        live_now,
        "{key}: {live_now} member(s) in the map after removing {remove_count} of {population}, so \
         the fixture's removals did not land"
    );
    Divergence {
        key: key.to_string(),
        members,
        removed,
        persisted_before_removal,
    }
}

/// EVERY READER OF THE RESIDENT MAP, ASKED ON THE STATE #2017 BUILT.
///
/// TWO KEYS, AND THE DISCRIMINATOR IS A COUNT. `ghost` gets a stale snapshot put back under it, so
/// the merge is asked to resurrect; `control` gets its own real snapshot put back, so the merge is
/// asked for something and resurrects nothing. A one-key fixture would differ from its own fix only
/// in which address won a walk, which is the shape #2016's first mutation run survived.
///
/// # WHAT MOVED, AND WHY THE LOCK IS NOW SCOPED
///
/// The population each row was compared against came out of the index entry's component. The entry
/// stopped naming its element, so that read returns NOTHING for every key and the per-row control
/// fired -- correctly: it says in as many words that this fixture's page index says nothing. The
/// population is re-attributed to the SERVING PATH, which means releasing the shard lock to ask the
/// command surface, so the readers below run inside a scope and the comparison happens after it.
///
/// # AND THE MERGE RESURRECTS AGAIN, SO TWO ASSERTIONS ARE TURNED OVER RATHER THAN RE-GOLDENED
///
/// #2017's fix made `fill_absent_elements` ask "is there still a page at this address?". Its premise
/// was ONE PAGE PER ELEMENT, and the collapse removed it: with one index entry a page, a page
/// SURVIVES the removal of one of its elements, so the address stays live while the element on it is
/// dead. Measured here at ghost map 4 against a served 2.
///
/// SO THE READER-4 DENOMINATOR IS RE-ATTRIBUTED, NOT RE-GOLDENED. It read `assert_eq!(6,
/// named_by_map)` on the arithmetic "8 members written, 2 removed". The merge now restores the 2, so
/// the maps name all 8 -- and 8 is written as the two fixtures' own populations rather than as a
/// literal, because it is the fixture's number and not the store's.
///
/// AND READER 4'S SAFETY PROPERTY STILL HOLDS, FOR A BETTER REASON THAN BEFORE. Every address the
/// resident maps name is a live page -- including the resurrected ones, whose pages are live exactly
/// because the collapse keeps them. `compact_shard_blocks_relocating` therefore relocates nothing
/// that is not there, and the hazard reader 2 was asserted against -- re-rendering a component for a
/// member whose page was REMOVED -- is now unreachable rather than absent.
///
/// rust-internal: calls the engine's own reconcile and its map readers directly, then the commands
#[test]
fn the_resident_map_readers_asked_in_the_over_complete_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    // 4 written, 2 removed -> the stale snapshot is over-complete by 2.
    let ghost = build_divergence(&engine, "ghost", 4, 2);
    // 4 written, 0 removed -> its own snapshot is exact. THE CONTROL.
    let control = build_divergence(&engine, "control", 4, 0);

    let (rows, emitted_set_entries) = {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard 1 is loaded");

        // The load path's input: the durable map as an index snapshot predating the removals.
        shard
            .sets
            .insert_elements_for_test(&ghost.key, ghost.persisted_before_removal.clone());
        shard
            .sets
            .insert_elements_for_test(&control.key, control.persisted_before_removal.clone());

        let live_blocks: usize = shard
            .bucket_index
            .bucket_map
            .values()
            .map(|bucket| {
                bucket
                    .block_index
                    .values()
                    .filter(|block| !block.deleted)
                    .count()
            })
            .sum();
        assert!(
            live_blocks >= 6,
            "DENOMINATOR: {live_blocks} live page(s), so the derived view is too thin for the merge \
             below to be doing anything"
        );

        crate::engine::storage_bucket_internals::reconcile_secondary_views_from_bucket_index(
            &engine.block_store,
            shard,
            None,
        );

        let mut rows: Vec<(&str, BTreeSet<Vec<u8>>)> = Vec::new();
        for divergence in [&ghost, &control] {
            let in_map = shard
                .sets
                .get(&divergence.key)
                .map(|members| members.keys().cloned().collect::<BTreeSet<_>>())
                .unwrap_or_default();
            rows.push((
                if divergence.key == "ghost" {
                    "ghost"
                } else {
                    "control"
                },
                in_map,
            ));
        }

        // -----------------------------------------------------------------------------------------
        // READER 1: record_exists_exact -- KEY granularity, so an extra MEMBER cannot move it.
        // -----------------------------------------------------------------------------------------
        assert!(
            crate::engine::record_exists_exact(shard, "ghost"),
            "the ghost key has a live member and a live page, so EXISTS must be true here -- if \
             this fails the fixture is not the over-complete state this module is about"
        );
        println!(
            "  reader record_exists_exact       LATENT: key granularity \
             (`shard.sets.contains_key`), the key is in both populations"
        );

        // -----------------------------------------------------------------------------------------
        // READER 2: visit_model_live_blocks -- walks the map and re-renders the component.
        // -----------------------------------------------------------------------------------------
        let emitted_set_entries =
            crate::engine::storage_bucket_internals::collect_model_live_block_entries(shard)
                .into_iter()
                .filter(|entry| entry.kind.as_str() == "set" && &*entry.object_key == "ghost")
                .count();
        println!(
            "  reader visit_model_live_blocks   emitted {emitted_set_entries} live set entr(ies) \
