// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A DURABLE MAP MUST OUTRANK A NAME DERIVED FROM IT -- AND THERE IS NO NAME LEFT TO OUTRANK.
//!
//! # THE DEFECT, AND WHY IT IS NOW UNREACHABLE RATHER THAN FIXED
//!
//! `reconcile_secondary_views_from_bucket_index` rebuilt the model maps from the bucket index, and
//! for three kinds it recovered an element's identity by PARSING the component name the write path
//! had spelled it into -- then assigned the result over the top:
//!
//! ```text
//!     if saw_lists { shard.lists = lists; }
//!     if saw_zsets { shard.zsets = zsets; }
//!     if saw_sets  { shard.sets  = sets;  }
//! ```
//!
//! All three of those maps are PERSISTED. So a stored value was being reconstructed from a second
//! copy of itself rendered as text, and the durable copy had no say: a zset written at score 7.5
//! came back at 99.25 when only its NAME was changed. The merge that replaced the assignment is
//! what this module was built to hold.
//!
//! THE SECOND COPY IS GONE. `BlockIndex` has no element-name field -- all four container kinds file
//! one entry a page -- so there is no text to parse and
//! `rebuild_unserialized_model_maps_from_bucket_index` passes an EMPTY derived view, keeping only
//! its live-address filter. The four resident maps are durable (`#[serde(default)]`) and are the
//! sole authority for which elements exist.
//!
//! SO EVERY ARM THAT PLANTED A DISAGREEMENT BETWEEN THE TWO SOURCES HAD ITS SUBJECT BECOME
//! UNREPRESENTABLE, and each is restated to say so rather than relaxed to pass. The four that moved,
//! and what each says now:
//!
//!   * `a_durable_zset_score_has_no_name_left_to_disagree_with_it` -- no component text of any
//!     shape reaches a served index, counted over the real files with the object key as the control
//!     that makes the zero mean "absent".
//!   * `an_element_name_cannot_be_made_unreadable_because_the_index_holds_none` -- the corruption
//!     this module's one load-bearing arm performed cannot be performed; there is no name to
//!     corrupt.
//!   * `an_element_the_durable_map_does_not_hold_does_not_come_back` -- the repair case is gone BY
//!     DESIGN, and that arm carries the three mechanisms that make the state unreachable in
//!     production rather than merely absent in this fixture.
//!   * `an_empty_zset_member_is_a_whole_component_and_survives_the_fold_shape` -- its length
//!     boundary is re-attributed to the PAGE, which is where a component still exists.
//!
//! A TEST PROVING A DEFECT UNREACHABLE IS WORTH MORE THAN ONE PROVING IT CURRENTLY ABSENT, which is
//! why these are restatements and not deletions: each one now fails if an element name returns to
//! a page index entry, which is the single change that would make the whole defect class reachable
//! again.
//!
//! # WHAT EACH DURABLE MAP HOLDS, AND WHICH KINDS HAVE ONE
//!
//! ```text
//!     set     set_index_serde      (member bytes, address)                 the member's identity
//!     zset    zset_index_serde     (member bytes, (score, address))        identity AND score
//!     list    plain serde          (i64 sequence, address)                 the sequence
//!     string  #[serde(skip)]       nothing                 no component at all; the key IS identity
//!     hash    serde(default)       the field name          the component IS the caller's field name
//! ```
//!
//! HASH HAS CHANGED GROUPS, AND THE RULE BELOW HAD TO BE RE-DERIVED RATHER THAN ITS ROW CORRECTED.
//! This table read `hash  skip_serializing  nothing written`, and the sentence under it -- "three
//! kinds have a durable copy of what their name spells and two genuinely do not" -- counted hash as
//! one of the two. `ShardState::hashes` carries `#[serde(default)]` now and is written, so hash has
//! a durable copy of its field names and the count is FOUR and one. Correcting the row alone would
//! have left a rule that no longer followed from its own table.
//!
//! What the re-derivation changes, and what it does not: a STRING still has no component, so for it
//! deriving is not a second copy of anything and it is still left alone. A HASH now has a durable
//! copy -- which is why `an_unnamed_hash_block_is_skipped_and_the_durable_map_keeps_the_field` below
//! can pass at all, and that test already says so, naming the six-fields-served-as-zero shape as
//! what the durability fixed. So the hash arm is in the same position as the three spelled kinds:
//! the derived view decides which elements exist, and the durable map keeps what a name cannot
//! supply. It is left alone for a DIFFERENT reason than a string is -- not "there is nothing to
//! outrank it with", but "the component is the caller's own text rather than something this engine
//! rendered, so there is no parse to get wrong."
//!
//! # WHY THE RULE IS NOT THE ONE `control_state` USES
//!
//! The `control_state` arm keeps the persisted series wholesale for every key it has, saying why: "the
//! serialized i64 series is authoritative (the page is a copy of it)". Copying that rule here would be
//! wrong. `apply_key_states` folds `features` and the control-state maps out of the delta log and NOT
//! `sets`, `zsets` or `lists`.
//!
//! THERE ARE TWO CHANNELS AND THAT SENTENCE DESCRIBES ONE OF THEM. It used to continue "so for these
//! three the derived view is the ONLY path by which a folded element arrives", which is true of
//! `apply_key_states` and false of the fold. The fold path also collects the record's
//! `CARRIED_CONTAINER_FIELDS` -- `set_elements`, `zset_elements`, `list_elements` and `hash_fields`
//! -- and `fold_carried_container_elements` merges all four into their durable maps, gated on the
//! pages the finished fold actually left behind. So a folded element of any of those four kinds
//! arrives by BOTH routes, and `engine::tests::fold_hash_map_completeness` measures that for hash:
//! after a fold the durable map and the page index name the same field set, in both directions.
//!
//! Naming one function's behaviour as the whole path's is how this was read as "the derived view is
//! the only path" by three separate documents. Both channels are named here so the next reader has
//! to meet the second one.
//!
//! The rule WAS per ELEMENT: the derived view decided which elements exist and which page backs
//! each, because it reflected the fold; the durable map supplied what the name merely re-spelled,
//! and kept any element the derived view could not produce.
//!
//! IT IS NOT A RULE ABOUT TWO SOURCES ANY MORE, because for a container element there is only one.
//! The derived view produces NOTHING: an entry has no element name, so it cannot say which elements
//! exist, which page backs one, or what a name re-spells. The resident map answers all three. What
//! survives of the rule is its live-address FILTER -- `reconcile_from_durable` is
//! `fill_absent_elements(derived, persisted, live, refused)`, and the `live` set still drops a
//! durable element whose page the settled index does not hold. That is the half
//! `length_answer_and_listing_agree` drives from the other side, and it is why the call is not
//! simply deleted: without it a reload would keep MORE than it should, not less.
//!
//! # AND THREE SILENT GUESSES BECAME SKIPPED READS, THEN BECAME IMPOSSIBLE
//!
//! Each arm handled an unreadable name differently and none of them said so. A set's became
//! `unwrap_or_default()` -- the EMPTY member, a real value that then took a genuine element's
//! address. A list's became SEQUENCE ZERO, a real position whose entry it overwrote. A zset's was
//! dropped. They were made skips, and counted, and the skip was safe precisely because the durable
//! map kept the element.
//!
//! THERE IS NOW NO NAME TO GUESS FROM, so none of the three can fire. The guess that remains is one
//! layer down: a container page spells its elements, and an element key the page's own spelling
//! cannot render is counted as `unrenderable_items` by `derive_membership` rather than defaulted.
//! The empty-member assertion in the set arm below is kept pointing at exactly that, because it is
//! the same defect at a new door.
//!
//! # WHY THIS IS NOT THEORETICAL
//!
//! A list component is a single `u64` spelled in hexadecimal, and #1985 measured that 100% of list
//! names are also well-formed values of a different meaning. A view built by parsing names is one
//! ambiguity away from a wrong value, and the test below shows the wrong value winning over the
//! durable one.

#![allow(clippy::all)]
use super::*;

const OPERATOR_END: u32 = crate::DEFAULT_END_ROUTING_BUCKET;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "durable-outranks".to_string(),
        shard_uri: "local://durable-outranks/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
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

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

fn read(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "a read failed: {response:?}");
    response.response
}

/// The component name a zset member is filed under, as the write path spells it.
fn zset_name(member: &[u8]) -> String {
    hex::encode(member)
}

/// The component name a zset member USED TO be filed under, before the score left it. Kept only
/// for `a_durable_zset_score_outranks_a_component_name_that_disagrees`, to prove a needle shaped
/// like the retired encoding no longer appears anywhere in a served index.
fn zset_name_with_retired_score_prefix(score: f64, member: &[u8]) -> String {
    format!(
        "{:016x}{}",
        crate::engine::execute_on_shard::zset_score_bits(score),
        hex::encode(member)
    )
}

/// Replace every occurrence of `from` with `to` inside a served index, whatever payload it carries.
///
/// `None` when the file holds no such text. Both codecs are handled and the JSON one is compressed,
/// so a byte search over the frame would find nothing and silently report success. EVERY occurrence,
/// because `block_index_written_key` renders the component into the map key the index is serialized
/// under as well as into the entry -- replacing one would leave the two naming different elements,
/// which is a corrupt index rather than a controlled experiment.
fn swap_in_index(bytes: &[u8], from: &str, to: &str) -> Option<(Vec<u8>, usize)> {
    const MAGIC: &[u8] = b"TSIDX\x01";
    let swap = |plain: &[u8]| -> Option<(Vec<u8>, usize)> {
        let needle = from.as_bytes();
        let mut out = Vec::with_capacity(plain.len());
        let mut swaps = 0usize;
        let mut at = 0usize;
        while at < plain.len() {
            if plain[at..].starts_with(needle) {
                out.extend_from_slice(to.as_bytes());
                at += needle.len();
                swaps += 1;
                continue;
            }
            out.push(plain[at]);
            at += 1;
        }
        (swaps > 0).then_some((out, swaps))
    };
    if !bytes.starts_with(MAGIC) {
        return swap(bytes);
    }
    let codec = *bytes.get(MAGIC.len())?;
    let payload = &bytes[MAGIC.len() + 1..];
    let (header, body) = match codec {
        1 => (0usize, payload),
        2 => (4usize, payload.get(4..)?),
        _ => return None,
    };
    let plain = zstd::stream::decode_all(body).ok()?;
    let (swapped, swaps) = swap(&plain)?;
    let recompressed = zstd::stream::encode_all(swapped.as_slice(), 3).ok()?;
    let mut out = Vec::with_capacity(MAGIC.len() + 1 + header + recompressed.len());
    out.extend_from_slice(MAGIC);
    out.push(codec);
    if header == 4 {
        out.extend_from_slice(&payload[..4]);
    }
    out.extend_from_slice(&recompressed);
    Some((out, swaps))
}

/// Swap a name across every index file, and answer how many occurrences moved.
pub(super) fn swap_across_index_files(indexes: &std::path::Path, from: &str, to: &str) -> usize {
    let mut total = 0usize;
    for entry in std::fs::read_dir(indexes).expect("the index directory exists") {
        let path = entry.expect("a directory entry").path();
        if !path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&path).expect("the index reads");
        if let Some((swapped, swaps)) = swap_in_index(&bytes, from, to) {
            std::fs::write(&path, &swapped).expect("the index rewrites");
            println!("  swapped {swaps} occurrence(s) in {:?}", path.file_name());
            total += swaps;
        }
    }
    total
}

/// How many times a name appears across every index file, with NO mutation.
///
/// SHARES THE SUBJECT'S MATCHER EXACTLY, which is the whole reason it exists rather than a byte
/// search of its own. It asks [`swap_in_index`] to swap the needle with ITSELF, so the count comes
/// from the same decompress-and-scan that [`swap_across_index_files`] performs and the bytes it
/// writes back are the bytes it read. A separate search would miss a compressed frame entirely and
/// report a confident zero -- and a zero is exactly what the arms below are asserting, so the
/// control that makes a zero mean "absent" has to read the same set as the subject.
pub(super) fn count_across_index_files(indexes: &std::path::Path, needle: &str) -> usize {
    let mut total = 0usize;
    for entry in std::fs::read_dir(indexes).expect("the index directory exists") {
        let path = entry.expect("a directory entry").path();
        if !path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&path).expect("the index reads");
        if let Some((_unchanged, found)) = swap_in_index(&bytes, needle, needle) {
            total += found;
        }
    }
    total
}

// =============================================================================================
// 1. THE TEST THAT IS THE WHOLE POINT
// =============================================================================================

/// A DURABLE ZSET SCORE HAS NO NAME LEFT TO DISAGREE WITH IT.
///
/// RESTATED A SECOND TIME, AND THIS TIME THE DIVERGENCE CANNOT BE REPRESENTED AT ALL. The original
/// experiment -- the one that found the defect -- wrote a zset at 7.5, changed its served-index
/// component to spell 99.25, and required the reload to answer 7.5 from the durable `zsets` map.
/// The first restatement observed that a zset component is `hex::encode(member)` with no score term
/// in it, asserted a score-shaped needle swaps ZERO times, and used a MEMBER-shaped needle as the
/// control proving that zero meant "absent" rather than "the scan found nothing in anything".
///
/// THAT CONTROL IS WHAT FAILED, and it was right to: "the member-shaped needle was not found in any
/// index file, so the zero above proves nothing". `BlockIndex` has no element-name field, so a
/// served index carries NO component text of any shape -- not the score prefix, and not the member
/// hex the first restatement assumed was still there. There is nothing in the stored form for a
/// corrupted name to disagree with the durable map about, and nothing for this experiment to
/// corrupt.
///
/// SO BOTH NEEDLES ARE NOW SUBJECTS AND THE CONTROL HAS MOVED TO SOMETHING THAT IS ACTUALLY
/// PRESENT. The object key is in these files; it is counted first, through the SAME matcher, so a
/// zero from either needle means the text is absent rather than the scan being broken. That
/// distinction is the only thing standing between this arm and a pair of assertions that pass on an
/// empty directory.
///
/// AND THE DEFECT IS NOW UNREACHABLE RATHER THAN ABSENT, which is worth more than the original
/// test was. A score parsed out of a name cannot outrank the durable map, because there is no name
/// to parse: `shard.zsets` (`zset_index_serde`) is the only place a score is written down. The
/// positive half is still driven -- the score comes back at 7.5 across a reload -- so this arm
/// still fails if the durable map stops being read.
///
/// rust-internal: reads the engine's own served index files, no external surface
#[test]
fn a_durable_zset_score_has_no_name_left_to_disagree_with_it() {
    let dir = tempfile::tempdir().unwrap();
    let indexes = dir.path().join("indexes");
    let member = b"outranked-member".to_vec();
    let durable_score = 7.5f64;
    let name_score = 99.25f64;

    {
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        write(
            &engine,
            Command::ZSetAdd {
                key: "do-zset".to_string(),
                member: member.clone(),
                score: durable_score,
            },
        );
        engine.unload_shard(1);
    }

    // THE CONTROL, FIRST AND THROUGH THE SAME MATCHER: text that IS in these files. Without it the
    // two zeros below would pass over an empty directory, an unreadable frame, or a codec this
    // scan cannot decompress.
    let object_key_hits = count_across_index_files(&indexes, "do-zset");
    assert!(
        object_key_hits > 0,
        "the object key was not found in any index file, so the scan is reading nothing and the \
         zeros below say nothing about what the index contains"
    );
    println!("[outrank] control: the object key appears {object_key_hits} time(s)");

    // SUBJECT 1: the score-shaped name, as a component used to be spelled. Gone with the score.
    let retired_from = zset_name_with_retired_score_prefix(durable_score, &member);
    let retired_to = zset_name_with_retired_score_prefix(name_score, &member);
    assert_ne!(
        retired_from, retired_to,
        "the two scores spell the same retired-shape name"
    );
    let retired_hits = count_across_index_files(&indexes, &retired_from);
    assert_eq!(
        0, retired_hits,
        "a score-shaped name appears {retired_hits} time(s) in a served index -- the score is \
         supposed to have left the component entirely"
    );

    // SUBJECT 2: the member-shaped name, which the first restatement used as its control. Gone with
    // the FIELD. This is the assertion that inverted.
    let member_hits = count_across_index_files(&indexes, &zset_name(&member));
    assert_eq!(
        0, member_hits,
        "a member-shaped name appears {member_hits} time(s) in a served index. An entry has no \
         element-name field, so no component text of any shape should reach the stored form -- if \
         this is nonzero the name is back on the entry and the whole of this arm's reasoning has \
         to be re-driven, not relaxed"
    );
    println!(
        "[outrank] neither a score-shaped nor a member-shaped name appears: there is no stored \
         text for a durable score to be outranked by"
    );

    // AND THE DURABLE MAP IS STILL READ, which is the half that can still fail. The score survives
    // a reload because `shard.zsets` is written and read back; nothing re-derives it.
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let answered = match read(
        &engine,
        Command::ZSetScore {
            key: "do-zset".to_string(),
            member: member.clone(),
        },
    ) {
        crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
            String::from_utf8_lossy(&bytes).to_string()
        }
        other => panic!(
            "the member did not come back at all: {other:?}. The durable map is now the ONLY \
             source for a zset element, so this is that map not being read rather than a score \
             source question -- investigate rather than adjusting this test."
        ),
    };
    let score: f64 = answered
        .parse()
        .unwrap_or_else(|_| panic!("a score came back as {answered:?}"));
    println!("[outrank] wrote {durable_score}, reloaded, answered {score} from the durable map");
    assert!(
        (score - durable_score).abs() < 1e-9,
        "the reload answered {score}, not the durable {durable_score}"
    );
}

/// AND THE SAME THING THROUGH THE OTHER DOOR: A RELEASE AND RELOAD, NOT A PROCESS RESTART.
///
/// `reconcile_secondary_views_from_bucket_index` is reached from five places -- two on the load path
/// in `persistence.rs`, and three on reconstruct (`lifecycle.rs`, `stream_batch_methods.rs`,
/// `engine.rs`). A fix proved on one door says nothing about the other, and a sibling has found these
/// two behaving differently before. This drives the reconstruct door by unloading and reloading the
/// shard inside ONE engine, so no file is re-read from scratch.
///
/// rust-internal: drives the engine's own unload/load cycle, no external surface
#[test]
fn the_durable_score_outranks_the_name_through_the_reconstruct_door_as_well() {
    let dir = tempfile::tempdir().unwrap();
    let member = b"reconstruct-member".to_vec();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    write(
        &engine,
        Command::ZSetAdd {
            key: "rc-zset".to_string(),
            member: member.clone(),
            score: 3.25,
        },
    );

    // Unload and load again in the same engine: the reconstruct path, not a fresh process.
    engine.unload_shard(1);
    load_on(&engine, OPERATOR_END);
    let after_first = match read(
        &engine,
        Command::ZSetScore {
            key: "rc-zset".to_string(),
            member: member.clone(),
        },
    ) {
        crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
            String::from_utf8_lossy(&bytes).to_string()
        }
        other => panic!("the member did not survive a release and reload: {other:?}"),
    };
    let score: f64 = after_first.parse().expect("a score parses");
    println!("[door] after a release and reload the score is {score}");
    assert!(
        (score - 3.25).abs() < 1e-9,
        "the reconstruct door answered {score}, not 3.25"
    );

    // And a SECOND cycle, because the first may have been served from a map that was never rebuilt.
    engine.unload_shard(1);
    load_on(&engine, OPERATOR_END);
    let again = match read(
        &engine,
        Command::ZSetScore {
            key: "rc-zset".to_string(),
            member: member.clone(),
        },
    ) {
        crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
            String::from_utf8_lossy(&bytes).to_string()
        }
        other => panic!("the member did not survive a second cycle: {other:?}"),
    };
    let score: f64 = again.parse().expect("a score parses");
    assert!(
        (score - 3.25).abs() < 1e-9,
        "the second cycle answered {score}, not 3.25"
    );
    println!("[door] and again after a second cycle: {score}");
}

// =============================================================================================
// 2. ALL THREE KINDS, AND THE TWO THAT GENUINELY HAVE ONLY A NAME
// =============================================================================================

/// EVERY KIND WHOSE ELEMENT IDENTITY IS SPELLED INTO A NAME SURVIVES A RELOAD INTACT.
///
/// `set`, `zset` and `list` all have a durable map and all three used to be assigned over. `string`
/// and `hash` have none, and for them deriving is not a second copy of anything -- a string has no
/// component and a hash's component IS the caller's field name. Both are included so the fix is shown
/// not to have moved them.
///
/// rust-internal: drives a reload, no external surface
#[test]
fn all_three_spelled_kinds_and_the_two_without_a_durable_map_survive_a_reload() {
    let dir = tempfile::tempdir().unwrap();
    let members: Vec<Vec<u8>> = (0..7)
        .map(|element| format!("kind-member-{element:03}").into_bytes())
        .collect();

    {
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        for (element, member) in members.iter().enumerate() {
            write(
                &engine,
                Command::ZSetAdd {
                    key: "ak-zset".to_string(),
                    member: member.clone(),
                    score: element as f64 + 0.5,
                },
            );
            write(
                &engine,
                Command::SetAdd {
                    key: "ak-set".to_string(),
                    member: member.clone(),
                },
            );
            write(
                &engine,
                Command::ListPush {
                    key: "ak-list".to_string(),
                    member: format!("entry-{element}").into_bytes(),
                    left: false,
                },
            );
            write(
                &engine,
                Command::HashSet {
                    key: "ak-hash".to_string(),
                    field: format!("field-{element}"),
                    value: format!("value-{element}").into_bytes(),
                },
            );
        }
        write(
            &engine,
            Command::StringSet {
                key: "ak-string".to_string(),
                value: b"string-value".to_vec(),
            },
        );
        engine.unload_shard(1);
    }

    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    // zset: identity AND score, both from the durable map.
    for (element, member) in members.iter().enumerate() {
        let answered = match read(
            &engine,
            Command::ZSetScore {
                key: "ak-zset".to_string(),
                member: member.clone(),
            },
        ) {
            crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
                String::from_utf8_lossy(&bytes).to_string()
            }
            other => panic!("zset member {element} is gone after the reload: {other:?}"),
        };
        let score: f64 = answered.parse().expect("a score parses");
        assert!(
            (score - (element as f64 + 0.5)).abs() < 1e-9,
            "zset member {element} came back at {score}, not {}",
            element as f64 + 0.5
        );
    }

    // set: the member's identity. The empty member must NOT appear -- that is what an unreadable
    // name used to become.
    let set_members = match read(
        &engine,
        Command::SetMembers {
            key: "ak-set".to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a set read answered {other:?}"),
    };
    assert_eq!(
        set_members.len(),
        members.len(),
        "the set came back with {} of {} members",
        set_members.len(),
        members.len()
    );
    assert!(
        !set_members.iter().any(|member| member.is_empty()),
        "the set came back holding the EMPTY member, which is what an unreadable name used to \
         become: {set_members:?}"
    );
    for member in &members {
        assert!(
            set_members.iter().any(|held| held == member),
            "the set lost {:?}",
            String::from_utf8_lossy(member)
        );
    }

    // list: the sequence, and in order. A name that could not be read used to become sequence ZERO
    // and overwrite whatever was there, so the ORDER is the thing to check.
    let list = match read(
        &engine,
        Command::ListRange {
            key: "ak-list".to_string(),
            start: 0,
            stop: -1,
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a list read answered {other:?}"),
    };
    assert_eq!(
        list.len(),
        members.len(),
        "the list came back with {} of {} entries",
        list.len(),
        members.len()
    );
    for (element, held) in list.iter().enumerate() {
        assert_eq!(
            held.as_slice(),
            format!("entry-{element}").as_bytes(),
            "list position {element} came back as {:?}",
            String::from_utf8_lossy(held)
        );
    }

    // hash: a durable map NOW, like the other three. This used to read "no durable map, and none
    // needed -- the component IS the caller's field name", and the second half is still true: the
    // component spells the field name exactly, so a readable name needs no help. What was wrong was
    // the inference that no help was ever needed, because a hash page can name NO field at all, and
    // then the name spells nothing and the durable map is the only record. That case is driven by
    // `an_unnamed_hash_block_is_skipped_and_the_durable_map_keeps_the_field` below.
