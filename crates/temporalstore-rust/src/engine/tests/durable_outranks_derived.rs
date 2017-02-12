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
    for element in 0..members.len() {
        let expected = format!("value-{element}").into_bytes();
        assert!(
            matches!(
                read(
                    &engine,
                    Command::HashGet {
                        key: "ak-hash".to_string(),
                        field: format!("field-{element}"),
                    },
                ),
                crate::types::CommandResponse::Bytes { value: Some(ref got) } if *got == expected
            ),
            "hash field {element} did not come back"
        );
    }

    // string: no component at all.
    assert!(
        matches!(
            read(&engine, Command::StringGet { key: "ak-string".to_string() }),
            crate::types::CommandResponse::Bytes { value: Some(ref got) } if got == b"string-value"
        ),
        "the string did not come back"
    );

    println!(
        "[kinds] {} zset members with their scores, {} set members with no empty member, {} list \
         entries in order, {} hash fields and the string all came back",
        members.len(),
        set_members.len(),
        list.len(),
        members.len()
    );
}

// =============================================================================================
// 3. THE FALLBACK: AN ELEMENT WITH NO DURABLE ENTRY IS STILL DERIVABLE
//    (and NOT "durable wins where present" -- the index-derived address wins a collision)
// =============================================================================================

/// AN ELEMENT THE DURABLE MAP DOES NOT HOLD DOES NOT COME BACK, AND THE STATE IS UNREACHABLE.
///
/// INVERTED, AND IT IS THE ARM THIS WHOLE MODULE TURNS ON. It asserted that an element missing from
/// the durable map "is still derivable" -- recovered BY IDENTITY from the element name on its page
/// index entry -- and a first restatement narrowed that to "the member still comes back, only its
/// SCORE is now a placeholder". Measured: `fb-two` does not come back at all,
/// `Bytes { value: None }`. The member half went with the score half, for the same reason and one
/// step further on: there is no element name on an entry to recover an identity FROM.
/// `rebuild_unserialized_model_maps_from_bucket_index` passes an EMPTY derived view and keeps only
/// its live-address filter.
///
/// SO THE DIVERGENCE CAN NO LONGER BE REPRESENTED, and that is the finding rather than a loss to
/// chase. The four resident maps are durable (`#[serde(default)]`) and are the sole authority for
/// which elements exist. An element they do not hold is an element that does not exist.
///
/// AND THE PLANTED STATE IS UNREACHABLE IN PRODUCTION, WHICH IS WHY THIS IS SAFE RATHER THAN MERELY
/// TRUE. Three mechanisms close it, and all three were read rather than assumed:
///
///   1. THE FOLD CANNOT PRODUCE IT. This fixture builds the state by mutating the resident map
///      directly, and its own comment called that "the shape a delta fold produces: an element the
///      index knows about and the persisted map does not". The fold cannot produce it any more:
///      `collect_upsert_index_items` resolves each item's address THROUGH the resident map for all
///      four container kinds -- `shard.hashes`/`sets`/`zsets`/`lists` -- and `continue`s where the
///      lookup misses, so a delta item exists only where the map held the element.
///   2. THE DURABLE MAP DOES NOT LOSE ELEMENTS ACROSS A LOAD. It is written to the index snapshot
///      and read back; `length_answer_and_listing_agree` drives that a short container STAYS short
///      across a reload rather than being re-derived, which is the same precedence from the other
///      side.
///   3. AN INDEX THAT REALLY CARRIES AN ELEMENT NAME IS REFUSED. `persistence` refuses a stamp
///      below `SHARD_INDEX_FORMAT_VERSION` with `<`, and 12 was stamped by the commit that made all
///      four kinds page-named, so such an index is rebuilt from the WAL rather than decoded. The
///      argument is set out in full beside that constant.
///
/// WHAT IS ASSERTED, THEREFORE: the element the map kept comes back at its TRUE score (the positive
/// control -- without it this arm would pass on a reload that lost the whole key), the element the
/// map lost does not come back, and the resident map holds exactly the one element after the reload
/// so the derived view cannot be quietly resurrecting it under a different score.
///
/// rust-internal: reads the merge the load path uses, no product behaviour
#[test]
fn an_element_the_durable_map_does_not_hold_does_not_come_back() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    // Two members of one zset, written normally, so both are durable and both have pages.
    for (element, member) in [b"fb-one".to_vec(), b"fb-two".to_vec()].iter().enumerate() {
        write(
            &engine,
            Command::ZSetAdd {
                key: "fb-zset".to_string(),
                member: member.clone(),
                score: element as f64 + 1.0,
            },
        );
    }

    // Drop one from the durable map only, leaving its page and its index entry.
    {
        let mut shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get_mut(&1).expect("shard is loaded");
        let removed = shard
            .zsets
            .elements_mut_for_test("fb-zset")
            .expect("the zset is present")
            .remove(b"fb-two".as_slice());
        assert!(
            removed.is_some(),
            "the durable map did not hold fb-two, so this fixture cannot create the state it needs"
        );
        println!("[fallback] removed fb-two from the durable map, leaving its page entry");
    }

    engine.unload_shard(1);
    load_on(&engine, OPERATOR_END);

    let score_of = |member: &[u8]| -> Option<f64> {
        match read(
            &engine,
            Command::ZSetScore {
                key: "fb-zset".to_string(),
                member: member.to_vec(),
            },
        ) {
            crate::types::CommandResponse::Bytes { value: Some(bytes) } => {
                String::from_utf8_lossy(&bytes).parse::<f64>().ok()
            }
            _ => None,
        }
    };
    let kept = score_of(b"fb-one");
    let lost = score_of(b"fb-two");
    println!("[fallback] after the reload: fb-one={kept:?} fb-two={lost:?}");

    // THE POSITIVE CONTROL. Without it a reload that lost the whole key would read as this finding.
    assert_eq!(
        Some(1.0),
        kept,
        "fb-one, which the durable map never lost, came back as {kept:?} rather than its true 1.0 \
         -- this fixture lost the whole key and says nothing about the element it dropped"
    );

    // THE INVERSION.
    assert_eq!(
        None, lost,
        "fb-two came back as {lost:?} after being dropped from the durable map. Nothing can name \
         it: an entry carries no element name and the derived view is empty by construction. A \
         value here means an element name has come back to the entry, or something else has \
         started deriving identity from the index -- re-read this arm's doc before adjusting it"
    );

    // AND THE RESIDENT MAP HOLDS EXACTLY THE ONE ELEMENT, so the `None` above is the map's own
    // content rather than a read path that happens to miss.
    {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let members = shard.zsets.get("fb-zset").expect("the zset is present");
        assert_eq!(
            1,
            members.len(),
            "the resident map holds {} members after the reload, not the one the fixture left it \
             with; the derived view is putting something back",
            members.len()
        );
        assert!(
            members.contains_key(b"fb-one".as_slice()),
            "the member the map kept is not the one that survived"
        );
    }
    println!(
        "[fallback] so an element the durable map does not hold is gone, and the map is the sole \
         authority. The state this fixture planted by hand is one the fold cannot produce"
    );
}

/// AN ELEMENT NAME CANNOT BE MADE UNREADABLE, BECAUSE A SERVED INDEX HOLDS NONE.
///
/// RESTATED INTO THE UNREACHABLE FORM. This planted a corrupt element name into a served index file
/// -- one character of a set member's hex swapped for `z`, length kept so nothing shifted -- and
/// asserted that the derived view skipped the element while the durable map put it back. It is the
/// only case in which the merge was load-bearing: a mutant that reverted it to an assignment
/// survived every other arm here.
///
/// THE EXPERIMENT IS UNRUNNABLE, AND THE ARM'S OWN FLOOR IS WHAT SAID SO: "the name was not found
/// in any index file, so nothing was corrupted and this test would pass without testing anything".
/// `BlockIndex` has no element-name field, so a served index carries no component text to corrupt.
/// A name cannot be made unreadable when there is no name.
///
/// SO THE DEFECT IS UNREACHABLE RATHER THAN ABSENT, which is the stronger of the two things this
/// can say. The old failure was a silent GUESS: a set's unreadable name became
/// `unwrap_or_default()` -- the EMPTY member, a real value that then took a genuine element's
/// address. There is no parse left to get wrong, because there is nothing to parse.
///
/// WHAT IS ASSERTED, AND WHY EACH HALF IS HERE:
///
///   * THE CONTROL, FIRST: the object key IS in these files, counted through the same matcher the
///     subject uses. A zero from a scan that reads nothing would pass for the finding.
///   * THE SUBJECT: no component text for this object's members appears in any index file. This
///     fails if an element name returns to the stored form -- which is the thing that would make
///     the old defect reachable again.
///   * AND THE PROPERTIES THE CORRUPTION USED TO PROTECT, still driven end to end: both members
///     come back, NEITHER is the empty member, and both are removable. The empty-member assertion
///     is kept as a tripwire rather than retired: a set is served from its PAGES now, through
///     `derive_membership`, and an element key that cannot be rendered is counted as unrenderable
///     rather than defaulted -- so the shape that produced the empty member has moved rather than
///     gone, and this is where it would show.
///
/// rust-internal: reads the engine's own served index files, no external surface
#[test]
fn an_element_name_cannot_be_made_unreadable_because_the_index_holds_none() {
    let dir = tempfile::tempdir().unwrap();
    let indexes = dir.path().join("indexes");
    let kept = b"kept-member".to_vec();
    let second = b"second-member".to_vec();

    {
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        for member in [&kept, &second] {
            write(
                &engine,
                Command::SetAdd {
                    key: "un-set".to_string(),
                    member: member.clone(),
                },
            );
        }
        engine.unload_shard(1);
    }

    // THE CONTROL, THROUGH THE SAME MATCHER AS THE SUBJECT.
    let object_key_hits = count_across_index_files(&indexes, "un-set");
    assert!(
        object_key_hits > 0,
        "the object key was not found in any index file, so the scan is reading nothing and the \
         zeros below say nothing about what the index contains"
    );

    // THE SUBJECT: there is no element name in the stored form to corrupt.
    for member in [&kept, &second] {
        let name = hex::encode(member);
        let hits = count_across_index_files(&indexes, &name);
        assert_eq!(
            0, hits,
            "the element name {name:?} appears {hits} time(s) in a served index. An entry carries \
             no element name, so there should be none to find -- and if there is one, it can be \
             made unreadable and the defect this arm records is reachable again"
        );
    }
    println!(
        "[unreadable] no element name appears in any index file ({object_key_hits} object-key \
         hits prove the scan reads them), so there is nothing a corruption could make unreadable"
    );

    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);
    let members = match read(
        &engine,
        Command::SetMembers {
            key: "un-set".to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        other => panic!("a set read answered {other:?}"),
    };
    println!(
        "[unreadable] the set came back with {} member(s): {:?}",
        members.len(),
        members
            .iter()
            .map(|member| String::from_utf8_lossy(member).to_string())
            .collect::<Vec<_>>()
    );
    assert!(
        !members.iter().any(|member| member.is_empty()),
        "the set came back holding the EMPTY member. That is what an unreadable name used to become \
         -- a real value taking a genuine element's address. A set is served from its pages now, so \
         this would be `derive_membership` defaulting an element key it could not render instead of \
         counting it unrenderable: the same defect at a new door."
    );
    assert!(
        members.iter().any(|member| member == &kept),
        "a member written normally is gone, which is a different failure than this arm is for"
    );
    assert!(
        members.iter().any(|member| member == &second),
        "a member written normally is gone, which is a different failure than this arm is for"
    );
    assert_eq!(
        members.len(),
        2,
        "the set came back with {} members rather than 2",
        members.len()
    );
    println!(
        "[unreadable] both members came back from the durable map and the pages, with no name in \
         the index for either of them"
    );

    // AND IT MUST STILL BE REMOVABLE, WHICH IS NOW A CLAIM ABOUT THE RESIDENT MAP ALONE.
    //
    // This paragraph read "which is the door the merge is actually for ... Without the merge the
    // derived view has skipped this member, the remove cannot find it, and the set holds a member
    // nobody can delete." There is no skip to recover from: nothing derives a set member from an
    // index entry, so `shard.sets` is not a repair of a derived view -- it IS the membership.
    //
    // WHAT MAKES THE ASSERTION WORTH KEEPING is that the two sides of a set are still different
    // artefacts. `SetMembers` answers from the PAGES, and `SetRemove` is the one command that
    // consults `shard.sets`. So a member present in the pages and absent from the map is served
    // and undeletable -- the same end state the old defect produced, by a different route -- and
    // this is the arm that would show it.
    let removed = read(
        &engine,
        Command::SetRemove {
            key: "un-set".to_string(),
            member: second.clone(),
        },
    );
    println!("[unreadable] removing a member answered {removed:?}");
    // Asserted on the EFFECT, not on the answer: `SetRemove` answers `Empty`, and what matters is
    // whether the member stops being served.
    let after = match read(
        &engine,
        Command::SetMembers {
            key: "un-set".to_string(),
        },
    ) {
        crate::types::CommandResponse::Members { members } => members,
        unexpected => panic!("a set read answered {unexpected:?}"),
    };
    assert!(
        !after.iter().any(|member| member == &second),
        "a member is STILL SERVED after a remove: {after:?}. `SetMembers` answers from the pages \
         and `SetRemove` consults `shard.sets`, so a member the map does not hold is served and \
         undeletable -- the end state the unreadable-name defect used to produce, by a different \
         route."
    );
    assert!(
        after.iter().any(|member| member == &kept),
        "the removal took the wrong member"
    );
    println!(
        "[unreadable] and it removed cleanly, leaving {} member(s) -- the pages and the resident \
         map agree about membership with no name in the index for either of them",
        after.len()
    );
}

/// AN UNNAMED HASH PAGE LOSES NOTHING, BECAUSE THE DURABLE MAP KEEPS THE FIELD.
///
/// THE ARM THAT CHANGED IS THE ARM THAT HAD NO DRIVEN CASE. The merge was extended to `hashes` on
/// the claim that its mechanism is now byte-for-byte the other three's, and "closed by construction"
/// is exactly the kind of statement a test has repeatedly found to be false in this tree. So this is
/// the hash twin of `an_unreadable_component_name_is_skipped_and_the_durable_map_keeps_the_element`,
/// and it is the one test here that fails if `shard.hashes = hashes` comes back.
///
/// # RESTATED: THE SHAPE IS NO LONGER PLANTED, BECAUSE IT IS THE ONLY SHAPE THERE IS
///
/// This test used to BLANK the component on one field's page entry and keep a second field's name as
/// a control -- a named page and a nameless one, side by side. Under one entry a page NO live entry
/// of any container kind carries an element name, so there is nothing left to blank: the blanking
/// loop matched zero entries and the fixture's own floor refused it with `blanked 0, not one`. That
/// floor is why this reads as a restatement rather than as a test that quietly stopped testing.
///
/// THE CONTROL HAD TO GO WITH IT, AND THAT IS THE HONEST VERSION. `field-0` was the control because
/// its page still named it, so it arrived through the DERIVED view while `field-1` could only arrive
/// from the durable map. Neither is named now, so there is no named/nameless contrast to draw, and
/// keeping `field-0` as a "control" would be a second copy of the same case wearing the word
/// control. What replaces it is a DENOMINATOR on the other side of the claim: the index must be
/// shown to hold live hash entries for this key and to name NOTHING with them, or "the durable map
/// supplied it" is a conclusion about an index that was simply empty.
///
/// # WHAT IS ASSERTED, AND WHAT WOULD HAVE TO DISAGREE FOR IT TO FAIL
///
/// Both fields must be in `shard.hashes` after a reload, and the index must name neither. The two
/// independent artefacts are the index, which names zero fields, and the durable map, which names
/// two: if the merge stops consulting the map -- `shard.hashes = hashes`, or
/// `insert_element_if_absent` ceasing to insert -- the reload serves ZERO fields, because nothing
/// else can name one. That is a strictly larger failure than the single field the planted version
/// could lose, so the restatement did not weaken what this holds.
///
/// AND IT IS A TRIPWIRE IN THE OTHER DIRECTION TOO. If any live hash entry is ever found naming a
/// field again, the `named` floor below reddens and says so: the collapse would have been reverted,
/// and the arms in this module that assume a nameless index would all need re-reading.
///
/// Asserted on the resident map rather than through a command, deliberately: `HashGet` and
/// `HashGetAll` resolve through the resident map now, so reading them would be reading the same
/// source twice rather than the index against the map.
///
/// rust-internal: reads the engine's own in-memory index, no external surface
#[test]
fn an_unnamed_hash_block_is_skipped_and_the_durable_map_keeps_the_field() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    // Two fields of one hash, written normally.
    for element in 0..2 {
        write(
            &engine,
            Command::HashSet {
                key: "un-hash".to_string(),
                field: format!("field-{element}"),
                value: format!("value-{element}").into_bytes(),
            },
        );
    }

    // THE TWO DENOMINATORS, both read BEFORE the unload that writes the index.
    {
        let shards = engine.shards.write().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        assert_eq!(
            shard.hashes.get("un-hash").map(|fields| fields.len()),
            Some(2),
            "the durable map does not hold both fields, so this fixture cannot create the state it \
             needs and the assertions below would be about nothing"
        );
        let named = super::BlockObjects::of(&shard.bucket_index);
        let mut entries = 0usize;
        for bucket in shard.bucket_index.bucket_map.values() {
            for (handle, block) in &bucket.block_index {
                if block.model_id.as_str() == "hash"
                    && &**named.at(bucket.routing_bucket, *handle) == "un-hash"
                {
                    entries += 1;
                }
            }
        }
        println!(
            "[unnamed-hash] the index holds {entries} hash entr(ies) for this key, none of which \
             can name a field; the durable map names 2"
        );
        assert!(
            entries > 0,
            "THE INDEX HOLDS NO HASH ENTRY FOR THIS KEY AT ALL. Then the claim below would be a \
             statement about an EMPTY index rather than about a NAMELESS one, and this test would \
             pass without exercising the merge."
        );
        // THE `named == 0` FLOOR IS GONE, AND THE CLAIM IS STRONGER WITHOUT IT.
        //
        // It read "Under one entry a page none can: if this reddens the collapse has been
        // reverted". `BlockIndex` has no component field, so an entry naming a field is not a
        // reversion this test could observe -- it is UNREPRESENTABLE, and `engine_storage_state.rs`'s width pin
        // refuses it at const-evaluation. That is what this whole module is for: a divergence
        // proved unreachable is worth more than one measured absent, and this arm's subject has
        // just become the former.
        //
        // The `entries > 0` floor above STAYS and is doing the work now: it is what makes the
        // merge below a statement about a NAMELESS index rather than about an EMPTY one.
    }

    // The unload writes the index -- nameless entries and the durable map together -- and the
    // reload runs the merge over them.
    engine.unload_shard(1);
    load_on(&engine, OPERATOR_END);

    let shards = engine.shards.write().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    // DELIBERATELY NOT AN `expect` ON THE KEY. The regression this arm guards against -- the
    // reconcile assigning the derived view WHOLESALE instead of merging the durable map into it --
    // takes the whole KEY with it, not one field of it. An `expect` here reddens with "the hash key
    // did not survive the reload", which reads as a broken fixture, and the mutation proving this
    // arm non-vacuous did exactly that. An absent key IS zero fields, so it is folded into the
    // field set and the assertion under test is the one that speaks.
    let field_names: std::collections::BTreeSet<String> = shard
        .hashes
        .get("un-hash")
        .map(|fields| fields.keys().map(|name| name.to_string()).collect())
        .unwrap_or_default();
    println!(
        "[unnamed-hash] reloaded holding {} field(s): {:?}",
        field_names.len(),
        field_names
    );
    for field in ["field-0", "field-1"] {
        assert!(
            field_names.contains(field),
            "{field} IS NOT SERVED after the reload; the key came back with {:?}. No page entry \
             names a field, so the derived view cannot produce either of them and the durable map \
             is the only thing that can. This reddens if `fill_absent_elements` stops filling -- \
             i.e. if the reconcile goes back to assigning the derived view wholesale.",
            field_names
        );
    }
    assert_eq!(
        field_names.len(),
        2,
        "the hash came back with {} field(s) rather than 2: {:?}",
        field_names.len(),
        field_names
    );
    println!(
        "[unnamed-hash] both fields came back from an index that names neither, so the durable map \
         supplied the whole field set rather than one field of it"
    );
}

// =================================================================================================
// THE FOLD CARRIES ELEMENT IDENTITY, AND AN EMPTY ZSET MEMBER IS A WHOLE COMPONENT TOO
// =================================================================================================

/// AN EMPTY ZSET MEMBER SPELLS EXACTLY ZERO CHARACTERS, AND THE RECONCILE ONCE REFUSED TO READ IT.
///
/// RESTATED: `zset_component` used to be `{biased:016x}` followed by `hex::encode(member)`, so a
/// member of zero bytes spelled exactly sixteen characters -- a complete score with an empty
/// member after it -- and the reconcile's zset arm opened `if component.len() <= 16 { return None
/// }`, refusing the one length a real, empty-member write could produce. That sixteen-character
/// boundary is gone along with the score: `zset_component` is `hex::encode(member)` alone now, so
/// an empty member spells the empty string, and there is no length left to special-case at all --
/// `hex::decode("")` was always `Ok(vec![])`, and the reconcile's arm no longer asks about length
/// before decoding. This test keeps its ORIGINAL subject, not the boundary that caused it: an
/// empty zset member must stay reachable and durable through the fold shape that strands an
/// element with a page in the index but no durable-map entry behind it.
///
/// Historically, `zset_component` is `{biased:016x}` followed by `hex::encode(member)`. A member
/// of zero bytes -- which nothing on the write path rejects -- therefore spelled EXACTLY sixteen
/// characters, and the reconcile's zset arm opened `if component.len() <= 16 { return None }`. So
/// that component decoded to nothing, the element was counted in `unreadable_names` and skipped,
/// and on the one door where
/// the durable map does not already hold the element -- the delta fold, whose records carry the
/// elements written after the base snapshot -- the member was silently gone on reload.
///
/// THE ENGINE ALREADY SPELLS THE SAME BOUNDARY THE OTHER WAY. Both the insert arm and the removal
/// arm of `apply_outcome_item` ask `component.len() < 16`, so WAL replay accepted the very component
/// the reconcile refused. Two readers of one encoding disagreeing about its shortest legal form is
/// the defect, and this was the side that was wrong: sixteen characters is a complete score with an
/// empty member after it, and `hex::decode("")` is `Ok(vec![])`.
///
/// # THE EXPERIMENT, WHICH IS `an_element_the_durable_map_does_not_hold_still_comes_back_from_its_name`
/// # WITH AN EMPTY MEMBER
///
/// Two members, one of them empty. Both are written normally, so the fixture first PROVES an empty
/// member is reachable and durable rather than assuming it. Then the empty one is dropped from the
/// durable map only -- the shape a fold produces -- and the shard is reloaded.
///
///   * The non-empty member comes back. THE CONTROL: this arm is untouched by the boundary, so a
///     fixture that lost the whole key cannot pass as this finding.
///   * The empty member comes back too. Before the boundary was corrected it did not, and could
///     not: the reconcile skipped it and the durable map had nothing.
///
/// ITS SCORE AND ITS PRESENCE BOTH DEPEND ON THE DURABLE MAP NOW, which is the half this
/// restatement inverts. The fixture drops the empty member from that map ON PURPOSE, and nothing
/// else holds its identity: an entry carries no element name, so the reload cannot put it back at
/// any score. It asserted a PLACEHOLDER score; it now asserts ABSENCE, for the same reason
/// `an_element_the_durable_map_does_not_hold_does_not_come_back` does, and that arm carries the
/// argument for why the state is unreachable in production.
///
/// AND THE BOUNDARY THIS ARM EXISTS FOR SURVIVES, RE-ATTRIBUTED TO WHERE COMPONENTS STILL LIVE.
/// The finding was never about the durable map: it was that an empty member spells a WHOLE
/// component, so no reader may treat a short component as an absent one. Two readers of one
/// encoding once disagreed about its shortest legal form -- the reconcile refused
/// `component.len() <= 16` while WAL replay accepted `< 16` -- and that is the class of defect
/// being guarded. A page still spells its elements, so the boundary is asked of the PAGE: the
/// object's pages must derive the empty member as a zero-length component, through the engine's own
/// `container_membership::derive_membership`, which is the door `SetMembers` and `ZSetScore`'s
/// fold both read. If a length boundary comes back anywhere in that path, the empty member
/// disappears from the derivation and this fails.
///
/// rust-internal: mutates the engine's own in-memory index, no external surface
#[test]
fn an_empty_zset_member_is_a_whole_component_and_survives_the_fold_shape() {
    let dir = tempfile::tempdir().unwrap();
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let empty: Vec<u8> = Vec::new();
    let control = b"mt-control".to_vec();
    for (member, score) in [(empty.clone(), 3.0f64), (control.clone(), 1.0f64)] {
        write(
            &engine,
            Command::ZSetAdd {
                key: "mt-zset".to_string(),
                member,
                score,
            },
        );
    }

    // THE DENOMINATOR, and the reachability claim. An empty member has to be accepted and durable,
    // or everything below is about a state no caller can reach.
    let empty_component_len = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard is loaded");
        let members = shard.zsets.get("mt-zset").expect("the zset is present");
        assert_eq!(
            members.len(),
            2,
            "the fixture stored {} member(s), not the two it needs -- if the empty member was \
             rejected on the way in, this finding is about an unreachable state",
            members.len()
        );
        assert!(
            members.contains_key(empty.as_slice()),
            "the empty member is not in the durable map, so nothing here is about a live path"
        );
        zset_name(&empty).len()
    };
    // CHECKED, NOT ADJUSTED: an empty member's component is still zero characters, so there is no
    // length boundary for two readers to disagree about. That was the finding and it still holds.
    assert_eq!(
        empty_component_len, 0,
        "an empty member's component is {empty_component_len} characters, not the zero this \
         finding rests on -- re-read `zset_component` before trusting the boundary below"
    );

    // THE BOUNDARY, RE-ATTRIBUTED TO THE PAGES. The pages are where a component still exists, and
    // the empty member must appear in their derivation as a zero-length key.
    {
        let addresses = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard is loaded");
            let named = super::BlockObjects::of(&shard.bucket_index);
            let mut addresses: Vec<crate::block_store::ElementEntry> = Vec::new();
            for bucket in shard.bucket_index.bucket_map.values() {
                for (handle, block) in &bucket.block_index {
                    if block.model_id.as_str() == "zset"
                        && &**named.at(bucket.routing_bucket, *handle) == "mt-zset"
                    {
                        addresses.push(block.address.clone());
                    }
                }
            }
            addresses
        };
        assert!(
            !addresses.is_empty(),
            "DENOMINATOR: the object holds no page entries, so the derivation below would be empty \
             for a reason that has nothing to do with an empty member"
        );
        let derived = crate::engine::container_membership::derive_membership(
            "zset",
            addresses,
            |address| engine.block_store.read(address).ok(),
