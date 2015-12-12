// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// =================================================================================================
// DOES THE FOLD ROUTE LEAVE THE DURABLE SET MAP NAMING EVERY MEMBER THE PAGE INDEX NAMES?
//
// #2078 established this for `hashes` and for `hashes` ONLY. The proof does not carry over: a
// different kind has different install arms, a different level-2 key, and a different reconcile
// shape, so "the fold leaves the container complete" has to be re-established per kind or it is an
// assumption wearing a test's clothes. This module is that proof for `sets`, or its refutation.
//
// WHY IT MATTERS HERE, which is the same reason it mattered for hash. A page entry carries
// `component` -- for a set, the member hex -- and the model map is a second copy of the same
// identity. The entry's `component` is 16 of the 56 bytes a resident entry weighs, and taking it off
// leaves `SetMembers` nowhere to read a member name FROM except `shard.sets`. So the order is not
// "reroute the readers, then remove the field": it is prove the map complete FIRST, because the
// removal is what makes the map load-bearing.
//
// WHAT IS ASSERTED: that the two sources name THE SAME SET of members after a fold -- set equality
// in both directions, not a count. A count cannot tell a missing member from an extra one, and the
// two failures mean opposite things: a member the map lacks is the removal losing data, and a member
// the map holds alone is the fold resurrecting something.
//
// AND THE REMOVAL GATE, DRIVEN. A deleted member must be in NEITHER source after a reload. Without
// that, set equality can be satisfied by a fold that resurrects a removal into both.
// =================================================================================================

#![allow(clippy::all)]
use super::*;

/// THE CENSUS: EVERY PATH THAT INSTALLS A SET MEMBER REPORTS A TOUCHED ELEMENT, and the class is
/// bounded by the engine's own match rather than by a grep.
///
/// # WHY A CENSUS AND NOT A COUNT OF GREP HITS
///
/// The question is "could some writer install a member the carried channel never hears about?", and
/// a grep answers a different question -- it finds the sites that spell a pattern. The class here is
/// bounded twice over from the engine's own source: the `Command` enum names every command that
/// exists, and `execute_on_shard`'s match names every arm that handles one. A command absent from
/// the enum cannot be sent, and an arm absent from the match falls to the catch-all and performs no
/// write at all.
///
/// # WHAT THIS FOUND FOR `sets`, STATED EITHER WAY
///
/// `command_touched_container_elements` has ONE arm for this kind -- `Command::SetAdd` -- and its
/// catch-all returns an empty vector, which is where an install could go unreported. For hash the
/// equivalent reading turned up a writer with no record at all: `write_context_node`, which installs
/// a hash element and is deliberately never filed in the bucket index. **There is no equivalent for
/// `sets`.** Every install of a set member goes through `Command::SetAdd` or through WAL replay, and
/// replay is the record being re-applied rather than a new write.
///
/// rust-internal: reads this crate's own source text, no product behaviour
#[test]
fn every_command_that_installs_a_set_member_reports_a_touched_element() {
    let types = include_str!("../../command_protocol_types.rs");
    let execute = include_str!("../execute_on_shard.rs");
    let engine = include_str!("../../engine.rs");
    let lifecycle = include_str!("../engine_lifecycle.rs");

    // VACUITY, FIRST. Four source files that did not load would make every count below a zero
    // presented as a finding.
    assert!(
        types.len() > 50_000
            && execute.len() > 50_000
            && engine.len() > 100_000
            && lifecycle.len() > 50_000,
        "a source file did not load: {} / {} / {} / {}",
        types.len(),
        execute.len(),
        engine.len(),
        lifecycle.len(),
    );

    // ---- BOUND 1: the commands that exist, from the enum. -------------------------------------
    //
    // `Set` prefixed and NOT `ZSet` prefixed, because a zset is a different kind with a different
    // map. Taken at the enum's own indentation so a doc comment mentioning a name is not counted.
    let declared: std::collections::BTreeSet<String> = types
        .lines()
        .filter_map(|line| {
            let trimmed = line.strip_prefix("    ")?;
            if trimmed.starts_with("    ") || !trimmed.starts_with("Set") {
                return None;
            }
            let end = trimmed.find(|c: char| !c.is_alphanumeric())?;
            Some(trimmed[..end].to_string())
        })
        .collect();
    println!("[set-census] Set-family commands declared: {declared:?}");
    assert!(
        declared.len() >= 3,
        "only {} Set-family command(s) were found in the enum, which is too few to be this surface \
         -- the scan is not reading what it thinks: {declared:?}",
        declared.len(),
    );

    // ---- BOUND 2: the arms that handle them, from the match. ----------------------------------
    let handled: std::collections::BTreeSet<String> = declared
        .iter()
        .filter(|name| execute.contains(&format!("Command::{name} {{")))
        .cloned()
        .collect();
    println!("[set-census] of those, handled by an arm in execute_on_shard: {handled:?}");
    assert_eq!(
        declared, handled,
        "a Set-family command is declared with no arm to handle it, or the arm spells its name \
         differently. Either way the class below is not bounded: declared {declared:?}, handled \
         {handled:?}"
    );

    // ---- WHICH OF THOSE ARMS INSTALLS, read from the arm bodies. ------------------------------
    //
    // An install is an arm that puts an element INTO `shard.sets`. The two spellings that do that
    // are an `entry(..)` chain and a direct `insert`, and both are looked for inside the arm's own
    // text rather than anywhere in the file.
    let mut installs: Vec<String> = Vec::new();
    for name in &declared {
        let marker = format!("Command::{name} {{");
        let Some(at) = execute.find(&marker) else {
            continue;
        };
        // The arm runs to the next `        Command::` at the match's own indentation.
        let rest = &execute[at + marker.len()..];
        let body = match rest.find("\n        Command::") {
            Some(end) => &rest[..end],
            None => rest,
        };
        // AN INSTALL IS A CALL TO THE RECORDED OPERATION, and that is now the ONLY spelling.
        //
        // This matcher has been blinded once already and the blinding was by the change that made
        // the statement stronger. It used to look for `.sets` plus an `.entry(` or `.insert(`,
        // which is what an arm spelled when it reached the model map directly. Since
        // `RecordedSetContainer` took the map, the inner field is private and an arm CANNOT reach
        // it -- so the only way to install is `install_set_member`, and looking for the old shape
        // reported zero installs on a tree where the install was right there.
        //
        // The stronger statement is why the restatement is not a relaxation: before, an arm could
        // install by any spelling that reached a `HashMap`, and this matcher had to guess them all.
        // Now there is exactly one, enforced by privacy, and the matcher names it.
        // RESTATED AGAIN, by the collapse of the two containers into one generic map. This asked
        // for `recorded_set_container::install_set_member(`, which was itself a restatement of an
        // earlier `.sets` plus `entry`/`insert`. The operation is now written ONCE for every kind
        // and named with the kind it is for, so what identifies a set install is the turbofish.
        //
        // Each restatement has been STRONGER than the one before, which is why none of them is a
        // relaxation: first any spelling that reached a `HashMap`, then one named per-kind function,
        // and now one generic function whose kind parameter the compiler checks.
        if body.contains("install_element::<super::recorded_map::SetKind>(") {
            installs.push(name.clone());
        }
    }
    installs.sort();
    println!("[set-census] arms that install a set member: {installs:?}");
    // VACUITY: the operation must be findable by name, so a rename cannot make `installs` empty
    // and have someone "fix" this test by expecting nothing.
    assert!(
        execute.contains("install_element::<super::recorded_map::SetKind>("),
