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
