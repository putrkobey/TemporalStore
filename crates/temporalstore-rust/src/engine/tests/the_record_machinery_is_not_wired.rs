// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE ONE-RECORD-PER-OBJECT MACHINERY IS BUILT AND NOT WIRED, and this says so in a form that
//! cannot go stale.
//!
//! # THE QUESTION THIS ANSWERS
//!
//! `ObjectRecord` exists to collapse the sibling rows an object is named by -- its own doc says so:
//! the model map, the page lookup, the dirty index and the page index. It carries a `deadline` word
//! with two tag bits, a `DIRTY` flag, and a page-layout handle for an address that does not fit
//! inline. So the obvious question is why `expires_at_ms` and `dirty_objects` are still separate
//! structures.
//!
//! The answer was not a design disagreement: nothing in production ever filled any of it. The
//! constructors that would have set a deadline or a dirty bit had no production caller, the
//! page-layout side table was touched only from `#[cfg(test)]` code, and the one production path
//! that builds a slot uses `ObjectSlot::default()`. The representation changed and **the leg that
//! fills it never moved**.
//!
//! **THE DEADLINE AND THE DIRTY BIT ARE NOW GONE RATHER THAN UNWIRED.** Eight bytes of deadline and
//! three flag bits that were always zero came off the record, taking it from twenty-four bytes to
//! sixteen; the three bits retired into `RESERVED`, so a record carrying one is refused as a newer
//! writer's rather than read with its meaning dropped. What this module still guards is the half
//! that remains built and unfilled: the two row constructors and the page-layout side table.
//!
//! The duplication the record was going to remove is therefore still being paid by the structures
//! that were always paying it -- the bucket-grouped dirty index and the per-key expiry table -- and
//! neither is a duplicate of anything on the record any more.
//!
//! # WHY THIS IS A TEST AND NOT A COMMENT
//!
//! A comment saying "this is not wired yet" rots silently in both directions: someone wires it and
//! the comment still says otherwise, or someone deletes the machinery as dead and the comment was
//! the only record of why it existed. This arm fails EITHER WAY -- it names the sites it expects to
//! find and the ones it expects not to -- so the next person is told which world they are in.
//!
//! # WHAT IS NOT A DUPLICATE, STATED SO IT IS NOT "FIXED" BY MISTAKE
//!
//! `dirty_objects` is **not** redundant and must not be folded into the record's bit.
//! `object_record.rs`'s own doc gives the reason: it is grouped BY BUCKET, and answers "which
//! objects in this bucket are dirty", which a per-object bit cannot answer without visiting every
//! object. It is also `#[serde(skip)]`, so it costs no stored bytes at all. The redundant half of
//! that pair is the record's `DIRTY` flag, and since nothing sets it there is no live duplication
//! today -- only an unused bit.
//!
//! `expiry_by_deadline` is **not** redundant either, for the same shape of reason: it is ordered BY
//! DEADLINE and answers "which object expires next". What the record would absorb is
//! `expires_at_ms`, the per-key lookup -- "when does THIS key expire" -- and only that.

/// Production source: every `.rs` under the crate's `src/`, excluding any path with a `tests`
/// component. A `#[cfg(test)]` region inside a production FILE is NOT excluded, which matters here:
/// the only call that fills a record's deadline lives in one, and counting it as production would
/// have reversed this module's entire conclusion.
fn production_sources() -> Vec<(String, String)> {
    fn walk(dir: &std::path::Path, out: &mut Vec<(String, String)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if path.file_name().is_some_and(|name| name == "tests") {
