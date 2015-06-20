// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE COMPARISON MAPPING, AS A TEST RATHER THAN A DOCUMENT.
//!
//! # WHY
//!
//! This storage-metric comparison has been re-derived by hand three times and came out differently
//! each time — because the answer depends entirely on *how* you compare. Matching by field name
//! against one struct gives one number; matching by the QUANTITY each field measures, against the
//! struct that actually owns that kind of figure, gives a very different one.
//!
//! Each re-derivation was cheap to get wrong and expensive to redo, and **a document cannot fail
//! when the code moves underneath it.** So the mapping lives here, and this asserts every
//! counterpart field still exists where the mapping says it does.
//!
//! # WHICH FAILURE THIS CATCHES, AND WHICH THE COMPILER ALREADY DID
//!
//! Checked by planting both, because "it would catch a rename" was the first thing claimed here and
//! it was wrong:
//!
//!   * **RENAMING** a counterpart does not reach this arm — every one of these fields has other
//!     users, so `cargo check` fails first with `no field named ...`. The compiler is the guard
//!     there, and this arm is redundant for that case.
//!   * **REMOVING** one together with its users **compiles cleanly** — the quantity silently stops
//!     being reported, every other test stays green, and only this arm fails. Planted exactly that
//!     on `best_compaction_candidate_ratio_ppm`: zero compile errors, and this arm failed naming
//!     the entry and its pairing note.
//!
//! That second case is the one a hand-written comparison has no defence against: nothing breaks, and
//! the mapping quietly becomes fiction.
//!
//! It checks **source**, not behaviour. It cannot verify two quantities mean the same thing — that
//! judgement is written beside each entry and is only as good as the reading behind it.
//!
//! # THE THREE THAT ARE GENUINELY UNRESOLVED, EACH FOR A DIFFERENT REASON
//!
//! Recorded rather than rounded away, because *why* a figure is absent is more useful than the fact:
//!
//!   * `retained_window_compacted_count` — `compaction_runs` is a **lifetime** count and
//!     `compaction_watermark` a **position**, so a count *within* a retained window needs runs
//!     tagged by position, which this store does not keep.
//!   * `skipped_disabled_chunk_endpoints` — `enabled` comes from a **per-cycle parameter** the
