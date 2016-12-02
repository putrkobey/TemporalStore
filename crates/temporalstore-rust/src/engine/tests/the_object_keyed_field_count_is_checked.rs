// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The object-keyed field count on `ShardState` is a CHECKED number, not a claimed one.
//!
//! # WHY THIS EXISTS
//!
//! The consolidation campaign is measured against "how many maps can a key live in", and that
//! number was reported as **eight** when it was **nine**. `control_state_changes` was missed, and
//! the reason is worth more than the correction: the query that produced the count required
//! `name: type,` to sit on ONE source line, and that field wraps its type onto a second.
//!
//! So the count read LOW. That is the dangerous direction mid-campaign, because a low count looks
//! like progress rather than like a broken needle -- the same failure as the turbofish that
//! defeated a `name(` query and reported 5 of 12.
//!
//! The same query read **18 fields of 30** on this struct for the same reason, and joining wrapped
//! types is what took it to 30. That is worth stating precisely, because I first attributed the
//! 18 to a truncated struct body and it was not: a plant showed the naive body query is fine here.
//! **One defect, named correctly, beats two guessed at.**
//!
//! The lesson the sibling guard in this directory states is the one that applies:
//! **when the population is characterised by a type, the type is the enumeration.** A claim of the
//! form "there are N such fields" should name the mechanical query that produced it so a reader can
//! re-run it. This test IS that query, and it prints its enumeration so the number is auditable
//! rather than merely asserted.
//!
//! # WHAT IT COUNTS, AND THE THREE DENOMINATORS
//!
//! "A map" means three different things after the domain-keyed merges, and conflating them is how
//! the headline flatters itself:
//!
//!   * **object-keyed fields** -- a field whose OUTERMOST key is an object key, plus a
//!     domain-keyed field counted ONCE. This is the campaign's denominator.
//!   * **object-keyed tables** -- each domain of a domain-keyed field counted separately. This is
//!     the one that says where a key can actually live.
//!   * **map fields** -- every field holding a map at all.
//!
//! This guard pins the first and prints all three.
//!
//! # THE PART THAT MAKES IT EVIDENCE
//!
//! A source-level count is only as good as its ability to see every spelling, so the arm below
//! asserts that it finds the field that the previous query MISSED. Without that, a guard written
//! with the same blind spot would reproduce the same wrong number and assert it confidently.
//!
//! Verified by planting, both sides:
//!
//! | plant | result |
//! |---|---|
//! | drop the wrapped-type joining, restoring the one-line query | **REFUSED** -- names the field |
//! | swap brace-matching for the naive body query | passes, so that arm is defensive only |
//!
//! The second is reported rather than quietly kept, because a green arm has two readings and this
//! one is "nothing needs it", not "it works".

