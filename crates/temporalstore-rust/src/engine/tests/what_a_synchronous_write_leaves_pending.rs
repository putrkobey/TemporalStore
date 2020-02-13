// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A SYNCHRONOUS WRITE LEAVES PENDING, which decides whether we already write through.
//!
//! # THE QUESTION
//!
//! The design being followed writes through per object: extents reach storage before the manifest
//! entry commits, so nothing is ever pending write-back and no set of dirty objects is needed. Ours
//! keeps `dirty_objects`, and the standing reading has been that this is a divergence in the write
//! path.
//!
//! **One name is enough to doubt that.** The only two sites that mark an object dirty are reached
//! through `mark_object_dirty`. If dirtying is a property of the ASYNC path, then
//! `dirty_objects` is the cost of an option they do not offer rather than the cost of our default,
//! and a synchronous write already does what theirs does.
//!
//! That is the difference between a configuration and an architecture change, so it is measured
//! rather than argued: the same corpus written twice, once synchronously and once through the async
//! path, with the write-back set read after each.
//!
//! # AND THE ANSWER IS THE SECOND ONE
//!
//! **Both paths leave the whole corpus pending — 64 of 64.** So the name describes the caller it
//! was first written for, not a condition: dirtying is unconditional, a synchronous write does not
//! write through, and write-through has to be *built* rather than selected. The reasoning from the
//! name was wrong and this arm is what says so.
//!
//! # WHAT MAKES THAT BUILD AFFORDABLE, AND WHERE IT WOULD NOT BE
//!
