// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! AN ASSIGNED DURABLE ID DEFEATS GROUND ONE ONLY, AND GROUND ONE IS THE FIXABLE ONE.
//!
//! [`super::the_entry_cannot_name_its_object_by_slot`] refuses a bucket-relative ordinal on two
//! independent grounds. A later proposal reopens the leg from a different direction -- keep the
//! page index addressed by a `u64`, but make that `u64` an ASSIGNED, PERSISTED id rather than the
//! computed digest `block_index_handle` produces, so that the non-invertibility that kills the
//! digest route does not arise. This module prices that proposal, because it is the best-shaped
//! one the leg has had and the next reader will have it too.
//!
//! IT REACHES TWENTY-FOUR AND THAT PART IS REAL. The map arm already holds `(u64, BlockIndex)`, so
//! swapping a digest key for an assigned key costs no width at all, and the entry lands at
//! twenty-four with zero slack -- measured in [`super::entry_width_at_each_step`]. Every other
//! route to twenty-four that this campaign has priced reached a wider number than it claimed. This
//! one does not.
//!
//! IT STILL DOES NOT CLEAR THE REFUSAL, FOR ONE REASON STATED IN THE REFUSAL ITSELF. The slot
//! module's verdict is at its own line 160: the leg "is refused on Ground 2, and Ground 1 is the
//! reason a partial fix would not rescue it". Ground 1 is that positions do not survive a reload,
//! and the module calls it "a fixable defect" in as many words. Ground 2 is that THE ENTRY IS THE
//! ONLY DURABLE HOME OF THE KEY'S CHARACTERS, and the module's closing sentence on it is that it
//! "is not fixable without a durable ordinal-to-characters map, which is the second index this leg
//! exists to avoid."
//!
//! An assigned id is durable, so it answers Ground 1. It is a `u64`, so it answers nothing about
//! Ground 2. ASSIGNED VERSUS COMPUTED IS ORTHOGONAL TO THE BINDING OBJECTION, and the pins below
//! are what say so rather than the paragraph.
//!
//! AND THE THING THIS MODULE SAID WAS UNANSWERED HAS SINCE BEEN ANSWERED, FOR THIRTEEN KINDS OF
//! FIFTEEN. The claim above is that a `u64` hands nobody the characters, and it is still true of
//! the `u64`. What changed is that the characters do not have to come from the id:
//! [`super::where_the_characters_are_durable_per_kind`] measures them as already durable, per
//! kind, in the model map that kind's objects live in -- so `id -> characters` costs no new
//! storage, and Claim 6's "at least twenty-four for a persisted key-to-id map" is not what that
//! direction costs any more.
//!
//! WHAT CLAIM 6 STILL PRICES CORRECTLY IS THE OTHER DIRECTION, and this module is where the next
//! reader should start from rather than stop at. An ASSIGNED id -- unlike a computed one -- needs
//! `characters -> id`, and the per-kind maps are keyed BY the characters and carry no id.
//! [`super::what_an_assigned_id_costs_the_object_lookup`] prices the three homes that direction
//! can have: a new durable map (this leg's refused shape), a resident map rebuilt on load --
//! sixteen bytes per object worse at the floor, which is Claim 6's sign by another road -- and the
//! id carried inside the per-kind map's EXISTING value, where the two eight-byte terms cancel and
//! the net saving is exactly the characters' heap payload. Claim 5's one site is reopened there
//! too: it needs the characters, and after the census it has a source for them.
//!
//! # CLAIM 1 -- A DURABLE, RESTART-STABLE OBJECT ID ALREADY EXISTS, AND IT CONSUMES THE CHARACTERS
//!
//! `hashing::stable_block_object_id` at `hashing.rs:175` is FNV-1a over the shard, the kind and
//! the KEY BYTES. So the tree does not lack a stable id; it lacks a way back from one. The
//! signature pin below is the checkable form of that: the id is produced BY consuming the
//! characters, which is precisely why holding the id does not hand them back.
//!
//! # CLAIM 2 -- THE ID HAS TO BE THE MAP KEY, NOT A FIELD, OR THE WIDTH DOES NOT LAND
//!
//! Carried as a field in place of the shared pointer, the field sum is thirty-two and the entry
//! measures thirty-two. The mirror below measures it. So the proposal only reaches twenty-four in
//! the form where the id is the key of the holding map and the entry carries no identity term at
//! all -- which is the form Ground 2 is about.
//!
//! # CLAIM 3 -- "DURABLE BECAUSE THE LOG ITEM CARRIES IT" IS FALSE IN THIS TREE
//!
//! This is the trap most likely to be walked into again, because it is true of the protocol the
//! proposal was drawn from and false here. An index-log row DOES carry `object_key` as text, and
//! the slot module already records that it is written FROM the entry rather than independently of
//! it. The second half is the one that bites a persisted id:
//! `Engine::dump_and_reclaim_index_logs_with_min_reclaimable` at `persistence.rs:744` calls
//! `gc_reflected_before_anchor` at `persistence.rs:755`, whose own comment is "drop every record
//! the durable base already reflects", and the line below it reads "Pages at or below the anchor
//! are in the durable base now".
//!
//! SO THE LOG IS GIVEN BACK AT A DUMP. Anything whose only durable home is a log item has the
//! lifetime of the window between dumps. A persisted id would have to live in the BASE index --
//! and then so would the key-to-id map, which is the second index again.
//!
//! # CLAIM 4 -- THE READER SPLIT, WHICH IS WHERE THE REAL SCOPE IS
//!
//! The slot module's census stands and is the number that matters: fifty-nine production sites,
//! split by WHAT EACH SITE WANTS rather than by what it touches.
//!
//!   * TWENTY-EIGHT are served by an id. Fourteen compare the entry against a caller's string,
//!     six compare it against another in-tree row, two test a caller's set, one is the handle
//!     itself, and five reach the object lookup, which could be re-keyed.
//!   * TWENTY-TWO WANT THE CHARACTERS AND HAVE NO OTHER SOURCE. An assigned id changes none of
//!     them, which is the whole of this module.
//!   * ONE CANNOT BE SERVED AT ALL by any `u64`, and it is named in claim 5.
//!
//! # CLAIM 5 -- THE ONE SITE NO ID REACHES, AND WHY ITS OBVIOUS FIX IS A TAUTOLOGY
//!
//! `storage_bucket_internals.rs:2298` refuses a release when
//! `block_routing_bucket(&block.object_key, start, end) != routing_bucket`. The signature pin
//! below is the checkable claim: that function takes `&str`, so no id of any provenance can be fed
//! to it.
//!
//! AND THE ENTRY'S OWN `routing_bucket` FIELD IS NOT A SUBSTITUTE. The site's comment says it
//! "compares two INDEPENDENT things rather than a block's copy of its bucket against the bucket
//! holding it" -- where the block IS against where its KEY routes. Substituting the field makes
//! both sides the same answer, so the check could no longer fail. That is the shape this campaign
//! has shipped before: a detector replaced by a comparison of a value against itself, two lines
//! from a comment explaining why it must not be.
//!
//! WHICH OF THE TWO ROUTING NUMBERS IS MEANT, because this file warns three times that they
//! differ. Both this site and the reload it defends use `(key, start, end)` over the shard's
//! stamped range -- `shard.routing_range()` is read into the loop at `storage_bucket_internals.rs`
//! just above it, and the released-bucket supplement derives with the same three arguments. The
//! `(key, 0, u32::MAX)` form the warnings are about belongs to the removal path and is not either
//! side of this comparison.
//!
//! # CLAIM 6 -- THE TOTALS, WITH BOTH DENOMINATORS NAMED WHERE THE NUMBERS ARE WRITTEN
//!
//! A width step is not a footprint step, and this proposal is the case where the two have opposite
//! signs. The arithmetic is pinned below rather than stated. At the measured one page per object
//! THE ENTRY AND THE OBJECT ARE ONE TO ONE, so moving the key out of the entry to share it between
//! pages has nothing to share it between -- the saving a re-keying exists to capture is zero by
//! construction at that population, and a container to hold the key costs more than the sixteen
//! bytes the entry frees.
//!
//! PER ENTRY, NODE AGAINST NODE: forty today, twenty-four under this proposal. A real sixteen
//! bytes, and it is the number a node-to-node comparison should use.
//!
//! PER OBJECT, TOTAL AGAINST TOTAL, at one page per object: forty-eight today for the entry and
//! its map key; thirty-two under this proposal, plus at least twenty-four for a persisted
//! key-to-id map holding one shared pointer and one id per object. Fifty-six against forty-eight.
//! EIGHT BYTES WORSE, AND THAT IS A FLOOR -- it counts payload only, with no container overhead
//! for the new map and nothing at all for the id-to-characters direction the twenty-two readers of
//! claim 4 need.
//!
//! THESE TWO RATIOS HAVE DIFFERENT DENOMINATORS AND MUST NOT BE MIXED. The first is per entry
//! against a node layout; the second is per object against this tree's total. A design whose index
//! never holds the key at all pays its identity cost somewhere else entirely, so its total is not
//! comparable to our total by narrowing our node.
//!
//! # WHAT IS AVAILABLE AND UNPROVEN, recorded so it is not mistaken for part of this refusal
//!
//! `DirtyObjectIndex` maintains a BUCKET-keyed side (`bucket_counts`, `bucket_ids`,
//! `drain_buckets`) beside its key-keyed one. The dirty rollup in
//! `refresh_one_bucket_runtime_flags` -- one of the two readers in that function, and one of the
//! hardest cases because it holds only a bucket -- could therefore be answered from the bucket
//! number alone, needing neither the characters nor an id, and more cheaply than the per-page
//! probe it runs today.
//!
//! IT IS NOT ASSERTED HERE AND MUST NOT RIDE ON THIS MODULE. It widens the predicate from "any
//! page's object is dirty" to "any dirty object routes here", and the differing-numbers warning
//! above applies again: the bucket a dirty object is FILED under is not guaranteed to be the
//! bucket HOLDING its pages. It needs its own fail-first test with a control that can refuse.
//! The other reader in that function, the TTL minimum over `expires_at_ms`, has no bucket-keyed
//! counterpart at all -- that map is `BTreeMap<String, u64>` at `engine_storage_state.rs:129`, keyed by the
//! characters.

use std::mem::{align_of, size_of};
use std::sync::Arc;

use crate::block_store::ElementEntry;
use crate::engine::state::BlockIndex;
use crate::engine::storage_bucket_internals::StoredModelKind;

/// CLAIM 1, AS A TYPE. The stable object id is produced by CONSUMING the key bytes.
///
/// A signature pin rather than prose: if this function ever grows a form that takes an id instead
/// of a key, this line stops compiling and the claim above is reopened rather than silently
/// stale. It is the checkable half of "we already have a durable id and it is still not a way back
/// to the characters".
const _: fn(crate::types::ShardId, &str, &str) -> u64 =
    crate::engine::hashing::stable_block_object_id;

/// CLAIM 5, AS A TYPE. The routing bucket is a function of the key BYTES and nothing else.
///
/// This is the pin that bounds the whole route: no `u64` can be fed to it, so the release guard at
/// `storage_bucket_internals.rs:2298` cannot be served by any id, assigned or computed. If a form
/// taking an id is ever added, this line fails and the one unservable site becomes servable.
const _: fn(&str, u32, u32) -> u32 = crate::engine::hashing::block_routing_bucket;
