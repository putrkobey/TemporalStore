// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE LOG-RESIDENT FACT HAS ONE SOURCE, AND THE STORED FIELD IS NOT IT.
//!
//! `BlockAddress::block_id()` is `None` exactly while a block still lives in the log, so the
//! address already answers "is this log-resident" and a stored flag beside it could only agree or
//! disagree. It did both: three sites derived it (`storage_bucket_internals.rs:362`, `:3655`,
//! `:3983`) and three wrote a constant `false` without consulting the address (`:3524`, `:5873`,
//! and a fixture in `engine_storage_state.rs`). Three maintain it, three override it — which is what makes the
//! invariant FALSE rather than merely unproven.
//!
//! # WHY THAT WAS INVISIBLE, AND WHY THE INVISIBILITY WAS LUCK
//!
//! The only behavioural reader is `object_manager`'s hot/cold classification, and it tests
//! `deleted` FIRST:
//!
//! ```text
//!     if page.deleted || object_deleted { ...deleted_block_ref_count... }
//!     else if bucket.in_memory() && !page.log_backed { ...hot... }
//!     else { ...cold... }
//! ```
//!
//! The one production site that writes the inconsistent flag is
//! `insert_container_tombstone_entry`, which sets `deleted: true` on the same entry. So that
//! reader never reached the flag for exactly the entries where it was wrong. **That protection is
//! the branch order of one `if`, not a property of the design** — reorder those branches and the
//! latent defect becomes live. It is the reason this is a fix rather than a tidy-up.
//!
//! # WHAT IT DID COST: THE REPORT
//!
//! `storage_reporting` asks "does any page in this bucket live in the log", and it asked the
//! stored flag. A bucket whose only log-resident page was a tombstone answered NO while a page was
//! in the log. The second test below drives exactly that bucket.
//!
//! # WHY THE FIELD IS GONE NOW, AND WHY IT WAS NOT A FOOTPRINT DECISION EITHER WAY
//!
//! This section said the field could not be cheaply deleted, and its arithmetic was right and is
//! still right: a `bool` leaving an entry carrying 52 bytes of field inside 56 buys **zero**
//! resident bytes. The entry is 56 bytes before this change and 56 after. Removing it was never
//! worth anything in width and is not being done for width.
//!
//! It is gone because the entry and the index-log row became ONE TYPE, and this field's own
//! documentation said its "VALUE IS NOT MAINTAINED and must not be consulted". Carrying a field
//! that is known to be wrong into a unified type is how it acquires readers: the next person sees
//! a field, not a paragraph. So the one fact it claimed to hold is now answerable only from the
//! address, through the accessor, and there is nothing left to disagree with it.
//!
//! AND ONE CLAIM HERE WAS SIMPLY WRONG, which is why it is corrected rather than deleted.
//! "Removing it refuses every existing index" names the wrong direction. `BlockIndex` derives its
//! `Deserialize` with no `deny_unknown_fields`, so an index written BEFORE this change still loads
//! -- serde ignores the key it no longer declares. The direction that breaks is the other one: a
//! NEW index read by an OLD binary, which finds a declared field absent. That is what the format
//! stamp is spent on, and it degrades to a refusal and a replay rather than to a misread.
//!
//! THE VALUE OF THE ORIGINAL CHANGE WAS CORRECTNESS, NOT FOOTPRINT. The same is true of this one.

#![allow(clippy::all)]
use super::*;

const OPERATOR_END: u32 = crate::DEFAULT_END_ROUTING_BUCKET;

/// THE LOG-RESIDENT FACT IS DERIVED, AND NOTHING CAN CONTRADICT IT ANY MORE.
///
/// THIS TEST REPLACES ONE THAT ASSERTED A DISAGREEMENT, on that test's own instruction. It said:
/// "If these agree, either the tombstone path started deriving the flag -- in which case this test
/// should be deleted rather than adjusted -- or the accessor stopped reading the address." The
/// stored flag is gone, so the first case has happened in the strongest available form: there is no
/// second source left to agree or disagree with the address.
///
/// SO THE SUBJECT CHANGED RATHER THAN THE ANSWER. The old test proved a defect was PRESENT. This
/// one proves it is UNREACHABLE, which is worth more: a test that shows a defect cannot be
/// represented outlives every fixture that shows one is currently absent.
///
