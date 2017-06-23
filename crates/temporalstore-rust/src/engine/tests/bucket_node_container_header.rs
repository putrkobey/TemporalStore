// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT THE BUCKET NODE'S 96 BYTES ARE SPENT ON, AND WHICH OF THEM THE CONTAINER HEADER IS.
//!
//! NINETY-SIX ON THIS BRANCH AND EIGHTY-EIGHT ON MAIN, AND THE DIFFERENCE IS ONE NAMED FIELD. This
//! module arrived from main written against 88. This branch took the element name off a page index
//! entry, and a removal's record of WHICH element it was about went to the node that holds the
//! entries: `tombstone_elements`, a `TombstoneElements` of one word. Every figure below is main's
//! plus that word, because no shape priced here can shed it -- it is eight-aligned, a whole word,
//! and not part of the small-field tail. The width pin and the field decomposition both REFUSED on
//! the merge rather than going quietly stale, which is what they are for.
//!
//! A per-bucket descriptor of 24 bytes is the width this campaign is aiming the node at. The node
//! is 96. The one-line explanation that has been carried from lane to lane is that the node holds
//! a CONTAINER of entries where a 24-byte descriptor holds a single address word, so the node's
//! width is set by a collection header while the narrow descriptor's is set by a pointer it can
//! overload with a tag.
//!
//! THAT EXPLANATION IS TRUE AND IT ACCOUNTS FOR LESS THAN A QUARTER OF THE DIFFERENCE. This module
//! takes the 72-byte shortfall apart into four groups, each derived from `size_of` over the live
//! declarations rather than written down:
//!
//!   * 24 B -- THREE LOG-SEQUENCE WORDS. `dirty_generation`, `first_dirty_wal_sequence` and
//!     `first_dirty_index_log_sequence`. A descriptor that does not anchor write-ahead-log
//!     reclaim has no equivalent of any of them.
//!   * 24 B -- A SECOND INDEX. `object_index` plus `deleted_object_index`: which object ids this
//!     bucket holds, and which it held. A descriptor that is itself per-item has no per-bucket
//!     object list to carry.
//!   * 16 B -- AND ONLY 16 -- THE CONTAINER HEADER ABOVE ONE WORD. `block_index` is 24 B; a single
//!     tagged address word would be 8. So the container costs 16 of the 72.
//!   * 8 B -- THE PER-ELEMENT TOMBSTONE ROWS, which is this branch's own addition and is the
//!     smallest group of the four.
//!
//! WHY THAT MATTERS MORE THAN THE ARITHMETIC. Every route anyone has proposed for this node so far
//! has been a route at the container, and the container is smaller than either of the two groups
//! it was being weighed against. It is no longer the SMALLEST of them all -- the tombstone rows
//! are, at half its size -- and that superlative is withdrawn where the table asserts it. Even
//! a container narrowed to one tagged word -- which this engine cannot reach, for the entry-width
//! reason below -- leaves the node at 80 against a goal of 24. The container is not what makes the
//! node wide; two jobs the narrow descriptor does not do are.
//!
//! AND THE ENTRY'S WIDTH IS INVISIBLE AT THE NODE, WHICH IS THE CLAIM THIS MODULE EXISTS TO PIN.
//! `BlockIndexMap` holds its single block behind a `Box` and its many blocks behind a `Vec`, so
//! NEITHER arm exposes the entry's width to the node. `the_node_cannot_see_how_wide_an_entry_is`
//! instantiates the same three-arm shape over stand-in entries of 8, 40, 56 and 200 bytes and
//! asserts one width for all four. A landing that narrows the entry therefore moves this structure
//! by zero, and the arithmetic says so before anyone measures it. That is not a prediction about
//! one change; it is a property of holding entries behind pointers, and it will hold for the next
//! entry narrowing too.
//!
//! WHAT IS ACTUALLY REACHABLE AT THE CONTAINER, and it is LESS THAN THE ARITHMETIC SUGGESTS.
//! This module was drafted asserting a 24 -> 16 -> 8 ladder: drop the `Vec`'s capacity word to
//! reach 16, then put the length inside the allocation to reach 8. BOTH RUNGS WERE WRONG, measured,
//! and the reason is the same one that refuses `ObjectIndex` at 8 -- A NICHE GIVES ONE SPARE
//! ENCODING AND THREE ARMS NEED TWO:
//!
//!   * 24 -> 24. Swapping the many-entry arm's `Vec` for a boxed slice drops a word of PAYLOAD and
//!     buys NOTHING, because the shape then has two 16-byte arms and no niche left to tell them
//!     apart, so the aligner hands the saved word straight back as a tag. The capacity word was
//!     being carried for free inside the tag's rounding.
//!   * 16, not 8. Putting BOTH arms behind a single pointer gets the handle to 16 and no further,
//!     for the same reason: two 8-byte pointer arms plus an empty arm is three variants over one
//!     null niche.
//!
//! So every SAFE three-arm shape floors at 16, which leaves the node at 88. One word is reachable
//! only by hand-rolling the tag into the pointer's low bits, which is unsafe code on the read path
//! and leaves the node at 80. `the_container_header_ladder_floors_at_sixteen_not_eight` asserts all
//! four rungs and the node width each lands on. Every rung below the first also trades the capacity
//! word for a reallocation on every growth step, which is a HEAP question and not a width one --
//! `inline_arm_trade.rs` is where that trade gets priced, and this module deliberately does not
//! price it. What this module establishes is the CEILING: 88 B by any safe shape, 80 B with a
//! hand-rolled tag, and 72 B if the small-field tail went too.
//!
//! WHAT REDDENS THIS MODULE, each one driven rather than asserted to work:
//!
//!   * INLINING the single arm of the shared mirror reddens the CONTROL in all six tests and not
//!     the invariance assertion, because a mirror that inlines has by definition stopped matching
//!     a declaration that does not. Recorded because it is a mutation that LOOKS like a fail-first
//!     for `the_node_cannot_see_how_wide_an_entry_is` and is not one -- the control got there
//!     first, which is the trap of a guard that passes under mutation because an earlier stage
//!     already did the work.
//!   * WIDENING the hand-rolled tag stand-in to two words reddens exactly one test, at the ladder
//!     equality itself, with the control and the other five green. That is the fail-first for the
//!     ladder.
//!   * ADDING a field to the node reddens the exhaustive pattern at compile time, as above.
//!   * `#[repr(align(16))]` ON `ObjectIndex` reddens the alignment pin for that type and nothing
//!     else in the width block, because the type is already 16 B wide so its width pin stays
//!     GREEN. That is the fail-first for the alignment pins, and it is the same run that shows
//!     the width pins do not cover the alignments.
//!
//! THE DECOMPOSITION IS TAKEN OVER A REAL INSTANCE, AND THE CLAIM THAT MOTIVATED THAT WAS WRONG.
//! This module was drafted saying that `every_byte_of_the_bucket_node_is_accounted_for` in
//! `per_item_byte_budget.rs` would stay GREEN on a field added to the node, because its guard is
//! `table.len() == 10` and a ten-row table stays ten rows when the struct grows to eleven. MEASURED
//! BY PLANTING AN ELEVENTH FIELD -- TWELFTH ON THIS BRANCH -- (`spare: u8`, chosen because the tail
//! absorbs it and the width stays put, so the width assertions stay REACHABLE rather than becoming
//! compile errors): that
//! module does NOT stay green. It fails to compile, at two `BucketNode { .. }` struct literals
//! (E0063). The table is blind; the literals beside it are not, and they are what covers it.
//!
//! SO WHAT THE EXHAUSTIVE PATTERN HERE ADDS IS NARROWER THAN THE DRAFT CLAIMED, and it is still
//! worth having. It fails AT THE DECOMPOSITION (E0027, `pattern does not mention field`), so the
//! error names the structure whose sum has gone wrong rather than an unrelated constructor; and it
//! keeps failing if those two literals ever become `..Default::default()`, at which point the
//! table's row count is the only thing left and it does not count fields. Widths come from
//! `size_of_val` on the pattern's own bindings rather than from a type named a second time, so a
//! field whose declared TYPE moved is caught here and would be summed at its old width there.
#![allow(clippy::all)]
use std::mem::{align_of, align_of_val, size_of, size_of_val};

use crate::engine::state::{
    BlockIndex, BlockIndexMap, BucketFlags, BucketLayoutState, BucketNode, BucketTtl,
    DeletedObjectIndex, ObjectIndex, TombstoneElements,
};

/// The per-bucket width this campaign is aiming the node at.
const GOAL: usize = 24;

/// One machine word here, derived rather than written as 8.
const WORD: usize = size_of::<u64>();

// =============================================================================================
// THE WIDTHS. Const pins, so every build checks them and not a test someone has to run.
//
// WHAT USED TO BE HERE AND WHY IT IS GONE. Each pin carried a `!= N-1 && != N+1` partner on the
// line below it, prescribed across this campaign as "proof the width is a value and not a bound
// in either direction". It proved nothing, for a reason that does not depend on any width: the
// partner sat in the SAME const context as the `== N` above it, and `x == N` already entails
// `x != N-1` and `x != N+1`. An inequality dominated by an equality over the same expression can
// distinguish no state the equality admits, so it could only fail in a build that was already
// failing one line higher. That is the whole argument, and it holds at every N and every
// alignment. Rewriting it at a REACHABLE neighbour -- `!= 16 && != 32` for a 24-byte type -- is
// the same mistake with better arithmetic: still dominated, still unable to name a single case
// that `== 24` lets through.
//
// THE ALIGNMENT IS WHY IT LOOKED HARMLESS, NOT WHY IT WAS EMPTY, and it is worth recording
// because it makes the partner worse than redundant. `size_of` is always a multiple of
// `align_of`, and all FIVE of these types are 8-aligned (pinned below; measured at 96, 24, 16, 8
// and 8 over align 8 -- the node is 96 here and `TombstoneElements` is this branch's fifth).
// N-1 and N+1 are therefore 7 and 1 mod 8, which no layout of an 8-aligned type
// can reach: the partner was not a weak claim about this engine, it was a theorem about Rust
// layout wearing this engine type names. Driven rather than argued -- on main a planted `u64`
// field took the node from 88 to 96, reddened the `== 88` there, and left `!= 87 && != 89`
// GREEN. The same run on this branch moves 96 to 104 against the `== 96` below. It could
// not see a real width change in either direction, which is the one thing it was written for.
//
// NOTE THE INVERSION, because it is how the shape survived a dozen briefs. `BucketTtl`,
// `BucketLayoutState` and `BucketFlags` carry no partner, and the two 1-byte types are the only
// ones here whose +-1 neighbours a layout can reach at all -- 0 and 2 are both multiples of 1.
// The partner was present exactly where it was unreachable and absent exactly where it was
// reachable. Under domination that is moot, it would have added nothing at one byte either, and
// that is the point: which neighbour to name was never the useful question.
// =============================================================================================

// NINETY-SIX ON THIS BRANCH, NOT EIGHTY-EIGHT, AND THE DIFFERENCE HAS A CAUSE.
//
// This pin arrived from main reading `== 88` and went red on the merge. That is the pin working: a
// width pin should break when the width moves, and this one did rather than going on describing a
// structure this branch does not have. It is restated and NOT deleted -- it is the thing that
// caught the difference. Main's own value is kept as a TERM in the relation below rather than as a
// second pin, so the two branches' numbers cannot drift apart silently.
//
// THE CAUSE IS `tombstone_elements`, the eleventh field. A container removal records which ELEMENT
// it was about, and that row left the page index entry for the bucket node that holds it, so the
// node carries `TombstoneElements` -- `Option<Box<Vec<TombstoneElement>>>`, one word through the
// null niche. Eighty-eight plus that word is ninety-six, and the relation is ASSERTED below rather
// than left for a reader to infer, so 88-versus-96 reads as a difference with a named cause
// instead of a discrepancy between two branches.
//
// THE NEIGHBOUR BRACKETS THESE TWO PINS CARRIED ARE GONE, which is the other half of this merge.
// They read `!= 95 && != 97` and `!= 7 && != 9`, and the argument against them is the one written
// at the head of this block, arriving here with #2127: an inequality in the same const context as
// an equality over the same expression can distinguish no case the equality admits. Restating a
// bracket at this branch's width would have carried the defect across the merge in the one module
// main had just cleared it out of.
const _: () = assert!(size_of::<BucketNode>() == 96);

// THE WORD THE ROWS COST, pinned on its own so the relation below cannot be satisfied by two
// numbers that moved together.
const _: () = assert!(size_of::<TombstoneElements>() == 8);
// AND THE DIFFERENCE, STATED AS ARITHMETIC. 88 is main's width and is written here as the term it
// is: whoever next moves either side has to move this line deliberately.
const _: () = assert!(88 + size_of::<TombstoneElements>() == size_of::<BucketNode>());

const _: () = assert!(size_of::<BlockIndexMap>() == 24);
const _: () = assert!(size_of::<ObjectIndex>() == 16);
const _: () = assert!(size_of::<DeletedObjectIndex>() == 8);
const _: () = assert!(size_of::<BucketTtl>() == 8);
const _: () = assert!(size_of::<BucketLayoutState>() == 1);
const _: () = assert!(size_of::<BucketFlags>() == 1);

// =============================================================================================
// THE ALIGNMENTS, AND THESE ARE NOT DOMINATED BY THE WIDTHS ABOVE.
//
// A width pin admits every alignment that DIVIDES it: `== 96` admits 1, 2, 4 and 8, `== 24`
// admits 1, 2, 4 and 8, `== 16` admits those and 16, `== 8` admits 1, 2, 4 and 8. So each line
// below can fail in a state that every assertion above this comment accepts -- which is the test
// the deleted partner failed, and the reason these five lines are a different claim rather than a
// rephrased bracket.
//
// AND THE ARITHMETIC IN THIS MODULE IS WRITTEN OVER THEM, which is why the omission mattered.
// `every_byte_of_the_node_sums_over_an_instance_and_not_a_table` partitions the fields on
// `align == align_of::<BucketNode>()` and then reconstructs the width as
// `eight_aligned + round_up(tail, align_of::<BucketNode>())`. That value is the PREDICATE and the
// MODULUS of the decomposition, `per_item_byte_budget.rs` partitions on it as well, and until
// these lines it was pinned nowhere in this crate. At align 4 the partition moves every 8-byte
// field into the tail and the module fails at `eight_aligned == 80`: a message naming the group
// width when the cause is the alignment. Pinned here it fails where it IS the premise, and at
// compile time, which is also where `cargo check` can see it -- the runtime `assert_eq!` on
// `align_of::<BlockIndex>()` in the control below cannot be.
//
// THE DOC ABOVE LEANS ON THEM TOO. The ladder floor of 16 and the refusal of `ObjectIndex` at 8
// are both niche-and-tag arguments, and the tag width there is the payload alignment; `engine_storage_state.rs`
// writes that identity out for `ObjectIndex` as `size == align_of + size_of::<Box<ObjectSlots>>`.
// All of it is arithmetic in the alignment and none of it was asserted.
//
// FAIL-FIRST, DRIVEN. `#[repr(align(16))]` on `ObjectIndex` leaves `size_of::<ObjectIndex>()` at
// 16, so the width pin above stays GREEN and the deleted partner would have stayed green too, and
// it reddens the `ObjectIndex` line below. A real layout change that every width assertion in
// this module is blind to is visible here.
// =============================================================================================

const _: () = assert!(align_of::<BucketNode>() == 8);
const _: () = assert!(align_of::<BlockIndexMap>() == 8);
const _: () = assert!(align_of::<ObjectIndex>() == 8);
const _: () = assert!(align_of::<DeletedObjectIndex>() == 8);
// THE FIFTH IS THIS BRANCH'S, and it is a premise rather than a flourish: the field partition
// in `every_byte_of_the_node_sums_over_an_instance_and_not_a_table` puts `tombstone_elements`
// in the eight-aligned group BECAUSE it is 8-aligned, and the `88 + size_of::<TombstoneElements>()`
// relation above is only the node's width if this word does not pad.
const _: () = assert!(align_of::<TombstoneElements>() == 8);

// =============================================================================================
// STAND-IN ENTRIES, AND THE SHAPE THAT HOLDS THEM.
// =============================================================================================

/// An entry stand-in of `N` words.
///
/// Eight-aligned like the live entry, which is asserted rather than assumed: a stand-in of a
/// different alignment would lay out differently inside the arms below and the invariance this
/// module claims would be a claim about alignment instead.
#[repr(transparent)]
struct Entry<const N: usize>([u64; N]);

/// The live block index's shape, over any entry type.
///
/// Spelled once and instantiated, so the four widths compared below cannot differ in anything but
/// the entry.
#[allow(dead_code)]
enum MirrorIndex<E> {
    Empty,
    One(u64, Box<E>),
    Many(Vec<(u64, E)>),
}

/// The same, with the many-entry arm's capacity word dropped.
#[allow(dead_code)]
enum MirrorIndexBoxedSlice<E> {
    Empty,
    One(u64, Box<E>),
    Many(Box<[(u64, E)]>),
}

/// A length-prefixed allocation: what a one-word container handle would have to point at.
#[allow(dead_code)]
struct LengthPrefixed<E> {
    len: usize,
    capacity: usize,
    first: E,
}

/// The same shape again, with BOTH arms behind a single pointer.
///
/// DRAFTED AS "the narrowest a three-arm handle can be: one word". It is not -- measured at 16,
/// because two pointer arms and an empty arm are three variants over ONE null niche and the
/// aligner adds a tag word. Kept as the rung it actually is.
#[allow(dead_code)]
enum MirrorIndexOneWord<E> {
    Empty,
    One(Box<(u64, E)>),
    Many(Box<LengthPrefixed<E>>),
}

/// A hand-rolled tagged pointer: the tag in the pointer's low bits, which a `Box` cannot express.
///
/// The ONLY shape that reaches one word here. It is a raw word with an unsafe discipline around
/// every read, and it is priced as a width only -- what it would cost the read path is not this
/// module's measurement.
#[allow(dead_code)]
struct MirrorIndexHandRolledTag(usize);
/// The live node, mirrored field for field so a width below is a width of something the engine
/// actually holds. `assert_mirrors_track_the_declaration` is what keeps the two in step.
///
/// `tombstone_elements` IS PART OF THE MIRROR ON THIS BRANCH. Without it the mirror measured 88
/// against a live node of 96, and the tracking assertion -- which every test here calls first --
/// would have failed at runtime in all six, after the compile errors were cleared. A mirror that
/// has stopped mirroring is the one failure this module cannot absorb, because every number in it
/// is read off these stand-ins.
struct MirrorNode<I> {
    routing_bucket: u32,
    layout: BucketLayoutState,
    flags: BucketFlags,
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: I,
    tombstone_elements: TombstoneElements,
}

/// The live node with the whole small-field tail gone.
///
/// Not a proposal -- `flags` is live state and `routing_bucket`'s removal is priced elsewhere as a
/// stored-format change for zero bytes. It is here to establish that the tail is ONE rounding, so
/// a claim that some single tail field is worth narrowing can be refused with a number.
///
/// IT KEEPS `tombstone_elements`, because that field is not tail: it is eight-aligned and a whole
/// word, so removing the tail does not touch it.
#[allow(dead_code)]
struct MirrorNodeEmptyTail<I> {
    ttl_ms: BucketTtl,
    dirty_generation: u64,
    first_dirty_wal_sequence: u64,
    first_dirty_index_log_sequence: u64,
    object_index: ObjectIndex,
    deleted_object_index: DeletedObjectIndex,
    block_index: I,
    tombstone_elements: TombstoneElements,
}

// =============================================================================================
// THE CONTROLS. Both run first, in every test that prices anything.
// =============================================================================================

/// The mirror tracks the declaration, or nothing below describes this engine.
fn assert_mirrors_track_the_declaration() {
    assert_eq!(
        size_of::<BlockIndexMap>(),
        size_of::<MirrorIndex<BlockIndex>>(),
        "the mirror of the block index is {} B against the declaration's {} B, so every width \
         below is a width of something this engine does not hold",
        size_of::<MirrorIndex<BlockIndex>>(),
        size_of::<BlockIndexMap>()
    );
    assert_eq!(
        size_of::<BucketNode>(),
        size_of::<MirrorNode<BlockIndexMap>>(),
        "the mirror of the node is {} B against the declaration's {} B",
        size_of::<MirrorNode<BlockIndexMap>>(),
        size_of::<BucketNode>()
    );
    assert_eq!(
        8,
        align_of::<BlockIndex>(),
        "the live entry is {}-aligned; the word-array stand-ins below are 8-aligned and would not \
         lay out as it does",
        align_of::<BlockIndex>()
    );
}

// =============================================================================================
// THE DECOMPOSITION, OVER A REAL INSTANCE.
// =============================================================================================

/// EVERY BYTE OF THE NODE, SUMMED OVER AN INSTANCE THE ENGINE BUILT.
///
/// `size_of_val` over the fields of a `BucketNode::default()`, so the sum reads the DECLARATION
/// and not a table beside it. The eight-aligned group and the small-field tail are summed
/// separately and the slack is named as the aligner's, because a width quoted without its sum
/// cannot tell a structure that is full from one that is a third padding, and those two want
/// opposite fixes.
///
/// THE FIELD SET IS ENUMERATED BY THE COMPILER, not by a table. The node is destructured with an
/// EXHAUSTIVE pattern and no `..`, so a field added to the declaration is a compile error here
/// rather than a row this test silently fails to sum. That is the half a `table.len() == 10` check
/// cannot cover: it asserts the table has ten rows, which stays true when the struct grows to
/// eleven. And because every width below comes from `size_of_val` on the binding rather than from
/// a type named a second time, a field whose declared TYPE changed is caught too -- the other half.
/// Between them a type change, a rename, an addition and a removal all fail, and three of the four
/// fail at compile time.
#[test]
fn every_byte_of_the_node_sums_over_an_instance_and_not_a_table() {
    assert_mirrors_track_the_declaration();

    let node = BucketNode::default();

    // EXHAUSTIVE. No `..` -- adding a field to `BucketNode` must not compile here.
    //
    // AND IT DID NOT, WHICH IS WHY `tombstone_elements` IS NAMED HERE. This pattern listed ten
    // fields and refused to compile on the merge (E0027, "pattern does not mention field") because
    // this branch carries an eleventh: the per-element rows a removal files, which left the page
    // entry for the node that holds them. That is the guard this module's own header says it is
    // for, firing on exactly the case it was written against.
    let BucketNode {
        routing_bucket,
        layout,
        flags,
        ttl_ms,
        dirty_generation,
        first_dirty_wal_sequence,
        first_dirty_index_log_sequence,
        object_index,
        deleted_object_index,
        block_index,
        tombstone_elements,
    } = &node;

    // Each field's width AND alignment read off the binding. No type is named twice.
    let fields: [(&str, usize, usize); 11] = [
        ("routing_bucket", size_of_val(routing_bucket), align_of_val(routing_bucket)),
        ("layout", size_of_val(layout), align_of_val(layout)),
        ("flags", size_of_val(flags), align_of_val(flags)),
        ("ttl_ms", size_of_val(ttl_ms), align_of_val(ttl_ms)),
        ("dirty_generation", size_of_val(dirty_generation), align_of_val(dirty_generation)),
        (
            "first_dirty_wal_sequence",
            size_of_val(first_dirty_wal_sequence),
            align_of_val(first_dirty_wal_sequence),
        ),
        (
            "first_dirty_index_log_sequence",
            size_of_val(first_dirty_index_log_sequence),
            align_of_val(first_dirty_index_log_sequence),
        ),
        ("object_index", size_of_val(object_index), align_of_val(object_index)),
        (
            "deleted_object_index",
            size_of_val(deleted_object_index),
            align_of_val(deleted_object_index),
        ),
        ("block_index", size_of_val(block_index), align_of_val(block_index)),
        (
            "tombstone_elements",
            size_of_val(tombstone_elements),
            align_of_val(tombstone_elements),
        ),
    ];

    println!("\n=== BucketNode, summed over an instance ===");
    println!("  {:<34} {:>5} {:>6}", "field", "size", "align");
    let mut eight_aligned = 0usize;
    let mut tail = 0usize;
    for (name, size, align) in &fields {
        println!("  {name:<34} {size:>5} {align:>6}");
        if *align == align_of::<BucketNode>() {
            eight_aligned += size;
        } else {
            tail += size;
        }
    }
    let sum = eight_aligned + tail;
    let slack = size_of::<BucketNode>() - sum;
    println!(
        "  -> {eight_aligned} B eight-aligned + {tail} B tail = {sum} B of field, {slack} B slack, \
         {} B declared",
        size_of::<BucketNode>()
    );

    // NINETY-FOUR IN NINETY-SIX, WHICH IS MAIN'S 86-IN-88 PLUS THE TOMBSTONE WORD. The
    // eight-aligned group takes the whole of it -- `TombstoneElements` is one word and 8-aligned --
    // so the tail and the slack are untouched at 6 and 2. All four numbers are asserted separately
    // because a sum that moved with its width would hide a field that changed shape.
    assert_eq!(
        94, sum,
        "the fields sum to {sum} B and not 94; a field has been added, removed or retyped and \
         every figure in this module is about the previous declaration"
    );
    assert_eq!(88, eight_aligned, "the eight-aligned group is {eight_aligned} B and not 88");
    assert_eq!(6, tail, "the small-field tail is {tail} B and not 6");
    assert_eq!(2, slack, "the aligner's slack is {slack} B and not 2");
    assert_eq!(
        size_of::<BucketNode>(),
        eight_aligned + tail.div_ceil(align_of::<BucketNode>()) * align_of::<BucketNode>(),
        "eight-aligned + round_up(tail) does not reconstruct the width, so this is not the layout \
         rule it claims to be"
    );
}

// =============================================================================================
// THE SHORTFALL, IN FOUR GROUPS.
// =============================================================================================

/// WHERE THE 72 BYTES ABOVE THE GOAL ACTUALLY ARE, and the container is not the whole of it.
///
/// Both arms of every ratio are named where the ratio is written. The denominator throughout is
/// `size_of::<BucketNode>() - GOAL` = the whole shortfall, 72 B; the numerators are the four
/// groups, each derived from the live declarations.
///
/// THREE GROUPS BECAME FOUR, AND THE PARTITION IS WHAT CAUGHT IT. This test arrived from main
/// summing three groups against a 64 B shortfall. The shortfall is DERIVED -- `size_of` minus the
/// goal -- so on this branch it reads 72, and the three groups still summed to 64: the assertion
/// failed with "the three groups sum to 64 B against a 72 B shortfall, so they do not partition
/// it and the percentages above have no denominator". That is a partition assertion doing exactly
/// what it is for, which is why the eight bytes get a NAMED group rather than being folded into
/// one of the three.
///
/// THE FOURTH GROUP IS THE TOMBSTONE ROWS. A container removal records which ELEMENT it was about;
/// that row left the page index entry for the bucket node that holds it, so the node carries
/// `TombstoneElements` -- one word through the null niche.
///
/// AND THE HEADLINE CLAIM IS NARROWED, NOT KEPT. This read "the container is the smallest group",
/// and it is not any more: at 16 B it is twice the tombstone rows' 8. What the two inequalities
/// below assert is what they always asserted -- that the container is smaller than the
/// log-sequence words and smaller than the object-index pair -- and those both still hold. The
/// superlative was a reading of a three-group table, and it does not survive a fourth.
#[test]
fn the_container_header_is_a_quarter_of_the_shortfall_and_not_the_whole_of_it() {
    assert_mirrors_track_the_declaration();

    let node = BucketNode::default();
    let shortfall = size_of::<BucketNode>() - GOAL;

    // Group 1: the log-sequence words.
    let log_sequences = size_of_val(&node.dirty_generation)
        + size_of_val(&node.first_dirty_wal_sequence)
        + size_of_val(&node.first_dirty_index_log_sequence);
    // Group 2: the second index.
    let object_pair = size_of_val(&node.object_index) + size_of_val(&node.deleted_object_index);
    // Group 3: what the container header costs ABOVE one tagged address word.
    let container_above_one_word = size_of_val(&node.block_index) - WORD;
    // Group 4: the per-element tombstone rows, read off the field rather than written as 8.
    let tombstone_rows = size_of_val(&node.tombstone_elements);

    println!("\n=== the {shortfall} B above the {GOAL} B goal, in four groups ===");
    for (name, bytes) in [
        ("three log-sequence words", log_sequences),
        ("a second index (object ids held, and held-and-deleted)", object_pair),
        ("the container header above one tagged word", container_above_one_word),
