// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ONE RECORDED MAP FOR EVERY MODEL KIND.
//!
//! `RecordedHashContainer` and `RecordedSetContainer` were two types with one shape. This is that
//! shape, once, generic over the kind -- and the two of them are gone, not kept beside it.
//!
//! # WHAT MADE THIS A GENERALISATION RATHER THAN A REWRITE
//!
//! [`super::ElementMap`] already existed and already spanned every kind. It is the level-2
//! abstraction -- the elements of ONE object -- and the tree ships three implementations that
//! between them cover all five maps this type is for:
//!
//!   * `HashFieldMap` -- hash, element `String`, value `BlockAddress`
//!   * the blanket `BTreeMap<E, V>` -- set (`Vec<u8>`), zset (value `(u64, BlockAddress)`),
//!     list (`i64`), features (`u64`)
//!   * `HashMap<String, V>`
//!
//! So nothing in the key types resists, and a zset needs no composite key: it is a `BTreeMap`
//! whose VALUE is the score-plus-address pair. More importantly `merge_container_elements` and
//! `fill_absent_elements` are ALREADY generic over `ElementMap`, so the two heaviest exceptions --
//! the delta fold and the reconcile -- generalise for free rather than being written twice more.
//!
//! # WHAT WE TAKE FROM THE SHAPE, AND THE TWO THINGS WE DELIBERATELY DO NOT
//!
//! We take one generic map, three mutators, and deletion expressed as a value rather than a flag.
//!
//! WE DO NOT TAKE A MONOTONIC TIMESTAMP IN THE WRAPPER. Our index log already carries sequence
//! numbers, so a stamp here would make one fact answerable from two places, and the two could
//! disagree. The thing a stamp would order is already ordered.
//!
//! WE DO NOT TAKE THE SINGLE-FUNCTION EMISSION. The shape being generalised here logs and then
//! assigns inside ONE function, which means the ordering is a convention inside that function: a
//! future edit can delete the log call and the code still compiles. Ours emits FIRST and the
//! emitter returns a WITNESS -- a struct whose single field is private to the emitter's own module,
//! so nothing else can construct one -- and the mutator REQUIRES it. Delete the emission and the
//! witness is gone, the proof cannot be built, and the call stops compiling.
//!
//! THAT DISTINCTION IS NOT THEORETICAL AND IT COST A ROUND TO FIND. The first version of the
//! one-call surface built the proof from the CALLER'S OWN ARGUMENTS. Deleting the emission compiled
//! cleanly and the defect surfaced only at run time, as `staged 0 records, not 1`, while the doc
//! comment claimed the compiler would catch it -- a guard holding a false claim. The witness exists
//! because driving that test to fail exposed the gap between the claim and the code. It is the one
//! place this design is deliberately stricter than the shape it follows, and it should not be
//! described as matching it.
//!
//! # THE EXCEPTIONS ARE PER-KIND DATA, NOT A COMMENT
//!
//! Five paths mutate a model map WITHOUT recording, because on each the durable record is the
//! SOURCE of the change rather than its consequence. Three are shared by every kind. Two are NOT,
//! and the asymmetry is the thing two concrete containers taught that one could not have:
//!
//!   * [`ReplaysInstallsUnrecorded`] -- hash only. Recovery's `context_node` arm installs a hash
//!     element and files NO record of its own, because a context node is never registered in the
//!     bucket index. The `set` replay arm re-files its block, so it needs no such path.
//!   * [`RepacksAfterDecode`] -- set, zset, list, features; NOT hash. A decode fills a `BTreeMap`
//!     by ascending insertion, the one order that leaves it half empty. `HashFieldMap` is a sorted
//!     vector whose length is its capacity, so it has nothing to pack.
//!
//! These are MARKER TRAITS, so the asymmetry is two `impl` lines and the compiler enforces it: a
//! kind that does not declare the marker cannot call the method at all. Flattening them into one
//! surface with a runtime check, or into a comment saying "only hash does this", would have thrown
//! away the only part of this that two instances established.

use super::state::ModelKey;
use std::collections::{HashMap, HashSet};

use crate::block_store::ElementEntry;
use crate::ShardId;

use super::state::ShardState;
use super::storage_bucket_internals::{BlockFiled, ObjectDeletionFiled};
use super::{ElementMap, LiveBlockKey};

/// What distinguishes one model kind from another, and nothing else.
pub(super) trait RecordedKind: Sized + 'static {
    /// The elements of one object, which is the level-2 container.
    type Elements: ElementMap;

    /// The model kind as the bucket index spells it. Spelled ONCE per kind here, so no arm can
    /// misspell it and no two arms can disagree.
    const KIND: &'static str;

    /// How this kind's map is encoded, and there is deliberately NO DEFAULT.
    ///
    /// THIS EXISTS BECAUSE ITS ABSENCE WAS A WIRE REGRESSION, caught by three reload tests. The
    /// first version of this generic type had one blanket `Serialize` doing
    /// `self.entries.serialize(..)` for every kind. That is correct for `hashes`, whose field was a
    /// plain `#[serde(default)]` map -- and WRONG for `sets`, whose field carried
    /// `#[serde(default, with = "super::set_index_serde")]` precisely because a
    /// `BTreeMap<Vec<u8>, _>` does not round-trip through a JSON object key. Collapsing the two
    /// containers silently dropped that codec: `set_index_serde` was left referenced only from
    /// comments, and a set's map stopped surviving a reload.
    ///
    /// So the codec is a REQUIRED associated function. A kind added later cannot inherit a wrong
    /// default; it has to say which encoding it uses, and saying "the plain map" is one line.
    fn serialize_entries<S>(
        entries: &HashMap<ModelKey, Self::Elements>,
        serializer: S,
    ) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer;

    fn deserialize_entries<'de, D>(
        deserializer: D,
    ) -> Result<HashMap<ModelKey, Self::Elements>, D::Error>
    where
        D: serde::Deserializer<'de>;

    /// Where this kind's map lives on the shard.
    ///
    /// Rust cannot abstract over a struct FIELD, so this is the one thing each kind must supply by
    /// hand -- and supplying it is what lets the operations below be written once for all kinds
    /// instead of once per kind.
    fn resident(shard: &mut ShardState) -> &mut RecordedMap<Self>;

    /// Bytes to carry on the WAL outcome's `value` slot for one install, beside the block address
    /// every kind already carries -- and deliberately NO DEFAULT, for the same reason
    /// `serialize_entries` has none.
    ///
    /// THIS EXISTS BECAUSE A ZSET'S COMPONENT STOPPED SPELLING THE SCORE. Every other kind's level-2
    /// value is fully named by its component and its address -- a hash field, a set member, a list
    /// sequence -- so `None` costs them nothing. A zset's value is `(u64, BlockAddress)`, and the
    /// `u64` had been riding the component as `{biased:016x}` until that string collapsed to the
    /// member alone; this is where it rides instead, on replay's only remaining path to it. A kind
    /// added later that also needs a byte string beside its address has to say so here rather than
    /// inherit silence -- the same defect class `serialize_entries`'s own doc comment describes.
    fn outcome_value(value: &<Self::Elements as ElementMap>::Value) -> Option<Vec<u8>>;
}

/// THIS KIND HAS A RECOVERY PATH THAT INSTALLS AN ELEMENT AND FILES NOTHING.
///
/// A marker, so the exception is data the compiler reads. Only `hashes` declares it, because only
/// its `context_node` replay arm installs without re-filing a block.
pub(super) trait ReplaysInstallsUnrecorded: RecordedKind {}

/// THIS KIND'S LEVEL-2 CONTAINER IS A B-TREE THAT A DECODE LEAVES HALF EMPTY.
///
/// A marker, as above. `hashes` does NOT declare it: a sorted vector has no half-empty nodes.
pub(super) trait RepacksAfterDecode: RecordedKind {}

/// The resident model map of one kind, readable from anywhere in `engine` and writable only through
/// the recorded operations below.
///
/// `entries` is private to this module. That single fact is the invariant: `rustc` refuses a
/// `&mut shard.<kind>.entries` from any other module, so the complete set of operations that can
/// change a model map is the set declared here -- once, for every kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RecordedMap<K: RecordedKind> {
    /// PRIVATE, AND THE WHOLE POINT. Do not add an accessor returning `&mut` to this.
    /// `engine::tests::recorded_map_invariant` fails if the module grows one.
    ///
    /// THE KEY IS A SHARED HANDLE. All four kinds were measured holding their OWN copy of a key
    /// text four other maps had already agreed on -- eight holders of one text reporting FIVE
    /// allocations, at 8 of 8 keys, in
    /// `which_string_keyed_holders_duplicate_a_key_another_map_holds`. Being private is what kept
    /// that true for so long: the key could not be converted from outside this module, so the
    /// adoption had to arrive as a declared operation here. See `adopt_key` below.
    entries: HashMap<ModelKey, K::Elements>,
}

impl<K: RecordedKind> Default for RecordedMap<K> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// THE PROOFS. One set, generic over the kind, each carrying the identity of what was recorded so
// the mutator writes the element the record names and cannot be handed a proof for another.
// ---------------------------------------------------------------------------------------------

/// PROOF THAT THE DURABLE RECORD FOR ONE ELEMENT IS ALREADY OUT.
#[must_use = "a recorded element that is never installed leaves the record describing a page the \
              resident map does not hold"]
pub(super) struct RecordedElement<K: RecordedKind> {
    /// The emitter's witness. Not read -- HOLDING it is the point, because it cannot be obtained
    /// without the record having been filed.
    #[allow(dead_code)]
    filed: BlockFiled,
    object_key: String,
    element: <K::Elements as ElementMap>::Element,
    value: <K::Elements as ElementMap>::Value,
}

/// PROOF THAT THE REMOVAL OF ONE ELEMENT IS ALREADY RECORDED.
///
/// NO WITNESS, and that is stated rather than hidden. Its emitter
/// `execute_on_shard::remove_container_element` is shared with six other arms, so a witness return
/// means changing all seven. Measured, not guessed, and the same is true for every kind -- which is
/// one small argument for this type: the gap is now in ONE place instead of once per kind.
#[must_use = "a recorded element removal that is never applied leaves the element resident after \
              its page was tombstoned"]
pub(super) struct RecordedElementRemoval<K: RecordedKind> {
    object_key: String,
    element: <K::Elements as ElementMap>::Element,
}

/// PROOF THAT THE DELETION OF A WHOLE OBJECT IS ALREADY RECORDED IN THE BUCKET INDEX.
#[must_use = "a recorded object deletion that is never applied leaves the object resident after the \
              index says it is gone"]
pub(super) struct RecordedObjectRemoval<K: RecordedKind> {
    #[allow(dead_code)]
    filed: ObjectDeletionFiled,
    object_key: String,
    kind: std::marker::PhantomData<K>,
}

// ---------------------------------------------------------------------------------------------
// THE OPERATIONS. One call each, written ONCE for every kind: emit the record, then mutate through
// the proof.
//
// FREE FUNCTIONS, NOT METHODS, and that is forced rather than stylistic.
// `upsert_bucket_index_block` takes `&mut ShardState` -- the WHOLE shard -- because filing a block
// reloads a released bucket, interns the component name and touches the pending-flag set. A model
// map is a FIELD of `ShardState`, so a method holding `&mut self` cannot lend the whole shard to
// the emitter, and a closure capturing the shard fails for the same reason. A free function clears
// it because the two borrows are SEQUENTIAL: the emitter's `&mut shard` ends when it returns, and
// only then is the field borrowed to apply the proof.
// ---------------------------------------------------------------------------------------------

/// Record an element's block and put it in the resident map, in that order, in one call.
///
/// `component` is the element rendered the way the page index names it; `element` is the level-2
/// key. BOTH are taken rather than one derived from the other, because every caller has already
/// computed the component to derive the block ordinal and the page frame from it, and deriving it
/// again here would be a second rendering of one identity -- the shape that let a claim about a
/// hash function be relayed as a claim about a hash path.
///
/// # A CORRECTION TO THE VERSION OF THIS NOTE THAT LANDED FIRST
///
/// The paragraph that stood here said reader (1) below is "the only durable spelling of a hash
/// field, a set member, a zset member or a list position that anything READS on the replay
/// path". That was true of the code it was written against and is FALSE now, which is why it is
/// quoted rather than quietly overwritten: `apply_outcome_item`'s four container arms read the
/// element off the PAGE, live and removed, so nothing on the replay path reads the outcome's
/// copy for those kinds any more. What the earlier note got right is the conclusion -- the
/// parameter stays -- and it got it right for a reason that has since changed, which is exactly
/// the shape this tree records rather than deletes.
///
/// # THIS PARAMETER WAS PUT UP FOR REMOVAL TOO, AND IT IS NOT A RECONSTRUCTION OF ANYTHING
///
/// The reading it was proposed on was that it is "the resident map's own key spelling, so removing
/// it removes the authority". IT IS NOT THAT, and the correction is worth having even though the
/// conclusion survives: the map's key is `element`, the parameter beside it, and the two are
/// different VALUES rather than two spellings of one. For `SetKind` the caller passes
/// `Some(hex::encode(member))` here and the raw `member` bytes as `element`; the map is keyed by
/// the bytes.
///
/// WHAT IT FEEDS, AFTER THE REPLAY PATH STOPPED ASKING THE OUTCOME. Both readers are inside
/// `upsert_bucket_index_block_inner`, which splits it into `ElementComponent` and
/// `FiledComponent`:
///
///   1. `WalOutcomeItem::component`. The replay path no longer READS that field for a container
///      kind -- it reads the page -- so this reader is now the record's own copy rather than
///      something a recovery depends on. It is still written, because the field cannot be removed
///      while six timestamped kinds read it and its absence is a meaning; the argument is set out
///      in full at the field.
///   2. THE TOMBSTONE SWEEP, which is the reader that makes this parameter stay. `element.as_deref()`
///      keys `tombstone_element_block`, and that is how a re-add clears the tombstone ITS OWN element
///      left. Narrow on purpose: the predicate it replaced widened to every tombstone of the object
///      the moment the filed name went to `None`, which is the resurrection
///      `engine::tests::write_after_fold::a_re_add_after_a_removal_does_not_bring_the_removed_member_back`
///      holds.
///
/// # AND IT COULD BE RESOLVED FROM THE PAGE, WHICH IS EXACTLY WHY IT SHOULD NOT BE
///
/// The sweep runs on the WRITE path, where the element name is the caller's own input -- already
/// computed, already in hand, one `&str` away. Decoding the page being written to recover a value
/// the caller passed in would be the same rendering done twice, which is what the paragraph above
/// this one exists to prevent. The rule that retires a derivable stored field does not reach a
/// plain argument: nothing here is STORED and nothing is RECONSTRUCTED, so there is no omission to
/// make a promise about.
///
/// AND NO CALLER PASSES `None`. Every arm that reaches here -- hash, set, zset and list, across
/// `execute_on_shard`, `stream_batch_methods` and the two replay paths in `lifecycle` -- passes
/// `Some(..)`, so the `Option` is a shape shared with the filer rather than a case this function
/// has.
pub(super) fn install_element<K: RecordedKind>(
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    component: Option<String>,
    element: <K::Elements as ElementMap>::Element,
    value: <K::Elements as ElementMap>::Value,
    dirty: bool,
    address: ElementEntry,
) where
    K::Elements: AddressedElementMap,
{
    // COMPUTED BEFORE `value` MOVES INTO THE RECORD BELOW. The only producer of a `Some` today is
    // `ZSetKind`, whose score has nowhere else to ride now that the component does not spell it.
    let outcome_value = K::outcome_value(&value);
    // WHICH PAGE THIS WRITE REPLACES, ASKED WHILE THE MAP STILL HOLDS THE OLD ANSWER.
    //
    // The filer cannot derive this. Under one entry a page an entry carries no element name, so
    // the only discriminator it has is the address -- and a rewrite lands at a NEW address, so the
    // landing key names no existing entry and the element's previous entry is never superseded.
    // See `AddressedElementMap` for the two measured losses that leaves.
    //
    // ASKED ONLY WHEN THE ENTRY IS PAGE-NAMED. While an entry names its element the filer's own
    // component-keyed supersede already takes the predecessor, so there is nothing to tell it and
    // nothing to pay for: the ungated path does not reach the scan at all.
    let replaces = if super::storage_bucket_internals::index_entry_names_a_block(K::KIND) {
        K::resident(shard).block_an_element_vacates(object_key, &element, &address)
    } else {
        None
    };
    // THE RECORD FIRST. The proof below cannot be built without the witness this returns, so
    // deleting this call is a compile error rather than a silent unrecorded write.
    let filed = super::storage_bucket_internals::upsert_bucket_index_block_filed(
        shard,
        shard_id,
        K::KIND,
        object_key,
        component,
        address,
        dirty,
        outcome_value,
        super::storage_bucket_internals::ReplacedBlock::of(replaces),
    );
    // THEN THE MUTATION, through the proof.
    K::resident(shard).install(RecordedElement::<K> {
        filed,
        object_key: object_key.to_string(),
        element,
        value,
    });
}

/// The same, for a record that is a STAGED OUTCOME and no bucket-index entry.
///
/// A context node's OUTCOME is deliberately never recorded under kind `hash` -- doing so would have
/// a rebuild add an entry naming a block the outcome never described -- so the record this emitter
/// files is the staged outcome alone. That is a different record, which is why it is a different
/// emitter rather than a boolean on the one above: a boolean would have let an arm pick the wrong
/// record silently.
///
/// CORRECTED: this said the node is "never registered in the bucket index". The BLOCK is registered
/// -- `append_value` files it from the object id, `CONTEXT_NODE_FIELD` and the routing bucket, and
/// `context_node_survives_reload` reads `("hash", Some("meta"), deleted=false)` back out of the
/// index. Only the outcome's KIND differs.
#[allow(clippy::too_many_arguments)]
pub(super) fn install_element_staged_only<K: RecordedKind>(
    shard: &mut ShardState,
    shard_id: ShardId,
    staged_kind: &str,
    object_key: &str,
    component: &str,
    routing_bucket: u32,
    element: <K::Elements as ElementMap>::Element,
    value: <K::Elements as ElementMap>::Value,
    address: ElementEntry,
) {
    let staged = super::block_in_wal::stage_outcome_attested(crate::wal::WalOutcomeItem {
        kind: staged_kind.to_string(),
        object_key: object_key.to_string(),
        component: Some(component.to_string()),
        object_id: super::stable_block_object_id(shard_id, staged_kind, object_key),
        routing_bucket,
        address: Some(address),
        value: None,
        ttl: None,
        deleted: false,
        meta: false,
    });
    K::resident(shard).install(RecordedElement::<K> {
        filed: staged.into_block_filed(),
        object_key: object_key.to_string(),
        element,
        value,
    });
}

/// Tombstone the element's page, clear its bucket-index entry, and drop it from the resident map.
///
/// Returns whether anything changed -- the removal's own answer OR the resident drop's. Both are
/// folded here because every caller OR-ed them into one `mutated` flag, and a removal that found no
/// page still has to reach the resident map: the resident map is exactly where a stale copy would
/// survive.
#[allow(clippy::too_many_arguments)]
pub(super) fn remove_element<K: RecordedKind>(
    cache: &matrixcache::MultiLayerCache,
    block_store: &crate::block_store::BlockStore,
    shard: &mut ShardState,
    shard_id: ShardId,
    object_key: &str,
    component: &str,
    element: <K::Elements as ElementMap>::Element,
    start_routing_bucket: u32,
    end_routing_bucket: u32,
    async_storage: bool,
) -> bool
where
    K::Elements: RemovableElementMap,
{
    let removed = super::execute_on_shard::remove_container_element(
        cache,
        block_store,
        shard,
        shard_id,
        K::KIND,
        object_key,
        component,
        start_routing_bucket,
        end_routing_bucket,
        async_storage,
    );
    let dropped = K::resident(shard).remove_element(RecordedElementRemoval::<K> {
        object_key: object_key.to_string(),
        element,
    });
    // AND WHEN THAT WAS THE LAST ELEMENT, THE INDEX HAS TO LET GO TOO.
    //
    // EXISTENCE HAS TWO SOURCES AND EMPTYING ONE DOES NOT EMPTY THE KEY. `record_exists_exact` ORs
    // the resident map's answer together with the bucket index's, so an object is gone only once
    // BOTH are. The resident removal above is unconditional -- which is why membership is right
    // under either projection -- but under one entry a page the removal deliberately keeps the live
    // page entry, on the stated grounds that the page still holds the object's other members.
    //
    // That justification is false for the LAST element: there are none. The retained entry then
    // keeps the key enumerable with nothing in it, and KEYS, SCAN, DBSIZE, EXISTS, TYPE and EXPIRE
    // all answer for it -- `redis::tests::redis_core_api_extensions_use_engine_and_state` is the
    // arm that caught it, through those surfaces rather than through the index.
    //
    // ASKED OF THE RESIDENT MAP RATHER THAN OF THE INDEX, because the resident map is the authority
    // for existence and the index is the derived view. And asked AFTER the removal above, so it
    // reads the post-removal state rather than predicting it.
    if K::resident(shard).get(object_key).is_none() {
        // THE LIVE ENTRIES ONLY. Not `mark_bucket_index_object_deleted`, which takes the tombstones
        // too: one live entry and one tombstone are different facts. The live entry claims this
        // object has a page with members on it, which is false once the last member goes. The
        // tombstone records that a NAMED element was removed, which stays true and is what makes
        // the removal win a fold by append position -- taking it along would undo the removal it
        // records, and `container_tombstone_entry::a_re_add_clears_the_tombstone_entry_so_churn_on_
        // one_element_does_not_accumulate` is the arm that holds the difference at one live and one
        // tombstone per cycle.
