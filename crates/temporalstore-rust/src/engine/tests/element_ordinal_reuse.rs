// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! AN ELEMENT ORDINAL WOULD HAVE TO BE HANDED OUT BY A COUNTER, AND EVERY PLACE TO KEEP ONE IS
//! REBUILT FROM THE PAGES THAT LOSE IT.
//!
//! # WHAT WAS ASKED
//!
//! `BlockIndex.component` is `Option<Arc<str>>` -- sixteen bytes on every page entry, plus one heap
//! allocation per page for a name this engine RENDERED rather than received. For the container kinds
//! the name is content-derived:
//!
//! ```text
//!     zset    format!("{score_bits:016x}{}", hex::encode(member))
//!     set     hex::encode(member)
//!     list    format!("{:016x}", (seq as u64).wrapping_sub(i64::MIN as u64))
//!     hash    the caller's OWN field name
//!     string  no component at all
//! ```
//!
//! So: replace the name with a small per-element ordinal. #1985 refuted the idea on three grounds and
//! two are gone -- #1989 made the durable map outrank the name, and #1982 enumerates what replay
//! rebuilds. The third was that no per-element ordinal EXISTS for the kinds that would benefit, and
//! this module is that question: **can one be assigned and PERSISTED, with non-reuse ENFORCED rather
//! than coincidental?**
//!
//! It cannot, and it is refused TWICE OVER. The reasons are measured below rather than argued.
//!
//! # THE STOP CONDITION, WHICH IS WORSE THAN THE NON-REUSE ONE AND IS SETTLED FIRST
//!
//! `an_ordinal_loses_the_member_the_fold_delivers_without_a_durable_entry`. The case FOR an ordinal
//! rests on the member already being persisted twice, which the reconcile's own comment supports by
//! calling the name "a second copy of it rendered as text". But the same comment names the exception:
//! the name is "the fallback for a member the durable map does not have, which is how an element
//! folded out of the delta log arrives".
//!
//! `apply_key_states` folds THIRTEEN maps and `sets`, `zsets`, `lists` and `hashes` are **not** among
//! them, while `fold_delta_block_items` DOES restore the page items. So after a fold the index holds
//! pages for elements the durable map never received, and for those the component is the ONLY copy of
//! the member. Driven with #1989's own fixture and the name made ordinal-shaped: the control comes back
//! at 1.0 and the ordinal-named member comes back as `None`. Its bytes survive only in the page
//! PAYLOAD, which the reconcile deliberately does not read.
//!
//! **An ordinal here is a lossy migration, so it was not built.** For it to be safe the FOLD must carry
//! the member bytes -- a change to the delta record's outcome shape, which is #1982's territory.
//!
//! # THE THREE WAYS TO STOP A NUMBER BEING REUSED, AND WHAT EACH ONE HITS
//!
//! ## 1. A TOMBSTONE THAT KEEPS ITS NUMBER IN THE `max`
//!
//! **THIS SECTION HAS BEEN OVERTAKEN, AND WHAT OVERTOOK IT IS NOT THE ORDINAL.** It used to say there
//! was no tombstone to keep a number in, because EVERY delete path removed the page: the per-element
//! four went through `mark_bucket_index_block_deleted`, named for a mark it did not make, whose body
//! was `block_index.retain(.., |_, page| ..false..)`. That was correct, and it is the reason the
//! reservation this section is about could not be built.
//!
//! There IS a per-element tombstone now, and it arrived for an unrelated reason: a container's pages
//! became the statement of its membership, and a page nothing points at is a page no derivation can
//! read. So a removal keeps an entry pointing at the page that records it.
//!
//! **IT STILL DOES NOT RESERVE THE NUMBER, and that is deliberate rather than incidental.** The
//! freed ordinal is still handed to the next element --
//! `the_element_ordinal_a_delete_frees_is_handed_straight_back_to_the_next_element` drives that and
//! passes: three members, the third at ordinal 2, and after removing it the arrival is handed 2.
//!
//! **WHAT KEEPS IT UNRESERVED IS NO LONGER A `deleted` FILTER.** This paragraph said
//! "`container_block_ordinal` filters `deleted`, so the tombstone is invisible to the `max`". That
//! walk is gone. The allocator asks the RESIDENT MAP, which is keyed by element and holds exactly
//! the object's live elements, so its high-water mark is over live elements by construction and no
//! filter is needed -- a removal drops its element from the map and the number falls with it. The
//! two hazards the filter existed for are unrepresentable rather than guarded now, which
//! `state::container_block_ordinal`'s own doc sets out.
//!
//! AND THAT MATTERS MORE THAN A TIDIER MECHANISM, because the retained live ENTRY a removal leaves
//! would have defeated the filter. An entry over a vacated page is not deleted, so a walk over live
//! entries no longer falls after a removal -- which is exactly how this module's own probe broke:
//! `next_element_ordinal_would_be` was `blocks_of(..).len()`, it reported one too many, and two arms
//! read that as the engine having stopped reusing. It asks the allocator now. The hazard the old
//! paragraph warned of -- "the ordinal would climb once per element ever written ... and a container
//! churning distinct members would walk to `MAX_ADDRESSABLE_BLOCK_ID`" -- is real and is what the
//! move to the resident map avoids, rather than something the filter is still holding off.
//!
//! So the tombstone exists and the reuse this module is about is unchanged; what a reader must not
//! conclude is that a tombstone now reserves anything.
//!
//! `every_per_element_delete_leaves_one_tombstone_and_the_whole_object_delete_leaves_none` drives the
//! new split over the same five arms the old claim covered.
//!
//! **THIS WIDENS #1990.** That issue names the whole-object path (`FeatureAppend`, `FeatureDelete`,
//! `FeatureAppend`) and says of a partial removal that it "does not free a number, because for the
//! container kinds no per-element number is assigned at all". True today, and it is true only because
//! nothing is assigned: the page it would have been assigned to is removed by the same `retain`. The
//! per-element path is the one an element ordinal would live on, and it is a removal too. It follows
//! that #1976's published mechanism -- "the absence of a `!block.deleted` filter is what reserves the
//! ordinal" -- is wrong on BOTH paths and not just the one #1990 corrected; `page.deleted` is written
//! by the upsert and read by five filters, and no delete command in this engine ever sets it.
//!
//! ## 2. THE ONE TOMBSTONE THIS STORE DOES KEEP, AND WHY IT IS STILL NOT A RESERVATION
//!
//! `BucketNode::deleted_object_index` is a real, persisted tombstone -- "THE TOMBSTONE SIDE OF A
//! BUCKET". It holds `stable_block_object_id(shard, kind, key)`, so under an ordinal it
//! would be keyed BY THE ORDINAL, which looks exactly like the reservation wanted.
//!
//! **THE FIRST DRAFT OF THIS MODULE REFUTED IT ON THE WRONG GROUND AND THE TEST WENT RED.** The claim
//! was that the reusing write clears it, reasoning off
//! `deleted_object_index.remove(&object_id)` sitting four statements before the page insert. That
//! call was in `sync_bucket_index_object_blocks_with_mode` -- the WHOLE-OBJECT restate path -- and
//! NOT in `upsert_bucket_index_block_inner`, the path every container ELEMENT write takes, which
//! did `object_index.insert(object_id)` and never touched the tombstone. Measured then: after
//! `ZSetAdd`, `ZSetRemove`, `ZSetAdd` of the same member the tombstone still held that page's
//! object id, and a LIVE page's object reported `deleted` through the public report. **That
//! asymmetry was a reporting defect and is fixed**; the element path clears the id it files a live
//! page for, and the test below holds the fixed arithmetic.
//!
//! The reuse idea is refuted on three grounds that the fix does not touch, all in
//! `the_element_rewrite_clears_the_tombstone_so_a_live_block_reads_as_hot`:
//!
//!   1. **IT IS A MEMBERSHIP SET, NOT A MARK.** It holds a HASH of the component. It answers "was
//!      ordinal N used?" and cannot answer "what is the highest?", so an assignment on it probes
//!      one candidate at a time and cannot reach a ceiling without 65,535 probes.
//!   2. **CLEARING IS WHAT KEEPS THE REPORT HONEST, AND A RESERVATION CANNOT ALLOW IT.** Both
//!      write paths now drop the id: the element one for the page it files, and a whole-object
//!      restate for every address it republishes -- so an unrelated whole-object write discards
//!      every element's reservation at once, and the element path discards its own on re-add.
//!   3. **A RESERVATION IS NEVER CLEARED, AND THAT INVERTS ITS OWN MEASURED ECONOMICS.** The field's
//!      doc chose its shape on a census -- 97.68% of buckets carry no tombstone, the widest carries a
//!      single id -- and named the turnover: "the shape wins while fewer than about a quarter of
//!      buckets carry one and loses above that, against a measured 2.32%". Kept for ever it is one id
//!      per removed element in every bucket that has ever had a removal.
//!
//! **AND THE STALE TOMBSTONE IS A DEFECT IN ITS OWN RIGHT, FOUND IN PASSING.**
//! `object_manager::runtime_report` reads `page.deleted || bucket.deleted() ||
//! bucket.deleted_object_index.contains(&page.object_id())`, so a LIVE page whose member was once
//! removed and re-added reports its object `deleted` and is counted as a deleted block ref instead of
//! a hot one. Reachable, not dead: `TemporalEngine::object_manager_runtime_report` is public and
//! `recovery_sweep_compact` calls it twice. Driven through that public surface. FOUND, NOT FIXED --
//! the missing `deleted_object_index.remove` belongs on the element write path, which is a change to
//! the write path and not to a test module.
//!
//! ## 3. A PERSISTED COUNTER -- WHICH IS THE ONE THIS STORE ALREADY REJECTED, TWICE
//!
//! Both rejections are in the tree and both are about this.
//!
//! `next_block_index_for_object`, which is the existing page ordinal, states the design:
//!
//! > Read from the blocks the object already has rather than from a counter, so it survives a restart
//! > without anything having to be persisted for it.
//!
//! And `block_index_handle` records what the counter COST:
//!
//! > Derived rather than assigned, because handles are written to disk inside the lookup's refs. Two
//! > processes holding the same page must compute the same handle or those refs point at nothing --
//! > which is what a counter did, silently, until a reload lost an object.
//!
//! **THE SECOND QUOTE IS NOT THE ONE THAT REFUTES THIS, AND SAYING SO IS THE POINT.** A handle is
//! assigned and never stored, so it must be recomputable; an element ordinal would be a STORED field
//! of the page, and a stored field does not have to be recomputed on load. Read carelessly the quote
//! closes the question, and it does not. What closes it is the third clause of the same sentence
//! applied to the ordinal's own home: **a reload lost an object because the number and the thing it
//! named were recovered from different places.**
//!
//! # SO WHERE COULD A HIGH-WATER MARK LIVE? THERE IS EXACTLY ONE CANDIDATE, AND THE REBUILD TAKES IT
//!
//! `a_high_water_mark_lives_only_where_the_rebuild_recomputes_it` drives a reload and enumerates the
//! three structures:
//!
//! ```text
//!     CoreIndex.bucket_map            SERIALIZED      the pages -- max over them is the derivation
//!                                                     #1990 shows dropping back
//!     CoreIndex.object_block_lookup   skip_serializing rebuilt from the bucket map on load
//!     ShardState.sets/zsets/lists     SERIALIZED      the durable maps #1989 made authoritative
//!     ShardState.hashes               skip_serializing rebuilt from the bucket map on load
//! ```
//!
//! So the durable maps are the candidate, and they genuinely survive a reload -- asserted, not
//! assumed. But the store's recovery contract is that **a stale index is treated exactly like an
//! absent one and rebuilt from the log**, and the rebuild door is
//! `reconcile_secondary_views_from_bucket_index`, which derives those same maps FROM THE PAGES and
//! merges. A mark held in the durable map is therefore recomputed as a `max` over the elements the
//! pages still hold -- which is the derivation the delete drops back. Driven below: the mark is 3
//! before the delete, 3 after it in memory, and 2 after the reload.
//!
//! **A counter that the rebuild can recompute from the data is not a counter. A counter the rebuild
//! cannot recompute cannot survive the rebuild this store requires.** That is the refutation, and it
//! is the same failure the `block_index_handle` comment names, reached from the other end.
//!
//! # AND THE ANSWER IS NOT ONE ANSWER -- IT IS FOUR, ONE PER KIND
//!
//! `what_an_element_ordinal_would_mean_for_each_kind` asserts the split in both directions, the way
//! #1982 asserted its two:
//!
//! ```text
//!     list    AN ORDINAL ALREADY EXISTS AND IS ALREADY PERSISTED. The component IS
//!             `format!("{:016x}", biased_seq)` and `shard.lists` is keyed by the `i64` itself. The
//!             only thing left to change is the SPELLING -- sixteen characters for eight bytes -- and
//!             that is not an ordinal proposal, it is #1976's fixed-width one with a narrower field.
//!     hash    REFUTED OUTRIGHT AND NOT ON PERFORMANCE. The component is the CALLER'S field name.
//!             An ordinal does not respell it, it destroys it -- and `hash` is the only kind the point
//!             read path reaches at all, twice, with that name as the argument.
//!     zset    THE ONLY TWO AN ORDINAL WOULD HELP, and the only two where it has to be ASSIGNED.
//!     set     Both already hold the member in a persisted map keyed BY the member, so the ordinal
//!             would be a third copy of an identity that already has two.
//! ```
//!
//! # THE READ PATH, COUNTED HERE RATHER THAN INHERITED
//!
//! #1985 established three call sites. Confirmed independently and at source level by
//! `the_point_read_path_is_three_call_sites_and_not_one_of_them_is_a_container`, with the input size
//! asserted before any count is believed: `bucket_index_block_address` has ONE non-doc caller in the
//! crate -- `read_bucket_index_value` -- and that has THREE, all in `execute_on_shard.rs`:
//! `StringGet` with `None`, `HashGet` with `Some(field)`, `HashIncrBy` with `Some(field)`. **No
//! container kind is reached by the point lookup at all.**
//!
//! `bucket_index_component_block_addresses` -- the reader #1986 found refutes ideas its neighbours
//! survive -- has three callers, `hash` twice and `set` once, and its signature is the whole of why:
//! `(shard, model_id, object_key)`. It takes no component and its caller has none to give, so an
//! ordinal answers it exactly as well as a name does. **It is not the obstacle here**, and that is
//! worth writing down because it was the obstacle twice before.
//!
//! # THE ORDERING PROPERTY, WHICH IS THE OBVIOUS REFUTATION AND IS NOT ONE
//!
//! A zset component is score-bits-then-member and a list component is an `i64::MIN`-biased sequence,
//! both so that lexical order IS the element's order; the write arm says the bias is "what lets
//! recovery and range reads walk the bucket index directly". An ordinal that did not preserve that
//! would force those readers to sort, and the sort could easily cost more than the bytes.
//!
//! **No reader on this revision takes its order from the component.**
//! `the_component_ordering_property_is_consumed_by_the_set_listing` classifies every consumer: the three
//! reconcile arms decode each component and insert into a `BTreeMap` keyed by the MEMBER BYTES or the
//! `i64` SEQUENCE, so order is re-established by the map rather than inherited from the walk; range
//! reads use `shard.zsets` (keyed by member, "derived per query -- V1 accepts the per-range sort") and
//! `shard.lists` (keyed by the `i64`); and the three readers that do sort by component need only a
//! TOTAL order, which an ordinal has. Of the three callers of
//! `bucket_index_component_block_addresses`, `SetMembers` discards the component, `HashLen` reads
//! `len()`, and `HashGetAll` keeps a field name an ordinal never touches.
//!
//! So the sixteen characters of score prefix on every zset element buy nothing that is consumed. That
//! is a point IN FAVOUR of the ordinal, recorded because a refutation must not collect support it has
//! not earned.
//!
//! # THE MIS-PARSE RATE, MEASURED
//!
//! #1976 assumed 0% and measured 1.935%. #1985 measured 100% for lists. An ordinal against a
//! hexadecimal name is the widest of the three, and
//! `the_mis_parse_rate_of_an_ordinal_against_the_names_this_store_already_holds` measures it per kind
//! over 20,000 names with the denominator printed:
//!
//! ```text
//!     list   20000 of 20000 well-formed ordinals of a DIFFERENT meaning   100.000%
//!     set     5007 of 20000                                               25.035%
//!     zset      20 of 20000                                                0.100%
//! ```
//!
//! **THE SET ROW IS THE ONE NOBODY PREDICTED: 25.035%.** A set component is `hex::encode(member)` and
//! nothing else, so every EIGHT-BYTE member spells exactly sixteen hexadecimal characters -- an
//! ordinary member length, not a corner. #1976's 1.935% was measured against a different spelling; a
//! sixteen-character ordinal against set names is thirteen times worse. The list row is the one that decides the stored
//! shape, and it is 100.00% by construction rather than by luck: a list component is SIXTEEN
//! hexadecimal characters and so is a sixteen-character ordinal, and `"0000000000000000"` is
//! simultaneously sequence `i64::MIN` and ordinal 0.
//!
//! # WHAT IT WOULD HAVE BEEN WORTH, AND IT IS NOT SIXTEEN BYTES -- THE MEMBER IS STORED TWICE
//!
//! The sixteen-byte pointer is the smaller half. `what_a_component_costs_an_element_at_five_member_widths`
//! measures the component against the member it names, per kind, at five widths, request and chunk,
//! as a HISTOGRAM with the store path held at 15 characters:
//!
//! ```text
//!     kind      n   pages  comp chars   REQUEST B    CHUNK B  chars/n
//!     set       8     100          16       48.00      68.64     2.00
//!     set      16     100          32       64.00      80.80     2.00
//!     set      32     100          64       96.00     114.40     2.00
//!     set      64     100         128      160.00     177.12     2.00
//!     set     256     100         512      544.00     560.16     2.00
//!     zset      8     100          32       64.00      80.80     4.00
//!     zset     16     100          48       80.00      96.16     3.00
//!     zset     32     100          80      112.00     130.56     2.50
//!     zset     64     100         144      176.00     192.16     2.25
//!     zset    256     100         528      560.00     576.32     2.06
//!     list      8     100          16       48.00      64.96     2.00   <- CONTROL, flat
//!     list    256     100          16       48.00      65.76     0.06   <- CONTROL, flat
//!     string  any     100        none        0.00       0.00        -   <- CONTROL, no component
//! ```
//!
//! **`chars/n` IS EXACTLY 2.00 FOR `set` AT EVERY WIDTH.** `hex::encode` spells the member at two
//! characters a byte, so a set element holds its member ONCE as the page payload and AGAIN, at double
//! width, as the name of the page. A zset adds a fixed sixteen characters of score bits on top -- and
//! that score is a third copy, because `zset_index_serde` persists it as a `u64` in the durable map.
//!
//! So at a sixteen-byte member a zset element pays **96.16 B of chunk for the name plus the 16-byte
//! fat pointer in the entry, to name sixteen bytes it already holds twice elsewhere.** At 256 bytes it
//! pays 576.32 + 16. That is far more than the sixteen-byte pointer this work was scoped around, and
//! it is why the refutation below is worth stating precisely rather than briefly.
//!
//! **TWO CONTROLS, BOTH AT THE PREDICTED ZERO EFFECT.** `list`'s component is sixteen characters
//! whatever the member is, and it reads 48.00 B a name at n=8 and 48.00 at n=256 -- flat across a 32x
//! change, asserted as an equality. `string` has no component and reads 0.00 at every width. A kind
//! that moved in either control would mean the measurement was picking up something other than the
//! member's spelling.
//!
//! **AND THE DOUBLING CAN BE ATTACKED WITHOUT AN ORDINAL, WHICH IS THE ACTIONABLE HALF OF THIS.** The
//! hex is there because the component is `Arc<str>` and a member is arbitrary bytes. A component that
//! held BYTES rather than characters halves it -- 2n back to n -- and needs no assignment, no
//! high-water mark and no non-reuse enforcement, because the name is still derived from the member.
//! That is a different change from this one and it is not refuted by anything below.
//!
//! The pointer half is measured separately by `what_an_element_ordinal_would_have_saved_a_block`, at
//! both routing ranges and two corpus sizes: 58.06-58.13 B requested and 69.20-69.32 B of chunk a
//! name over a container store whose mean name is 22.4 characters, against 0.00 on the routed control
//! where no page carries a component at all. Two populations and never a mean: routed p50 1 page an
//! object, containers p50 100.
//!
//! # THE WIDTH AND THE CEILING, IF IT EVER LANDS
//!
//! `the_width_an_element_ordinal_would_take_and_the_ceiling_that_implies` reads the widths from the
//! fields themselves and states the ceiling. The number that matters is not ours to choose freely:
//! #1994 narrowed `BlockAddress::block_id` to SIXTEEN BITS, so an ordinal folded into the existing
//! per-object ordinal caps at **65,535 elements an object** -- which is exactly the ceiling the
//! design that prompted this work has, against a measured container shape of p50 100 and one observed
//! case at 2,000. Thirty-two times the headroom, not sixty-five thousand.

#![allow(clippy::all)]
use super::*;

const OPERATOR_END: u32 = 1023;
const WHOLE_KEYSPACE_END: u32 = u32::MAX;

// =================================================================================================
// HARNESS
// =================================================================================================

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine, end_routing_bucket: u32) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "element-ordinal".to_string(),
        shard_uri: "local://element-ordinal/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 over 0..={end_routing_bucket}: {:?}",
        response.status
    );
}

fn write(engine: &TemporalEngine, command: Command) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "the fixture write failed: {response:?}");
}

/// Every page this object holds, as `(component, block_id)`, whatever its state.
///
/// Reads the SAME walk `next_block_index_for_object` reads -- every page of the object in the
/// bucket, with no filter on `deleted` -- so a count taken here is the count the assignment would
/// take.
/// THE OBJECT'S LIVE PAGES. Tombstone entries are excluded, and the exclusion is new.
///
/// WHAT THIS USED TO RETURN AND WHY THE CHANGE IS NOT A RELAXATION. It returned EVERY entry, with no
/// `deleted` filter -- and that was the same set, because nothing in this engine ever set
/// `BlockIndex::deleted` to true. Live entries and all entries were one quantity, so the distinction
/// could not be expressed and did not need to be. A container removal now leaves a tombstone entry
/// behind so the page recording the removal stays reachable, and the two quantities have come apart.
///
/// Every assertion in this module that uses this helper means the LIVE set: it is asking what the
/// object holds and what ordinal the next element would take. So the filter restores each of those
/// claims to the quantity it was always about, rather than weakening any of them.
/// [`tombstoned_blocks_of`] is the other half, and the arms below assert BOTH.
fn blocks_of(engine: &TemporalEngine, kind: &str, key: &str) -> Vec<(Option<String>, Option<u64>)> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut held = Vec::new();
    for (_routing_bucket, _handle, object_key, block) in
        super::blocks_with_identity(&shard.bucket_index)
    {
        if block.deleted {
            continue;
        }
        if block.model_id.as_str() == kind && &*object_key == key {
            held.push((None::<String>, block.address.block_id()));
        }
    }
    held.sort();
    held
}

/// The ordinals the object's TOMBSTONE entries carry, ascending.
///
/// Needed because a freed ordinal is now held by two entries for a while -- the tombstone that kept
/// it and the element that was handed it back -- and a guard about reuse has to be able to say which
/// is which rather than counting two and calling it corruption.
fn tombstone_ordinals_of(engine: &TemporalEngine, kind: &str, key: &str) -> Vec<Option<u64>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    // TOMBSTONES ARE THE SUBJECT HERE, so the object has to come from the door that names a
    // deleted page -- the bucket's own removal row. The object lookup holds no deleted page at
    // all, and a walk that asked it would come back empty and read as "no tombstone kept an
    // ordinal", which is the claim this exists to refute.
    let mut held = Vec::new();
    for (_routing_bucket, _handle, object_key, block) in
        super::blocks_with_identity(&shard.bucket_index)
    {
        if block.deleted && block.model_id.as_str() == kind && &*object_key == key {
            held.push(block.address.block_id());
        }
    }
    held.sort();
    held
}

/// Pages of this object that carry `deleted == true`.
fn tombstoned_blocks_of(engine: &TemporalEngine, kind: &str, key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    let mut count = 0usize;
    for (_routing_bucket, _handle, object_key, block) in
        super::blocks_with_identity(&shard.bucket_index)
    {
        if block.model_id.as_str() == kind && &*object_key == key && block.deleted {
            count += 1;
        }
    }
    count
}

/// WHAT THE ENGINE'S OWN ALLOCATOR WOULD HAND A NEW ELEMENT OF THIS OBJECT.
///
/// THIS WAS `blocks_of(engine, kind, key).len()` AND THAT PROXY HAS BROKEN. Counting the object's
/// LIVE page entries stood in for the ordinal high-water mark for as long as the two moved
/// together, and they no longer do: a container removal leaves the live entry over the page its
/// element vacated STANDING, beside a tombstone, because the page may still hold the object's other
/// members. So the live count does not fall after a removal and the proxy reported one too many --
/// `the_element_ordinal_a_delete_frees_is_handed_straight_back_to_the_next_element` read the
/// arrival's ordinal as 3 against the removed member's 2, and
/// `a_high_water_mark_lives_only_where_the_rebuild_recomputes_it` read 8 where 7 was due. Both were
/// the helper, which is what that arm's own message said to check first: "The helper above counts
/// LIVE pages; a red caused by the tombstone being counted is a helper that lost its `deleted`
/// filter, not a reservation." The filter is intact; what changed is that a retained entry is not
/// deleted.
///
/// SO IT ASKS `container_block_ordinal`, WHICH IS THE ALLOCATOR ITSELF rather than anything standing
/// in for it. There is no second implementation to drift: the number this returns is the number the
/// next write would be handed. The allocator reads the resident map -- keyed by element, holding
/// exactly the object's live elements -- so the high-water mark it reports is over live elements by
/// construction and a retained index entry is invisible to it.
///
/// THE PROBE COMPONENT IS ASSERTED ABSENT, because the allocator has two arms and this helper is
/// only ever asking about one. An element that already holds a block gets its own ordinal back; a
/// new one gets `high-water + 1`. A probe that accidentally named a held element would measure the
/// overwrite arm and report that element's position as though it were the next free one.
fn next_element_ordinal_would_be(engine: &TemporalEngine, kind: &str, key: &str) -> usize {
    let probe = hex::encode(b"-no-element-of-any-fixture-object-holds-this-");
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    assert!(
        crate::engine::state::resident_component_address(shard, kind, key, &probe).is_none(),
        "the probe component IS held by an element of {kind}/{key}, so the allocator would answer \
         its overwrite arm and this helper would be reporting that element's position as the next \
         free ordinal"
    );
    crate::engine::state::container_block_ordinal(shard, kind, key, &probe) as usize
}

/// The ordinal one named element of this object actually holds, from the resident map.
///
/// The authority: `container_block_ordinal`'s overwrite arm reads exactly this, so an element's
/// position and what the allocator would hand it back are one number read one way.
fn ordinal_of_element(
    engine: &TemporalEngine,
    kind: &str,
    key: &str,
    component: &str,
) -> Option<u64> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    crate::engine::state::resident_component_address(shard, kind, key, component)
        .and_then(crate::block_store::ElementEntry::block_id)
}

/// Whether any bucket's persisted tombstone holds this object id.
fn tombstone_holds(engine: &TemporalEngine, object_id: u64) -> bool {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    shard
        .bucket_index
        .bucket_map
        .values()
        .any(|bucket| bucket.deleted_object_index.contains(&object_id))
}

/// How many elements the durable map holds for this key, per kind.
fn durable_element_count(engine: &TemporalEngine, kind: &str, key: &str) -> usize {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard 1 is loaded");
    match kind {
        "zset" => shard.zsets.get(key).map_or(0, |members| members.len()),
        "set" => shard.sets.get(key).map_or(0, |members| members.len()),
        "list" => shard.lists.get(key).map_or(0, |entries| entries.len()),
        "hash" => shard.hashes.get(key).map_or(0, |fields| fields.len()),
        other => panic!("no durable map is claimed for kind {other}"),
    }
}

fn zset_name(member: &[u8]) -> String {
    crate::engine::execute_on_shard::zset_component(member)
}

// =================================================================================================
// 1. A PER-ELEMENT DELETE NOW LEAVES A TOMBSTONE; A WHOLE-OBJECT DELETE STILL DOES NOT
// =================================================================================================

/// EVERY PER-ELEMENT DELETE PATH LEAVES EXACTLY ONE TOMBSTONE. THE WHOLE-OBJECT PATH LEAVES NONE.
///
/// # WHAT THIS ASSERTED BEFORE, AND WHY IT IS THE OPPOSITE NOW
///
/// It was called `FORMERLY(every_delete_path_removes_the_page_rather_than_tombstoning_it)` -- wrapped
/// so that neither a reader nor the citation check mistakes a name that no longer exists for a guard
/// that does, which is the same trap the unwritten-guard marker was for -- and it asserted
/// `tombstoned == 0` on all five arms. That was true and load-bearing: the per-element four went
/// through `mark_bucket_index_block_deleted`, whose name states a mark it does not make, and both its
/// body and the whole-object deleter's were a `retain` returning false. Nothing in this engine ever
/// set `BlockIndex::deleted` to true.
///
/// A container's PAGES are now the statement of its membership, which is what the fold this module
/// sits under exists to make possible, and a page nothing points at is a page no derivation can read.
/// So a per-element removal appends a page that records it and keeps an entry pointing at that page,
/// carrying `deleted`. The old claim is therefore FALSE BY INTENT for four of the five arms.
///
/// IT IS INVERTED HERE RATHER THAN RELAXED OR DELETED. Relaxing it -- dropping the `tombstoned == 0`
/// line -- would leave a module that no longer says anything about tombstones at all, and a guard
/// that silently swaps sides is indistinguishable from one that was wrong all along. So the count is
/// still asserted on every arm; what changed is the number it is asserted against, and that number is
/// now PER ARM rather than one constant, because the two kinds of delete differ.
///
/// # THE WHOLE-OBJECT ARM IS THE CONTROL, NOT AN EXEMPTION
///
/// `CommonDelete` drops every entry of the key, tombstones included, because there is no membership
/// left to keep true -- the object is gone. If that arm started leaving tombstones it would be
/// leaking an entry per deleted object forever, so its zero is asserted as firmly as the others' one.
///
/// THE DENOMINATOR IS PRINTED AND ASSERTED for every arm, because a delete of nothing and a delete
/// that tombstones produce the same page counts.
///
/// rust-internal: drives five commands, no external surface
#[test]
fn every_per_element_delete_leaves_one_tombstone_and_the_whole_object_delete_leaves_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    // (kind, key, seed, delete, how many elements the seed makes)
    let member = b"member-00".to_vec();
    let seeds: Vec<(&str, &str, Command, Command)> = vec![
        (
            "zset",
            "eo-zset",
            Command::ZSetAdd {
                key: "eo-zset".to_string(),
                member: member.clone(),
                score: 1.5,
            },
            Command::ZSetRemove {
                key: "eo-zset".to_string(),
                member: member.clone(),
            },
        ),
        (
            "set",
            "eo-set",
            Command::SetAdd {
                key: "eo-set".to_string(),
                member: member.clone(),
            },
            Command::SetRemove {
                key: "eo-set".to_string(),
                member: member.clone(),
            },
        ),
        (
            "list",
            "eo-list",
            Command::ListPush {
                key: "eo-list".to_string(),
                member: member.clone(),
                left: false,
            },
            Command::ListPop {
                key: "eo-list".to_string(),
                left: false,
            },
        ),
        (
            "hash",
            "eo-hash",
            Command::HashSet {
                key: "eo-hash".to_string(),
                field: "f0".to_string(),
                value: member.clone(),
            },
            Command::HashDelete {
                key: "eo-hash".to_string(),
                field: "f0".to_string(),
            },
        ),
        (
            "zset",
            "eo-zset-whole",
            Command::ZSetAdd {
                key: "eo-zset-whole".to_string(),
                member: member.clone(),
                score: 1.5,
            },
            Command::CommonDelete {
                key: "eo-zset-whole".to_string(),
            },
        ),
    ];

    let mut arms_checked = 0usize;
    let mut per_element_arms = 0usize;
    let mut whole_object_arms = 0usize;
    for (kind, key, seed, delete) in seeds {
        // WHICH KIND OF DELETE THIS ARM IS, decided from the command rather than from the key's name.
        // Reading it off a hand-written list of which arms are whole-object would go stale the moment
        // a sixth arm was added, and nothing would fail.
        let whole_object = matches!(delete, Command::CommonDelete { .. });
        write(&engine, seed);
        let before = blocks_of(&engine, kind, key);
        assert!(
            !before.is_empty(),
            "DENOMINATOR: {kind}/{key} holds no page after its seed, so the delete below would \
             prove nothing"
        );
        assert_eq!(
            0,
            tombstoned_blocks_of(&engine, kind, key),
            "DENOMINATOR: {kind}/{key} already carries a tombstone before its delete, so the count \
             after it would not be attributable to the delete"
        );

        write(&engine, delete);
        let after = blocks_of(&engine, kind, key);
        let tombstoned = tombstoned_blocks_of(&engine, kind, key);

        println!(
            "  {kind:<5} {key:<14} {:<12} live pages {} -> {}, entries carrying deleted=true: {}",
            if whole_object { "whole-object" } else { "per-element" },
            before.len(),
            after.len(),
            tombstoned
        );
        // RESTATED FOR THE COLLAPSED PROJECTION, AS THE INVARIANT AND NOT AS A COUNT. A gated
        // per-element removal does not reduce the LIVE page count: the page it removes from still
        // holds the object's other elements, so the entry stays and a tombstone naming the removed
        // element is added beside it. What must fall is the live count OR -- under the collapse --
        // the tombstone count must rise, and the element must be gone either way.
        //
        // The removal's correctness under that arm is established by
        // `write_after_fold::a_gated_removal_leaves_every_other_member_whole_across_a_reload` and
        // `..::gated_removals_file_one_tombstone_per_distinct_element_and_do_not_accumulate`,
        // rather than by this count agreeing with whatever the code now does.
        // RESTATED FROM `kind == "set"`, WHICH WAS THE WHOLE COLLAPSED SET WHEN THIS WAS WRITTEN.
        //
        // `zset` and `list` are collapsed too now, so a hand-written kind here would leave their
        // per-element arms in the `else` branch below asserting that the LIVE page count FALLS --
        // which is exactly what a gated removal does not do, and the arm would have gone red for
        // the right reason with the wrong explanation. Asking the shared predicate means the kind
        // list lives in ONE place: a kind added to `index_entry_names_a_block` arrives here with
        // it, and a kind held OUT of it (hash, today) keeps the ungated expectation.
        let collapsed = !whole_object
            && crate::engine::storage_bucket_internals::index_entry_names_a_block(kind);
        if collapsed {
            assert!(
                tombstoned > 0,
                "{kind}/{key}: the gated per-element delete left {} live pages of {} and filed \
                 {tombstoned} tombstones, so it recorded the removal nowhere",
                after.len(),
                before.len()
            );
        } else {
            assert!(
                after.len() < before.len(),
                "{kind}/{key}: the delete left {} live pages of {}, so it did not remove",
                after.len(),
                before.len()
            );
        }
        if whole_object {
            whole_object_arms += 1;
            assert_eq!(
                0, tombstoned,
                "{kind}/{key}: a WHOLE-OBJECT delete left {tombstoned} tombstone entry(ies). There \
                 is no membership left to keep true once the object is gone, so an entry retained \
                 here is leaked for the life of the store rather than collected by a rewrite."
            );
        } else {
            per_element_arms += 1;
            assert_eq!(
                1, tombstoned,
                "{kind}/{key}: a PER-ELEMENT delete left {tombstoned} tombstone entry(ies), not one. \
                 Zero means the removal reached the index and NOT the pages, so a membership derived \
                 from the pages would put the element back -- which is the defect #2028 drove and \
                 this whole change exists to close. More than one means the removal filed a tombstone \
                 per bucket rather than per element."
            );
        }
        arms_checked += 1;
    }

    assert_eq!(
        arms_checked, 5,
        "DENOMINATOR: {arms_checked} delete paths were driven, not the five this claim covers"
    );
    // BOTH SIDES OF THE SPLIT ARE NON-EMPTY. Without this, five per-element arms and no whole-object
    // arm would satisfy every assertion above and the control would be untested.
    assert_eq!(
        4, per_element_arms,
        "DENOMINATOR: {per_element_arms} per-element arms were driven, not four"
    );
    assert_eq!(
        1, whole_object_arms,
        "DENOMINATOR: {whole_object_arms} whole-object arms were driven, not one"
    );
    println!(
        "\n  four per-element deletes left one tombstone each; the whole-object delete left none"
    );
}

// =================================================================================================
// 2. THE ORDINAL A DELETE FREES IS HANDED STRAIGHT BACK
// =================================================================================================

/// THE TWO ORDINARY COMMANDS #1990 NAMES, DRIVEN ON THE ELEMENT AXIS INSTEAD OF THE OBJECT AXIS.
///
/// #1990 drove `FeatureAppend`, `FeatureDelete`, `FeatureAppend` -- the whole-object path. The path an
/// element ordinal would live on is `ZSetAdd`, `ZSetRemove`, `ZSetAdd`, and it is the same story: the
/// removal takes the page away, the walk's `max` drops back, and the next element is handed the
/// number the removed one had.
///
/// **THIS IS THE TEST THAT WOULD FAIL IF NON-REUSE WERE ENFORCED**, and it is written the way round
/// that makes that true: it asserts the number IS reused, and names in its message what an
/// enforcement would have to make it say instead. An enforcement that persisted the high-water mark
/// turns the final assertion red, which is the signal wanted.
///
/// The two members are asserted DIFFERENT in the same breath, because a reused number is only
/// corruption when two distinct elements land on it.
///
/// rust-internal: drives three commands, no external surface
#[test]
fn the_element_ordinal_a_delete_frees_is_handed_straight_back_to_the_next_element() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let held: Vec<Vec<u8>> = (0..3)
        .map(|element| format!("held-{element:02}").into_bytes())
        .collect();
    for (element, member) in held.iter().enumerate() {
        write(
            &engine,
            Command::ZSetAdd {
                key: "eo-reuse".to_string(),
                member: member.clone(),
                score: element as f64,
            },
        );
    }

    let seeded = blocks_of(&engine, "zset", "eo-reuse");
    assert_eq!(
        seeded.len(),
        held.len(),
        "DENOMINATOR: {} pages for {} members seeded",
        seeded.len(),
        held.len()
    );
    let ordinal_the_third_member_took = next_element_ordinal_would_be(&engine, "zset", "eo-reuse") - 1;

    // Remove the member holding the HIGHEST ordinal, which is what makes `max` drop.
    let doomed = held.last().expect("three members were seeded").clone();
    write(
        &engine,
        Command::ZSetRemove {
            key: "eo-reuse".to_string(),
            member: doomed.clone(),
        },
    );
    let after_delete = next_element_ordinal_would_be(&engine, "zset", "eo-reuse");

    // A DIFFERENT member, so a shared number is two elements and not one rewritten.
    let arrival = b"arrival-99".to_vec();
    assert_ne!(
        doomed, arrival,
        "the fixture must use two DISTINCT members or a shared ordinal is not corruption"
    );
    write(
        &engine,
        Command::ZSetAdd {
            key: "eo-reuse".to_string(),
            member: arrival.clone(),
            score: 99.0,
        },
    );
    let ordinal_the_arrival_took = next_element_ordinal_would_be(&engine, "zset", "eo-reuse") - 1;

    println!(
        "\n  [reuse, element axis] seeded {} members; the third took ordinal {}; after removing it \
         the next would be {}; the arrival took ordinal {}",
        held.len(),
        ordinal_the_third_member_took,
        after_delete,
        ordinal_the_arrival_took
    );

    assert_eq!(
        ordinal_the_arrival_took, ordinal_the_third_member_took,
        "the arrival took ordinal {ordinal_the_arrival_took} and the removed member had \
         {ordinal_the_third_member_took}. IF THIS IS RED, non-reuse is now enforced somewhere and \
         this module's refutation should be re-read rather than this assertion relaxed. The helper \
         above counts LIVE pages; a red caused by the tombstone being counted is a helper that lost \
         its `deleted` filter, not a reservation."
    );

    // THE FREED NUMBER IS NOW HELD TWICE, AND EXACTLY ONE OF THE TWO IS LIVE.
    //
    // A NEW FACT, not a relaxation of the one above: the removal keeps an entry pointing at the page
    // that records it, and `container_block_ordinal` filters `deleted`, so the tombstone keeps the
    // number it had while the arrival is handed the same number. That is safe for exactly the reason
    // this module has always given -- an ordinal names a POSITION and identity lives in the component
    // -- and it is asserted here rather than left implicit, because "two entries share ordinal 2" is
    // what corruption would also look like.
    let tombstones = tombstone_ordinals_of(&engine, "zset", "eo-reuse");
    println!(
        "  [reuse, element axis] tombstone entries hold {tombstones:?}; the arrival holds \
         Some({ordinal_the_arrival_took})"
    );
    assert_eq!(
        1,
        tombstones.len(),
        "DENOMINATOR: {} tombstone entries for one removal, so the pairing below is not about one \
         removed element",
        tombstones.len()
    );
    assert_eq!(
        Some(ordinal_the_third_member_took as u64),
        tombstones[0],
        "the tombstone does not hold the ordinal its element had, so the number it kept is not the \
         one the arrival was handed back and this pairing is measuring two unrelated things"
    );
}

// =================================================================================================
// 3. THE ONE TOMBSTONE THIS STORE KEEPS CANNOT REFUSE THE WRITE
// =================================================================================================

/// THE TOMBSTONE SURVIVES THE REWRITE, AND A LIVE PAGE THEN READS AS DELETED.
///
/// **THIS TEST WAS WRITTEN TO ASSERT THE OPPOSITE AND THE TREE REFUSED IT.** The first draft claimed
/// the reservation is cleared by the write that would reuse it, reasoning from
/// `deleted_object_index.remove(&object_id)` sitting four statements before the page insert. That
/// call is real, and it is in `sync_bucket_index_object_blocks_with_mode` -- the WHOLE-OBJECT restate
/// path that `FeatureAppend` uses. It is **not** in `upsert_bucket_index_block_inner`, which is the
/// path every container ELEMENT write takes. So on the element path the tombstone only ever grows.
///
/// Measured: after `ZSetAdd`, `ZSetRemove`, `ZSetAdd` of the SAME member the bucket holds a live page
/// AND a tombstone for that page's own object id.
///
/// # SO THE TOMBSTONE IS NOT REFUTED BY BEING CLEARED. IT IS REFUTED THREE OTHER WAYS
///
///   1. **IT IS A MEMBERSHIP SET, NOT A MARK.** It holds `stable_block_object_id(shard, kind,
///      key)` -- a HASH. It can answer "has ordinal N been used?" and it cannot answer "what is
///      the highest ordinal used?", so an assignment built on it probes 0, 1, 2, ... one hash at a
///      time and cannot report a ceiling without 65,535 probes.
///   2. **BOTH WRITE PATHS CLEAR IT, AND THE WHOLE-OBJECT ONE CLEARS EVERY ELEMENT'S.** A restate
///      through `sync_bucket_index_object_blocks_with_mode` removes the id of every address it
///      republishes, so any reservation an element held is dropped by an unrelated whole-object
///      write; and `upsert_bucket_index_block_inner` now clears the id it is filing a live page
///      for, which is what keeps the report honest but is also a clear a reservation cannot allow.
///   3. **A RESERVATION MUST NEVER BE CLEARED, AND THAT INVERTS ITS OWN MEASURED ECONOMICS.** The
///      field's doc chose its shape on a census -- "97.68% of buckets carry no tombstone, and the
///      widest bucket that carries one carries a single id" -- and states where the trade turns over:
///      "the shape wins while fewer than about a quarter of buckets carry one and loses above that,
///      against a measured 2.32%". A tombstone kept for ever puts every bucket that has ever had an
///      element removed on the losing side, carrying one id per removed element rather than one.
///
/// # AND THE DEFECT THAT CAME WITH IT, NOW FIXED
///
/// `object_manager::runtime_report` computes `page.deleted || bucket.deleted() ||
/// bucket.deleted_object_index.contains(&page.object_id())`. While the element write path left the
/// id behind, a surviving tombstone made a LIVE page's object report `deleted` and counted its page
/// as a deleted block ref rather than a hot one -- reaching the public report as
/// `tombstone_object_count=1` on a store holding one live page. Reachable:
/// `TemporalEngine::object_manager_runtime_report` is public and `recovery_sweep_compact` calls it
/// twice.
///
/// The clear now happens on both write paths, and this test holds the fixed arithmetic: the
/// tombstone is gone after the rewrite, the page reads HOT, and the public aggregate reports zero.
/// The four assertions below were written to hold the defect and are inverted, not relaxed -- each
/// one still names the denominator it reads, so a fixture that writes nothing cannot pass it.
///
/// rust-internal: drives a removal and a re-add, no external surface
#[test]
fn the_element_rewrite_clears_the_tombstone_so_a_live_block_reads_as_hot() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let member = b"tombstone-member".to_vec();
    let component = zset_name(&member);
    let object_id = crate::engine::hashing::stable_block_object_id(
        1,
        "zset",
        "eo-tombstone",
    );

    write(
        &engine,
        Command::ZSetAdd {
            key: "eo-tombstone".to_string(),
            member: member.clone(),
            score: 4.25,
        },
    );
    assert_eq!(
        blocks_of(&engine, "zset", "eo-tombstone").len(),
        1,
        "DENOMINATOR: the seed did not file a page, so nothing below is being observed"
    );
    let held_before_delete = tombstone_holds(&engine, object_id);

    write(
        &engine,
        Command::ZSetRemove {
            key: "eo-tombstone".to_string(),
            member: member.clone(),
        },
    );
    let held_after_delete = tombstone_holds(&engine, object_id);

    write(
        &engine,
        Command::ZSetAdd {
            key: "eo-tombstone".to_string(),
            member: member.clone(),
            score: 4.25,
        },
    );
    let held_after_rewrite = tombstone_holds(&engine, object_id);
    let blocks_after_rewrite = blocks_of(&engine, "zset", "eo-tombstone").len();

    println!(
        "\n  [tombstone] object id {object_id}: held before delete={held_before_delete}, after \
         delete={held_after_delete}, after the next write={held_after_rewrite}; pages after that \
         write={blocks_after_rewrite}"
    );

    assert!(
        !held_before_delete,
        "the tombstone already held this id before anything was deleted, so the observation below \
         says nothing about the delete"
    );
    assert!(
        held_after_delete,
        "the delete left no tombstone for {object_id}, so there was never a reservation to clear"
    );
    assert!(
        !held_after_rewrite,
        "the tombstone SURVIVED the next write of the same triple. The element write path clears \
         `deleted_object_index` for the id it is filing a live page for; if this is red that \
         `remove` has been lost from `upsert_bucket_index_block_inner` and the stale tombstone is \
         back."
    );
    assert_eq!(
        blocks_after_rewrite, 1,
        "the rewrite must have filed its page, or the stale tombstone below is not sitting beside a \
         LIVE page"
    );

    // The consequence. Read TWICE: the per-object row through the internal walk (the same door
    // `address_footprint` reads it through), and the aggregate through the PUBLIC surface a recovery
    // sweep actually calls. The row carries the claim; the aggregate proves it is not confined to an
    // internal structure nobody serves from.
    let (row_deleted, row_block_refs, row_deleted_refs, row_hot) = {
        let shards = engine.shards.read().expect("engine lock poisoned");
        let shard = shards.get(&1).expect("shard 1 is loaded");
        let internal = crate::engine::object_manager::runtime_report(shard);
        let rows: Vec<_> = internal
            .objects
            .iter()
            .filter(|row| row.object_id == object_id)
            .collect();
        assert_eq!(
            rows.len(),
            1,
            "DENOMINATOR: the walk holds {} rows for object id {object_id}, not the one this \
             observation reads",
            rows.len()
        );
        let row = rows[0];
        (
            row.deleted,
            row.block_ref_count,
            row.deleted_block_ref_count,
            row.hot_block_ref_count,
        )
    };

    let public = engine.object_manager_runtime_report(1);
    println!(
        "  [object manager] object {object_id}: block_ref_count={row_block_refs} \
         deleted={row_deleted} deleted_block_ref_count={row_deleted_refs} hot={row_hot}\n  \
         [object manager] public aggregate: object_count={} block_ref_count={} \
         tombstone_object_count={}",
        public.object_count, public.block_ref_count, public.delete_marker_object_count
    );

    assert_eq!(
        row_block_refs, 1,
        "DENOMINATOR: the walk counts {row_block_refs} block refs for a live page, not one"
    );
    assert!(
        !row_deleted,
        "the walk still calls a LIVE page's object deleted, so a stale tombstone is still sitting \
         beside it"
    );
    assert_eq!(
        row_deleted_refs, 0,
        "the walk counts {row_deleted_refs} of this object's block refs as deleted; the page is \
         live and no tombstone should be reclassifying it"
    );
    assert_eq!(
        row_hot, 1,
        "the live page is counted as HOT now that no tombstone reclassifies it; {row_hot} says \
         the arithmetic landed somewhere else"
    );
    assert!(
        public.block_ref_count >= 1,
        "DENOMINATOR: the public report counts {} block refs, so it is not seeing the store at all",
        public.block_ref_count
    );
    assert_eq!(
        public.delete_marker_object_count, 0,
        "the PUBLIC report counts {} delete-marked objects on a store whose one page is LIVE. \
         This is the surface a recovery sweep reads, and the count is what the fix on the element \
         write path exists to make honest",
        public.delete_marker_object_count
    );
}

// =================================================================================================
// 4. WHERE A HIGH-WATER MARK COULD LIVE, AND WHAT THE RELOAD DOES TO IT
// =================================================================================================

/// THE DURABLE MAP SURVIVES A RELOAD. A MARK IN IT DOES NOT SURVIVE THE REBUILD.
///
/// Two separate claims, and the second is the refutation:
///
///   1. `ShardState::zsets` is serialized, so a per-object high-water mark kept beside the elements
///      WOULD come back from a reload. Driven: seven elements written, engine dropped, engine
///      reopened, seven elements still there.
///   2. But the mark's value is the thing at issue, and the reload's door is
///      `reconcile_secondary_views_from_bucket_index`, which derives the same maps FROM the pages and
///      merges. So after a removal the reloaded mark is a `max` over what the pages still hold.
///      Driven: three elements, remove the highest, the in-memory walk still reads 2 live pages and
///      the reload reads 2 -- the number the removed element had is free again, across the restart.
///
/// **ELEMENT BY ELEMENT, NOT BY COUNT**, because a count can balance while two elements swap.
///
/// rust-internal: drives a reload, no external surface
#[test]
fn a_high_water_mark_lives_only_where_the_rebuild_recomputes_it() {
    let dir = tempfile::tempdir().expect("tempdir");

    let survivors: Vec<Vec<u8>> = (0..7)
        .map(|element| format!("survivor-{element:02}").into_bytes())
        .collect();
    let doomed = b"doomed-highest".to_vec();

    {
        let engine = engine_on(dir.path());
        load_on(&engine, OPERATOR_END);
        for (element, member) in survivors.iter().enumerate() {
            write(
                &engine,
                Command::ZSetAdd {
                    key: "eo-reload".to_string(),
                    member: member.clone(),
                    score: element as f64,
                },
            );
        }
        write(
            &engine,
            Command::ZSetAdd {
                key: "eo-reload".to_string(),
                member: doomed.clone(),
                score: 999.0,
            },
        );

        let seeded_blocks = blocks_of(&engine, "zset", "eo-reload").len();
        let seeded_durable = durable_element_count(&engine, "zset", "eo-reload");
        assert_eq!(
            seeded_blocks,
            survivors.len() + 1,
            "DENOMINATOR: {seeded_blocks} pages for {} elements seeded",
            survivors.len() + 1
        );
        assert_eq!(
            seeded_durable,
            survivors.len() + 1,
            "DENOMINATOR: the durable map holds {seeded_durable} of {} seeded elements",
            survivors.len() + 1
        );

        write(
            &engine,
            Command::ZSetRemove {
                key: "eo-reload".to_string(),
                member: doomed.clone(),
            },
        );
        let after_delete = next_element_ordinal_would_be(&engine, "zset", "eo-reload");
        println!(
            "\n  [reload] {} elements seeded, highest removed; in memory the next ordinal would be \
             {after_delete}",
            survivors.len() + 1
        );
        assert_eq!(
            after_delete,
            survivors.len(),
            "in memory the walk should already have dropped back to {}",
            survivors.len()
        );

        engine.unload_shard(1);
    }

    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    // CLAIM 1: the durable map came back.
    let reloaded_durable = durable_element_count(&engine, "zset", "eo-reload");
    assert_eq!(
        reloaded_durable,
        survivors.len(),
        "the durable map came back with {reloaded_durable} of {} elements, so it is not a place a \
         mark could live at all",
        survivors.len()
    );

    // ELEMENT BY ELEMENT: every survivor is back, by identity and by score, and the removed one is
    // not. A count alone would pass if two elements had swapped.
    let mut recovered = 0usize;
    for (element, member) in survivors.iter().enumerate() {
        let answered = engine.execute(ExecuteRequest {
            shard_id: 1,
            command: Command::ZSetScore {
                key: "eo-reload".to_string(),
                member: member.clone(),
            },
        });
        let bytes = match answered.response {
            crate::types::CommandResponse::Bytes { value: Some(bytes) } => bytes,
            other => panic!(
                "survivor {element} ({:?}) did not come back after the reload: {other:?}",
                String::from_utf8_lossy(member)
            ),
        };
        let score: f64 = String::from_utf8_lossy(&bytes)
            .parse()
            .expect("a score parses");
        assert!(
            (score - element as f64).abs() < 1e-9,
            "survivor {element} came back at score {score}, not {element}"
        );
        recovered += 1;
    }
    assert_eq!(
        recovered,
        survivors.len(),
        "DENOMINATOR: {recovered} of {} survivors checked",
        survivors.len()
    );
    let doomed_answer = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::ZSetScore {
            key: "eo-reload".to_string(),
            member: doomed.clone(),
        },
    });
    assert!(
        matches!(
            doomed_answer.response,
            crate::types::CommandResponse::Bytes { value: None }
        ),
        "the removed element came back from the reload: {doomed_answer:?}"
    );

    // CLAIM 2: and the mark the rebuild recomputes has dropped back.
    let reloaded_next = next_element_ordinal_would_be(&engine, "zset", "eo-reload");
    println!(
        "  [reload] after the restart the durable map holds {reloaded_durable} elements and the \
         next ordinal the rebuild would derive is {reloaded_next}; the removed element's number \
         was {}",
        survivors.len()
    );
    assert_eq!(
        reloaded_next,
        survivors.len(),
        "the rebuilt walk answers {reloaded_next}, and the removed element's ordinal was {}. IF \
         THIS IS RED a mark now survives the rebuild, and that is the one thing this refutation \
         says cannot be had.",
        survivors.len()
    );
}

// =================================================================================================
// 5. THE ANSWER IS FOUR ANSWERS, ONE PER KIND
// =================================================================================================

/// WHAT AN ORDINAL WOULD MEAN FOR EACH CONTAINER KIND, ASSERTED IN BOTH DIRECTIONS.
///
/// The shape #1982 used: an enumeration is only a finding if the arms that do NOT need the change are
/// asserted not to need it, in the same test as the ones that do.
///
///   * `list` -- the component IS already an ordinal. Asserted by spelling one and matching it.
///   * `hash` -- the component IS the caller's field name. Asserted by writing a field and finding
///     that exact text on the page.
///   * `zset`, `set` -- the component is the member's CONTENT, and the member is already held in a
///     persisted map keyed by itself. Asserted both ways: the name contains the member's hex, and the
///     durable map holds the member.
///
/// rust-internal: reads page entries, no external surface
#[test]
fn what_an_element_ordinal_would_mean_for_each_kind() {
    // IT ASSERTS THAT NO PAGE CARRIES AN ABSENT ELEMENT NAME -- the absence of exactly what this
    // gate creates. Pinned rather than restated because its subject is the per-element ordinal,
    // and the ordinal under the collapsed projection is `ordinal_under_the_gate`'s subject, which
    // drives both arms.
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine, OPERATOR_END);

    let member = b"kind-member".to_vec();
    let member_hex = hex::encode(&member);

    write(
        &engine,
        Command::ListPush {
            key: "eo-k-list".to_string(),
            member: member.clone(),
            left: false,
        },
    );
    write(
        &engine,
        Command::HashSet {
            key: "eo-k-hash".to_string(),
            field: "a-callers-own-field".to_string(),
            value: member.clone(),
        },
    );
    write(
        &engine,
        Command::SetAdd {
            key: "eo-k-set".to_string(),
            member: member.clone(),
        },
    );
    write(
        &engine,
        Command::ZSetAdd {
            key: "eo-k-zset".to_string(),
            member: member.clone(),
            score: 2.5,
        },
    );

    // ONE ELEMENT'S COMPONENT SPELLING, READ FROM THE PAGE THAT SPELLS IT.
    //
    // RE-ATTRIBUTED. This read `blocks_of(...)[0].0` -- the component off the index entry -- and
    // `unwrap_or_else(|| panic!("{kind}/{key} holds a page with no component"))` is exactly what it
    // did, because an entry has no element-name field to hold one. The four spellings this arm is
    // about did NOT go anywhere: a container page states its own elements in its payload, which is
    // what `container_blocks::ElementKeySpelling` renders and what makes several pages foldable into
    // one. So the component comes off the page, through the engine's own
    // `container_membership::derive_membership`, which is the same door `SetMembers` answers from.
    //
    // THE DERIVATION IS FLOORED BOTH WAYS before its key is read: complete, and holding exactly one
    // live element. An incomplete derivation that happened to yield one key would yield it by luck,
    // and a derivation holding two would make `keys().next()` an arbitrary choice dressed as a
    // measurement.
    let one_component = |kind: &str, key: &str| -> String {
        let (addresses, live_entries) = {
            let shards = engine.shards.read().expect("engine lock poisoned");
            let shard = shards.get(&1).expect("shard 1 is loaded");
            let mut addresses: Vec<crate::block_store::ElementEntry> = Vec::new();
            let mut live = 0usize;
            {
                for (_routing_bucket, _handle, object_key, block) in
                    super::blocks_with_identity(&shard.bucket_index)
                {
                    if block.model_id.as_str() == kind && &*object_key == key {
                        if !block.deleted {
                            live += 1;
                        }
                        // TOMBSTONES INCLUDED, so the fold sees every statement about the object
                        // and a removal cannot read as an absence.
                        addresses.push(block.address.clone());
                    }
                }
            }
            (addresses, live)
        };
        assert_eq!(
            live_entries, 1,
            "DENOMINATOR: {kind}/{key} holds {live_entries} live pages, not the one this arm reads"
        );
        let derived = crate::engine::container_membership::derive_membership(
            kind,
            addresses,
            |address| engine.block_store.read(address).ok(),
        );
        assert!(
            derived.is_complete(),
            "the derivation for {kind}/{key} is incomplete ({} failure(s)): {} read, {} \
             undecodable, {} unframed, {} unrenderable -- its single key would be luck",
            derived.failures(),
            derived.read_failures,
            derived.undecodable,
            derived.unframed,
            derived.unrenderable_items
        );
        assert_eq!(
            derived.live.len(),
            1,
            "{kind}/{key} derives {} live element(s) from its pages, not the one this arm reads",
            derived.live.len()
        );
        derived
            .live
            .keys()
            .next()
            .expect("exactly one live element was just asserted")
            .clone()
    };

    // list: ALREADY an ordinal. Sixteen hexadecimal characters, and the value is the biased
    // sequence the durable map is keyed by.
    let list_component = one_component("list", "eo-k-list");
    assert_eq!(
        list_component.len(),
        16,
        "a list component is {} characters, not the sixteen an ordinal spelling would need",
        list_component.len()
    );
    assert_eq!(
        list_component,
        format!("{:016x}", (0i64 as u64).wrapping_sub(i64::MIN as u64)),
        "a list component is not the biased sequence this arm claims it is"
    );
    assert!(
        !list_component.contains(&member_hex),
        "a list component holds the member's bytes, so it is not a pure ordinal after all"
    );

    // hash: the CALLER'S text. An ordinal would not respell this, it would destroy it.
    let hash_component = one_component("hash", "eo-k-hash");
    assert_eq!(
        hash_component, "a-callers-own-field",
        "a hash component is not the caller's field name; it reads {hash_component}"
    );

    // set and zset: the member's content, twice over -- once in the name, once in a persisted map.
    //
    // RESTATED FOR ZSET: it used to be the member's hex with a sixteen-character score prefix
    // ahead of it, so this arm checked `ends_with` and a `16 +` length. The score left the
    // component, so a zset's is now EQUAL to a set's -- both are exactly `hex::encode(member)`,
    // nothing ahead of it and nothing after.
    let set_component = one_component("set", "eo-k-set");
    assert_eq!(
        set_component, member_hex,
        "a set component is not the member's hex"
    );
    let zset_component = one_component("zset", "eo-k-zset");
    assert_eq!(
        zset_component, member_hex,
        "a zset component is not exactly the member's hex any more: {zset_component}"
    );
    assert_eq!(
        zset_component.len(),
        member_hex.len(),
        "a zset component is {} characters, not the {} member characters alone -- the score \
         should have left this string entirely",
        zset_component.len(),
        member_hex.len()
    );

    assert_eq!(
        durable_element_count(&engine, "set", "eo-k-set"),
        1,
        "the set's durable map does not hold the member the name re-spells"
    );
    assert_eq!(
        durable_element_count(&engine, "zset", "eo-k-zset"),
        1,
        "the zset's durable map does not hold the member the name re-spells"
    );

    println!(
        "\n  list  {list_component}  ALREADY an ordinal ({} chars)\n  hash  {hash_component}  the \
         CALLER's field name -- an ordinal destroys it\n  set   {set_component}  the member's hex, \
         and the durable map holds the member too\n  zset  {zset_component}  the member's hex alone \
         now, and the durable map holds the member AND the score",
        list_component.len()
    );
}

// =================================================================================================
// 6. THE READ PATH, COUNTED AT SOURCE LEVEL
// =================================================================================================

/// THE POINT READ PATH IS ONE CALL SITE AND IT IS A STRING.
///
/// RESTATED, AND THE CLAIM GOT STRONGER RATHER THAN WEAKER. This was
/// `the_point_read_path_is_three_call_sites_and_not_one_of_them_is_a_container`, pinned at three
/// sites with kinds string/hash/hash: #1985 established it and the mandate for that work said to
/// confirm it from the source text rather than inherit it. Two of the three were the hash point
/// reads, and they MOVED -- taking the element name off the entry required `HashGet` and
/// `HashIncrBy` to answer from `shard.hashes`, because a nameless entry made a present field
/// unreachable through the index and `HashIncrBy` read that miss as `unwrap_or_default()` and
/// restarted the counter at zero.
///
/// So the count is one and the kind is `string`. The old name's second half -- "not one of them is
/// a container" -- is still the point of the arm, and it is now carried by a positive pin on the
/// one kind that remains rather than by the absence of a container among three.
///
/// THE COUNT IS NOT RE-GOLDENED SILENTLY. Three became one because two readers moved, and the kind
/// list is asserted rather than only the number, so a container reader arriving here changes
/// `kinds` even if it replaces one of these rather than adding to them.
///
/// **THE INPUT SIZE IS ASSERTED BEFORE ANY COUNT IS BELIEVED.** A zero-byte read scores every "there
/// are exactly N" as a pass; this campaign has already had a gate report a clean scan over an empty
/// diff.
///
/// rust-internal: reads the crate's own source, no external surface
#[test]
fn the_point_read_path_is_one_call_site_and_it_is_a_string() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/engine");
    let execute = std::fs::read_to_string(root.join("execute_on_shard.rs"))
        .expect("execute_on_shard.rs is readable");
    let store =
        std::fs::read_to_string(root.join("bucket_store.rs")).expect("bucket_store.rs is readable");
    assert!(
        execute.len() > 100_000 && store.len() > 5_000,
        "DENOMINATOR: read {} and {} bytes of source; a short read scores every count below as a \
         pass",
        execute.len(),
        store.len()
    );

    // The point lookup itself has ONE caller in the crate, and it is the read path. Unmoved.
    let definition_and_callers = store.matches("bucket_index_block_address(").count();
    assert_eq!(
        definition_and_callers, 2,
        "bucket_index_block_address appears {definition_and_callers} times in bucket_store.rs \
         (its definition plus its callers), not the two this claim rests on"
    );

    // ... and that one is called ONCE, from execute_on_shard.
    let read_sites = execute.matches("read_bucket_index_value(").count();
    assert_eq!(
        read_sites, 1,
        "read_bucket_index_value has {read_sites} call sites in execute_on_shard.rs, not one. It \
         had three -- string/hash/hash -- until the two hash point reads moved to `shard.hashes`; \
         a count above one here means a reader came back to the index and the kind list below says \
         which"
    );

    // Its kind argument. A container kind here would move the whole question.
    let mut kinds: Vec<&str> = Vec::new();
    for (offset, _) in execute.match_indices("read_bucket_index_value(") {
        let window = &execute[offset..(offset + 400).min(execute.len())];
        let kind = ["\"string\"", "\"hash\"", "\"set\"", "\"zset\"", "\"list\""]
            .into_iter()
            .find(|kind| window.contains(kind))
            .unwrap_or_else(|| panic!("no kind literal within 400 bytes of a read call site"));
        kinds.push(kind);
    }
    assert_eq!(
        kinds,
        vec!["\"string\""],
