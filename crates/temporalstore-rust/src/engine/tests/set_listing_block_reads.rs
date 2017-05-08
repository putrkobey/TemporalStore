// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A SET LISTING PAYS TO RECOVER BYTES IT IS ALREADY HOLDING.
//!
//! # THE CLAIM THIS MODULE WAS OPENED ON
//!
//! A set member's bytes appear in three places at once, and nobody had measured it:
//!
//! ```text
//!     sets: HashMap<String, BTreeMap<Vec<u8>, BlockAddress>>   the member IS the map key
//!     component = hex::encode(&member)                          the member again, two chars a byte
//!     append_value_of_object(.., &member, ..)                   the member again, as the payload
//! ```
//!
//! All three are real and all three are asserted below rather than read off the write arm. A
//! fourth site was NOT in the claim and is: `set_index_serde` persists the map as
//! `(member bytes, address)` pairs, so for a set the member reaches durable storage a second time,
//! in the index snapshot, beside the page that already holds it.
//!
//! # THE MEASUREMENT
//!
//! `Command::SetMembers` walks `bucket_index_component_block_addresses(shard, "set", &key)` and
//! reads ONE PAGE PER MEMBER to build its answer. The pairs it walks carry the component, and a
//! set's component IS `hex::encode(&member)` -- so every member the response returns was already
//! present, spelled in hex, in the tuple being iterated. `a_set_listing_reads_one_page_per_member`
//! counts the reads; `every_member_a_set_listing_returns_is_already_spelled_by_its_component`
//! proves the redundancy at byte level over a seeded shard rather than inferring it from the call
//! site.
//!
//! COUNTS, NOT TIMES. A timing ratio on this box reads 485x idle against 11x busy off identical
//! code, so the instrument is `maintenance_block_read_counts`, the counter inside the one place a
//! page read past the cache is noted (`note_block_read`).
//!
//! THE CONTROL IS THE ZSET, and it is a control that can fail. A zset's listing answers from
//! `shard.zsets` and reads no page at all, so it must report 0.00 reads per member WHILE STILL
//! RETURNING MEMBERS -- the returned bytes are asserted non-zero, because a listing over an empty
//! key reads zero pages too and would pass a weaker assertion without exercising anything.
//!
//! # WHAT THE PAGE READ BUYS, WHICH IS THE PART THAT DECIDES IT
//!
//! Two cheaper sources were put to the fixture and both are refused, for different reasons.
//!
//! ## Decoding the component instead of reading the page
//!
//! Byte-identical today -- that is the finding of
//! `every_member_a_set_listing_returns_is_already_spelled_by_its_component`. What it would cost is
//! not reads but the only validation a component has.
//!
//! THE SECOND HALF OF THAT SENTENCE USED TO READ "and nothing re-checks them; today a page that
//! disagrees with its name is served as the page", and `container_blocks` made it false. A container
//! page now states which element it holds, and the read funnel asks the page for the element the
//! index named -- so a page that disagrees with its name is NOT served, it answers missing. The
//! component and the payload are still written together by one call site, and the check is now on the
//! element KEY rather than on the value, but a check exists where none did.
//!
//! What does not change is the verdict: decoding the component instead of reading the page would
//! trade an answer storage vouches for against one nothing does, and it would give up the element
//! check as well as the byte one. It is measured here and not taken.
//!
//! ## Serving from `shard.sets`
//!
//! The members ARE the keys of that map, resident and ordered, and walking them would read no page.
//! `the_resident_set_map_and_the_live_block_index_are_not_the_same_population` drove why that was
//! refused: `reconcile_secondary_views_from_bucket_index` installs
//! `shard.sets = fill_absent_elements(derived, persisted)`, whose documented job is to KEEP EVERY
//! DURABLE ELEMENT THE DERIVED VIEW COULD NOT PRODUCE, and it kept them without asking whether the
//! page each named was still there. The map held members the page index did not, and a listing served
//! from it would have returned them. `durable_outranks_derived`'s
//! `a_carried_element_whose_block_the_fold_did_not_keep_is_not_restored` records the same hazard from
//! the fold side and states the consequence in as many words: "It would then be served after being
//! deleted."
//!
//! THAT OBJECTION IS ANSWERED and this module's conclusion survives it on other grounds. The merge
//! asks the live-page question of the persisted map now, so the map cannot over-report. What still
//! stops a listing is the opposite direction: a live page whose component cannot be decoded is in the
//! index and in no map, so a map-served listing would MISS a member -- #1989's case, latent because
//! nothing writes a non-hex set component. `resident_map_readers` carries that argument and the
//! per-reader sweep. The 1.00 reads per member measured below are therefore UNBLOCKED rather than
//! recovered, and are a serving-path change with its own gate.
//!
//! # SO NO PRODUCTION CHANGE SHIPS FROM THIS MODULE
//!
//! The page read is load-bearing: it is the answer the storage vouches for, and both cheaper
//! sources are cheaper precisely because they skip that. The three copies are real, the redundancy
//! is real, and the reads are the price of the one answer that is checked against a page. This
//! module is the measurement and the written reason, and it asserts the facts so a later change
//! that breaks one of them fails the build.
//!
//! THE STORE PATH LENGTH is held equal across every arm and asserted equal: it moves allocation
//! bytes at about six bytes a character, and an arm on a longer temporary directory would read as a
//! heavier representation. It does not move the page-read COUNTS this module is quoted on, which is
//! why those are the headline and the bytes are not.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeSet;

/// Member widths the ladder is measured at, in bytes.
const WIDTHS: [usize; 5] = [8, 16, 32, 64, 256];

/// Members per container key. Two populations, so a per-member figure that is really a fixed cost
/// divided by the count shows up as a different number at each.
const SMALL_POPULATION: usize = 8;
const LARGE_POPULATION: usize = 64;

fn engine_on(dir: &std::path::Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    )
}

fn load_on(engine: &TemporalEngine) {
    let response = engine.load_shard_with(crate::control::LoadShardRequest {
        shard_id: 1,
        table_name: "set-listing-reads".to_string(),
        shard_uri: "local://set-listing-reads/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket: 1023,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1: {:?}",
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

fn read(engine: &TemporalEngine, command: Command) -> crate::types::CommandResponse {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command,
    });
    assert!(response.status.ok, "a read failed: {response:?}");
    response.response
}

/// A member of exactly `width` bytes, distinct per index, and never all-equal: a fixture whose
/// members collide would file fewer pages than it wrote and every per-member figure below would be
/// divided by the wrong denominator.
fn member_of(width: usize, index: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; width];
    let stamp = format!("{index:08}");
    for (slot, byte) in stamp.as_bytes().iter().enumerate() {
        if slot < width {
            bytes[slot] = *byte;
        }
    }
    // Fill the tail with something index-dependent so two members of the same width differ beyond
    // the stamp as well.
    for slot in stamp.len()..width {
        bytes[slot] = (index as u8).wrapping_mul(31).wrapping_add(slot as u8);
    }
    bytes
}

fn members_returned(response: &crate::types::CommandResponse) -> Vec<Vec<u8>> {
    match response {
        crate::types::CommandResponse::Members { members } => members.clone(),
        other => panic!("expected a Members response, got {other:?}"),
    }
}

/// Page reads past the cache performed by `work`, on this thread.

/// Every member the LIVE PAGES of one set key still state as present, decoded out of the element
/// key each page item is filed under.
///
/// THIS IS THE SOURCE THAT REPLACED THE COMPONENT, and its limit is stated here because one arm in
/// this module turns on it: the payload is where a member's bytes live now, but it is NOT an
/// authority on element LIVENESS. A removal writes a tombstone page and leaves the element's
/// original page live and still stating the element, so this OVER-REPORTS after a removal. For
/// "which members does this key hold" the serving path is the authority; for "are the bytes still
/// there" this is.
fn payload_members(engine: &TemporalEngine, object_key: &str) -> BTreeSet<Vec<u8>> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut stated = BTreeSet::new();
    for bucket in shard.bucket_index.bucket_map.values() {
        for (handle, block) in &bucket.block_index {
            if block.model_id.as_str() != "set"
                || &**named.at(bucket.routing_bucket, *handle) != object_key
                || block.deleted
            {
                continue;
            }
            let Ok(bytes) = engine.block_store.read(&block.address) else {
                continue;
            };
            if let crate::engine::container_blocks::ContainerBlockDecode::Framed {
                spelling,
                items,
                ..
            } = crate::engine::container_blocks::decode_container_block(&bytes)
            {
                for item in items {
                    if item.deleted {
                        continue;
                    }
                    if let Some(component) =
                        crate::engine::container_blocks::component_from_element_key(
                            spelling, &item.key,
                        )
                    {
                        if let Ok(member) = hex::decode(&component) {
                            stated.insert(member);
                        }
                    }
                }
            }
        }
    }
    stated
}

fn block_reads_of<T>(work: impl FnOnce() -> T) -> (T, u64) {
    crate::engine::reset_maintenance_block_read_counts();
    let value = work();
    let counts = crate::engine::maintenance_block_read_counts();
    (value, counts.block_reads_total)
}

// =================================================================================================
// 1. THE COUNT
// =================================================================================================

/// ONE PAGE READ PER MEMBER FOR A SET LISTING, AND NONE FOR A ZSET'S.
///
/// The headline. `SetMembers` walks the page index and reads a page per entry; `ZSetRange` answers
/// out of `shard.zsets` and reads nothing. Both arms return members, and both return-populations
/// are asserted non-zero, so the zero row is a measured zero and not an unexercised one.
///
/// THE LISTING MUST BE COLD. `SetMembers` is wrapped in `cached_response`, which stores the whole
/// answer under `CacheKey::set_members`, so a second listing of the same key reads nothing at all.
/// Each key is listed exactly once here and
/// `a_second_set_listing_reads_no_blocks_at_all_because_the_answer_is_cached` states the other half
/// explicitly, so this module cannot be read as claiming the cost is paid per call.
///
/// rust-internal: drives the engine's own command surface
#[test]
fn a_set_listing_reads_one_block_per_member_and_a_zset_listing_reads_none() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path_len = dir.path().as_os_str().len();
    let engine = engine_on(dir.path());
    load_on(&engine);

    println!("\n=== page reads per member, cold listing ===");
    println!("  store path {path_len} characters (equal for every arm in this test)");
    println!(
        "  {:>6}  {:>6}  {:>9}  {:>11}  {:>9}  {:>11}  {:>10}",
        "width", "members", "set reads", "set per mem", "zset rds", "zset per m", "bytes back"
    );

    let mut set_rows: Vec<(usize, usize, u64, f64)> = Vec::new();
    let mut zset_total_reads: u64 = 0;
    let mut zset_total_members: usize = 0;

    for population in [SMALL_POPULATION, LARGE_POPULATION] {
        for width in WIDTHS {
            let set_key = format!("s-{width}-{population}");
            let zset_key = format!("z-{width}-{population}");
            for index in 0..population {
                let member = member_of(width, index);
                write(
                    &engine,
                    Command::SetAdd {
                        key: set_key.clone(),
                        member: member.clone(),
                    },
                );
                write(
                    &engine,
                    Command::ZSetAdd {
                        key: zset_key.clone(),
                        member,
                        score: index as f64,
                    },
                );
            }

            let (set_response, set_reads) = block_reads_of(|| {
                read(
                    &engine,
                    Command::SetMembers {
                        key: set_key.clone(),
                    },
                )
            });
            let set_members = members_returned(&set_response);

            let (zset_response, zset_reads) = block_reads_of(|| {
                read(
                    &engine,
                    Command::ZSetRange {
                        key: zset_key.clone(),
                        start: 0,
                        stop: -1,
                        rev: false,
                    },
                )
            });
            let zset_returned = members_returned(&zset_response);

            // THE POPULATION THE FIXTURE CLAIMS, ASSERTED. A listing that answered short would
            // divide the reads by a denominator the store never held.
            assert_eq!(
                population,
                set_members.len(),
                "the set listing for {set_key} returned {} members, not the {population} written -- \
                 every per-member figure in this row would be divided by the wrong denominator",
                set_members.len()
            );
            // A zset listing is INTERLEAVED member/score, so the element count is half the length.
            assert_eq!(
                0,
                zset_returned.len() % 2,
                "the zset listing for {zset_key} returned an odd number of entries ({}), so \
                 halving it to an element count would be quietly wrong",
                zset_returned.len()
            );
            assert_eq!(
                population,
                zset_returned.len() / 2,
                "the zset control listed {} elements, not the {population} written -- a control \
                 over a short population is not the control this test claims",
                zset_returned.len() / 2
            );

            // THE CONTROL'S EXERCISED BYTES. A zero read count is only informative if the arm
            // actually produced member bytes.
            let zset_bytes: usize = zset_returned.iter().map(|entry| entry.len()).sum();
            assert!(
                zset_bytes > 0,
                "the zset control returned {zset_bytes} bytes, so its 0.00 reads per member is a \
                 control over nothing"
            );
            let set_bytes: usize = set_members.iter().map(|member| member.len()).sum();
            assert_eq!(
                population * width,
                set_bytes,
                "the set listing returned {set_bytes} bytes for {population} members of {width} \
                 bytes"
            );

            // THE FINDING, per row: one page read per member for the set.
            assert_eq!(
                population as u64, set_reads,
                "the set listing for {set_key} performed {set_reads} page reads for {population} \
                 members. The claim this module is quoted on is exactly one per member"
            );
            // THE CONTROL, per row: none for the zset.
            assert_eq!(
                0, zset_reads,
                "the zset listing for {zset_key} performed {zset_reads} page reads. A zset answers \
                 from `shard.zsets` and must read no page at all"
            );

            let per = set_reads as f64 / population as f64;
            println!(
                "  {width:>6}  {population:>7}  {set_reads:>9}  {per:>11.2}  {zset_reads:>9}  \
                 {:>10.2}  {set_bytes:>10}",
                0.0_f64
            );
            set_rows.push((width, population, set_reads, per));
            zset_total_reads += zset_reads;
            zset_total_members += population;
        }
    }

    // THE SLOPE ACROSS WIDTH. The page-read count is per MEMBER and not per byte, so it must be
    // flat across a 32x width change at a fixed population -- if it moved with width, the cost
    // would be something other than one read an element and the per-member figure would be a
    // coincidence of this fixture's sizes.
    for population in [SMALL_POPULATION, LARGE_POPULATION] {
        let at_narrowest = set_rows
            .iter()
            .find(|(width, pop, _, _)| *width == WIDTHS[0] && *pop == population)
            .map(|(_, _, reads, _)| *reads)
            .expect("the narrowest width was measured");
        let at_widest = set_rows
            .iter()
            .find(|(width, pop, _, _)| *width == WIDTHS[WIDTHS.len() - 1] && *pop == population)
            .map(|(_, _, reads, _)| *reads)
            .expect("the widest width was measured");
        assert_eq!(
            at_narrowest, at_widest,
            "at {population} members the read count moved from {at_narrowest} to {at_widest} \
             across a {}x width change. One read per member does not depend on member width",
            WIDTHS[WIDTHS.len() - 1] / WIDTHS[0]
        );
    }

    println!(
        "\n  set : 1.00 page reads per member at every width and both populations\n  zset: \
         {zset_total_reads} reads over {zset_total_members} members = 0.00 per member, with member \
         bytes returned"
    );
}

/// THE COST IS PER COLD LISTING, NOT PER CALL.
///
/// Stated separately so the count above is not read as a per-request figure. `cached_response`
/// stores the encoded answer under `CacheKey::set_members`, so the second listing of a key reads
/// nothing. The first is asserted non-zero as this test's own denominator: if the first read no
/// pages either, the second reading none would say nothing.
///
/// rust-internal: drives the engine's own command surface
#[test]
fn a_second_set_listing_reads_no_blocks_at_all_because_the_answer_is_cached() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = engine_on(dir.path());
    load_on(&engine);

    let key = "cached-listing".to_string();
    const MEMBERS: usize = 16;
    for index in 0..MEMBERS {
        write(
            &engine,
            Command::SetAdd {
                key: key.clone(),
                member: member_of(32, index),
            },
        );
    }

    let (first_response, first_reads) =
        block_reads_of(|| read(&engine, Command::SetMembers { key: key.clone() }));
    let (second_response, second_reads) =
        block_reads_of(|| read(&engine, Command::SetMembers { key: key.clone() }));

    assert_eq!(MEMBERS, members_returned(&first_response).len());
    assert_eq!(MEMBERS, members_returned(&second_response).len());

    // THE DENOMINATOR.
    assert_eq!(
        MEMBERS as u64, first_reads,
        "the cold listing read {first_reads} pages for {MEMBERS} members; with zero, the warm \
         listing reading zero would be no finding"
    );
    assert_eq!(
        0, second_reads,
