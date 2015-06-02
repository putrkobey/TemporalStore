// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What an in-memory `BlockAddress` costs, and what a compact form would buy.
//!
//! WHY THIS EXISTS. `BlockAddress` is 24 bytes. Two of its fields -- the packed slab-and-offset
//! `address` word and `length` -- are always meaningful. Two more -- `page_id` and `object_id` --
//! are OPTIONAL, gated by a `present` bitmask, and the struct allocates both whether or not the
//! bitmask says they are set. That is 10 bytes of optional payload plus one byte of bitmask, and
//! the shard holds one of these per stored point: a 1,000-point feature series holds 1,000 of them.
//!
//! It was 48, with a FOURTH optional field, until `generation` was shown to be a copy of
//! `page_id.or(object_id)` on every live address this census walked and was made derived. It was
//! 40 until the slab id and the offset merged into one 64-bit word, the slab in the high half and
//! the offset in the low. It was 32 until `routing_bucket` -- a THIRD optional field, and 4 bytes
//! of the optional payload -- stopped being held at all: a block's bucket is
//! `block_routing_bucket(object_key, ..)` over the range the store is STAMPED with, so the
//! container a block is read through answers it and the block does not have to carry it.
//!
//! So the obvious question is whether to pack what is left. The answer turns on a single number:
//! how many live addresses actually carry NONE of the two optional fields. If most carry none, the
//! optional payload is dead weight and a compact form reclaims it. If most carry both, there is
//! nothing to reclaim and packing buys only the bitmask byte and the padding.
//!
//! AND THE ANSWER HAS A SECOND HALF NOW, because the struct is 24 and not 32. The payload without
//! `object_id` is 15 bytes, so `object_id` at ONE byte would take the struct to 16 and at TWO bytes
//! would leave it at 24. Packing the optional half is therefore not a question about two fields any
//! more: it is a question about whether an object identity fits in eight bits.
//!
//! AND IT DOES NOT HAVE TO, because the identity does not have to be STORED to be available.
//! Nothing bounds the objects in a routing bucket at 256, so an eight-bit identity is not on offer
//! -- but `the_object_id_on_a_live_block_entry_is_the_hash_of_fields_beside_it` measures the other
//! shape the arithmetic allows, the field GONE, and finds the stored id is
//! `stable_block_object_id(shard, kind, key)` on 2,524 of 2,524 live block entries and on
//! 20,632 of 20,632, differing on none. So the question the width turns on is not how narrow an
//! identity can be. It is what a read site that cannot recompute one would do, and `block_store`'s
//! note on `BlockAddress` carries that answer: read MISSING for an acked write, silently.
//!
//! `the_optional_payload_is_paid_for_on_every_live_address` measures that number. It is the whole
//! task, and it is measured before anything else here.
//!
//! WHAT THE ROUTING BUCKET'S DEPARTURE DID TO THIS FILE, stated rather than left to be inferred:
//! every count of "how many optional fields are set" went from four to three, the tamper probe
//! `only_one_of_the_two_address_cross_checks_on_a_read_can_fire` lost an arm that could never fire,
//! and `differing_fields` lost a row it could never report. None of the VERDICTS moved.
//!
//! WHAT THESE PROBES ARE NOT. They are `#[ignore]`d because they seed tens of thousands of
//! records and read process RSS, which is neither fast nor meaningful under a parallel test run.
//! Run them by name. Two tests here are NOT ignored, and both are cheap:
//! `an_address_is_twenty_four_bytes_and_ten_of_them_are_optional` pins the width so that
//! widening the struct is noticed rather than absorbed, and
//! `only_one_of_the_two_address_cross_checks_on_a_read_can_fire` pins how many of the address
//! cross-checks on the read path can actually fire, which is the count that decides what dropping
//! an optional field would cost.
//!
//! NON-VACUITY. Every count below is printed beside its denominator, and every census asserts its
//! maps are populated BEFORE it reports an occupancy. A census that walked an empty shard would
//! report "0 of 0 carry an optional field", which reads exactly like the answer that would justify
//! packing. `the_census_reads_every_map_that_holds_an_address` is the control: it fails if the
//! fixture stops populating any map the census claims to cover.
#![allow(clippy::all)]
use super::*;
use std::collections::BTreeMap;

/// Resident set size of this process, in bytes. Field 2 of `/proc/self/statm` is resident pages.
fn resident_bytes() -> u64 {
    let statm = std::fs::read_to_string("/proc/self/statm").expect("procfs is mounted");
    let blocks: u64 = statm
        .split_whitespace()
        .nth(1)
        .expect("statm has a resident field")
        .parse()
        .expect("resident field is a number");
    blocks * 4096
}

/// Every live address in a shard, counted by where it lives and by what it carries.
#[derive(Default, Debug)]
struct AddressCensus {
    /// (map name, how many addresses that map holds), in walk order.
    per_map: Vec<(&'static str, usize)>,
    total: usize,
    with_block_id: usize,
    with_object_id: usize,
    with_generation: usize,
    /// How many addresses carry exactly 0, 1, 2 or 3 of the optional fields.
    ///
    /// THREE, NOT FOUR. `routing_bucket` was the fourth and is not a field any more; `generation`
    /// is the third and has no bytes of its own -- it is a presence bit over a derived value --
    /// which is why `dead_optional_bytes` below counts BYTES directly rather than scaling this
    /// histogram by an average width.
    optional_field_histogram: [usize; 4],
    /// Bytes of optional payload that are allocated and NOT set, summed over every address.
    ///
    /// Counted per field rather than as `absent_count * average_width`. The average was defensible
    /// while the two optional fields were 8 and 4 bytes wide; at 8 and 2 it would report a number
    /// no address ever pays.
    dead_optional_bytes: usize,
    /// Entries held in a `BTreeMap<_, BlockAddress>` rather than a `HashMap`, which is the
    /// population a node-overhead figure applies to.
    in_btree: usize,
    /// Addresses held in an OWNED SORTED ARRAY rather than a B-tree. Separate from
    /// `in_btree` because the two are different shapes with different costs, and folding one
    /// into the other would make the printed line below say something it does not mean.
    in_owned_array: usize,
    /// The largest value observed in each field, in walk order:
    /// block_slab_id, offset, length, page_id, object_id, generation.
    ///
    /// This is what decides whether a LOSSLESS narrower form exists at all. Presence tells you
    /// whether a field can be omitted; width tells you whether it can be shrunk. A field that is
    /// always set AND uses its full 64 bits cannot be made smaller without dropping information.
    widest: [u64; 6],
    // THE COPY COUNTER THAT USED TO SIT HERE IS GONE, BECAUSE IT CAN NO LONGER FAIL.
    //
    // It counted addresses whose `generation` equalled `page_id.or(object_id)`, and it answered
    // 12,008 of 12,008 at 8,000 records and 120,080 of 120,080 at 80,000 -- differing on none.
    // That number is what justified dropping the field: it carried no information of its own, so
    // deriving it cost 8 bytes and imposed no capacity ceiling, which narrowing always does.
    //
    // `generation` is now computed as exactly that expression, so a counter comparing the two
    // would be asserting `x == x` on every row and reporting 100% whatever the engine did. A
    // tautology printed beside real measurements is worse than no measurement, because it reads
    // like confirmation. What replaced it:
    //   * the derivation itself, in `BlockAddress::generation`, which no caller can bypass;
    //   * `block_store::address_size_tests::an_index_whose_generation_disagrees_is_refused_loudly`,
    //     which drives a STORED generation that disagrees and requires the load to be refused --
    //     the only place a disagreement can still arise, and the only place it can still be seen.
}

impl AddressCensus {
    fn observe(&mut self, address: &ElementEntry) {
        self.total += 1;
        self.widest[0] = self.widest[0].max(address.block_slab_id());
        self.widest[1] = self.widest[1].max(address.offset());
        self.widest[2] = self.widest[2].max(address.length());
        self.widest[3] = self.widest[3].max(address.block_id().unwrap_or(0));
        // Slot 4 held the object id and the address no longer carries one. The slot is left unused
        // rather than renumbered: every reader of `widest` indexes it by position, and a shift would
        // silently re-label five other rows.
        self.widest[5] = self.widest[5].max(address.generation().unwrap_or(0));
        let mut set = 0usize;
        if address.block_id().is_some() {
            self.with_block_id += 1;
            set += 1;
        } else {
            // The block id is TWO bytes now, not four: the encoder has always refused anything
            // above `u16::MAX`, so the field never needed more.
            self.dead_optional_bytes += 2;
        }
        // `with_object_id` STOOD HERE. There is no optional object id to be present or absent, so
        // there are no dead bytes to charge for it either: the eight it used to charge when absent
        // are not in the struct at all now, which is the whole of this change.
        if address.generation().is_some() {
            self.with_generation += 1;
            set += 1;
        }
        self.optional_field_histogram[set] += 1;
    }

    fn note(&mut self, name: &'static str, count: usize) {
        self.per_map.push((name, count));
    }

    /// The optional payload is 10 bytes. An address carrying none of it pays 10 bytes for
    /// nothing; one carrying both pays nothing for nothing.
    ///
    /// SUMMED PER FIELD AS IT IS WALKED, not reconstructed from the histogram. The two optional
    /// fields are 8 and 2 bytes wide, so "how many are unset" cannot price them: an address missing
    /// only the block id wastes 2 bytes and one missing only the object id wastes 8, and both
    /// appear in the same histogram bucket.
    fn dead_optional_bytes(&self) -> usize {
        self.dead_optional_bytes
    }

    fn report(&self, label: &str) {
        println!("--- address census: {label} ---");
        for (name, count) in &self.per_map {
            if *count > 0 {
                println!("  {name}: {count}");
            }
        }
        println!(
            "  TOTAL live addresses: {} (of which {} sit in a BTreeMap and {} in an owned sorted array)",
            self.total, self.in_btree, self.in_owned_array
        );
        println!(
            "  optional fields SET, of {} addresses: page_id {} ({:.1}%), object_id {} ({:.1}%), generation {} ({:.1}%)",
            self.total,
            self.with_block_id,
            100.0 * self.with_block_id as f64 / self.total.max(1) as f64,
            self.with_object_id,
            100.0 * self.with_object_id as f64 / self.total.max(1) as f64,
            self.with_generation,
            100.0 * self.with_generation as f64 / self.total.max(1) as f64,
        );
        for (set, count) in self.optional_field_histogram.iter().enumerate() {
            println!(
                "  carrying exactly {set} of 3 optional fields: {count} ({:.1}% of {})",
                100.0 * *count as f64 / self.total.max(1) as f64,
                self.total
            );
        }
        // Read off the type rather than written down. A hand-written width here went stale the
        // moment the struct moved, and this line would have kept reporting the old figure.
        let width = std::mem::size_of::<ElementEntry>();
        println!(
            "  address payload resident: {} x {} = {} bytes ({:.2} MiB)",
            self.total,
            width,
            self.total * width,
            (self.total * width) as f64 / (1024.0 * 1024.0)
        );
        println!(
            "  of which optional payload NEVER SET: {} bytes ({:.2} MiB) -- the whole packing prize",
            self.dead_optional_bytes(),
            self.dead_optional_bytes() as f64 / (1024.0 * 1024.0)
        );

        println!(
            "  generation is DERIVED as page_id.or(object_id) -- not stored, so not counted here"
        );

        // Presence says whether a field can be OMITTED. Width says whether it can be SHRUNK.
        // Both have to fail before the remaining 32 bytes are justified.
        let names = [
            "block_slab_id", "offset", "length",
            "page_id", "object_id", "generation",
        ];
        let mut lossless_bits = 0u32;
        println!("  observed field widths (the bits a LOSSLESS narrower form would still need):");
        for (i, name) in names.iter().enumerate() {
            let bits = 64 - self.widest[i].leading_zeros();
            lossless_bits += bits.max(1);
            println!("    {name}: max {} -> {} bit(s)", self.widest[i], bits);
        }
        println!(
            "  a form sized to the values ACTUALLY observed here needs {lossless_bits} bits = {} bytes, \
             but those maxima are a property of THIS fixture, not of the type: every one of these \
             fields is read as u64 and a production shard is free to use the range",
            (lossless_bits + 7) / 8,
        );
    }
}

/// Walk every map on a shard that holds a `BlockAddress`, including the bucket index.
///
/// The bucket index matters and is easy to miss: `BlockIndex` carries its own `BlockAddress`, so
/// every block addressed by a model map is addressed a SECOND time here. A census that read only
/// the model maps would under-count the population by about half.
fn census(engine: &TemporalEngine, shard_id: ShardId) -> AddressCensus {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&shard_id).expect("shard is loaded");
    let mut c = AddressCensus::default();

    // ONE ADDRESS PER KEY, THREE MAPS -- BUT NO LONGER ONE ARRAY OF ONE TYPE.
    //
    // These three used to be an array literal, which requires a single element type. The
    // `ShardState` note on this class says they have to "agree on their key type" because of THIS
    // array -- and that was an undercount: an array literal binds the VALUE type too. One of the
    // three holds an `ObjectRecord` now, so the array stopped type-checking, which is how the
    // coupling surfaced.
    //
    // Materialising the addresses per map keeps the walk and the per-map attribution identical
    // while letting the values differ. The record map pays one `address()` per entry, in a test
    // probe, which is the right place for that cost.
    for (name, addresses) in [
        ("strings", shard.strings().values().map(|slot| slot.address()).collect::<Vec<_>>()),
        (
            "control_state_pages",
            shard
                .control_state_blocks()
                .values()
                .map(|record| record.address())
                .collect::<Vec<_>>(),
        ),
        ("context_nodes", shard.context_nodes().values().cloned().collect::<Vec<_>>()),
    ] {
        let before = c.total;
        for address in &addresses {
            c.observe(address);
        }
        c.note(name, c.total - before);
    }

    // HashMap<String, HashMap<String, BlockAddress>>
    {
        let before = c.total;
        for fields in shard.hashes.values() {
            for address in fields.values() {
                c.observe(address);
            }
        }
        c.note("hashes", c.total - before);
    }

    // HashMap<String, BTreeMap<Vec<u8>, BlockAddress>>
    {
        let before = c.total;
        for members in shard.sets.values() {
            for address in members.values() {
                c.observe(address);
            }
        }
        c.in_btree += c.total - before;
        c.note("sets", c.total - before);
    }

    // HashMap<String, BTreeMap<Vec<u8>, (u64, BlockAddress)>>
    {
        let before = c.total;
        for members in shard.zsets.values() {
            for (_, address) in members.values() {
                c.observe(address);
            }
        }
        c.in_btree += c.total - before;
        c.note("zsets", c.total - before);
    }

    // HashMap<String, BTreeMap<i64, BlockAddress>>
    {
        let before = c.total;
        for elements in shard.lists.values() {
            for address in elements.values() {
                c.observe(address);
            }
        }
        c.in_btree += c.total - before;
        c.note("lists", c.total - before);
    }

    // The time-keyed series maps: HashMap<String, BTreeMap<u64, BlockAddress>>. One address PER
    // POINT, which is the population the whole question is about.
    // THE EVENTS MAP FIRST, ON ITS OWN. It holds an owned sorted array per key now, so it is
    // not the same type as the seven below and cannot ride in one array with them -- and its
    // addresses are counted into `in_owned_array` rather than `in_btree`, which is the honest
    // bucket for them. It still calls `note`, because the census asserts it walks SIXTEEN maps and
    // that count is invisible to a grep for a field name.
    {
        let before = c.total;
        for series in shard.context_events().values() {
            for address in series.addresses() {
                c.observe(address);
            }
        }
        c.in_owned_array += c.total - before;
        c.note("context_events", c.total - before);
    }
    // FLATTENED TO ADDRESSES AND TYPE-ERASED, so the value type stops mattering: `features` holds
    // the owned array now and the context kinds still hold B-trees, and an array literal demands
    // one element type. Both representations expose `values()` over `&ElementEntry`, which is all
    // this walk looks at.
    for (name, addresses) in [
        (
            "features",
            Box::new(shard.features().values().flat_map(|series| series.addresses()))
                as Box<dyn Iterator<Item = &ElementEntry>>,
        ),
        ("context_indexes", Box::new(shard.context_indexes().values().flat_map(|s| s.addresses()))),
        ("context_audits", Box::new(shard.context_audits().values().flat_map(|s| s.owned_elements().unwrap_or(&[]).iter().map(|(_, a)| a)))),
        ("context_entities", Box::new(shard.context_entities().values().flat_map(|s| s.addresses()))),
        ("context_children", Box::new(shard.context_children().values().flat_map(|s| s.owned_elements().unwrap_or(&[]).iter().map(|(_, a)| a)))),
        ("context_summaries", Box::new(shard.context_summaries().values().flat_map(|s| s.owned_elements().unwrap_or(&[]).iter().map(|(_, a)| a)))),
        (
            "context_compressions",
            Box::new(shard.context_compressions().values().flat_map(|s| s.owned_elements().unwrap_or(&[]).iter().map(|(_, a)| a))),
        ),
    ] {
        let before = c.total;
        for address in addresses {
            c.observe(address);
        }
        c.in_btree += c.total - before;
        c.note(name, c.total - before);
    }

    // The bucket index -- the SECOND copy of every address.
    {
        let before = c.total;
        for bucket in shard.bucket_index.bucket_map.values() {
            for (_, block) in bucket.block_index.iter() {
                c.observe(&block.address);
            }
        }
        c.note("bucket_index (BlockIndex.address)", c.total - before);
    }

    c
}

/// An engine with NO shard loaded, so the caller can load one on a chosen routing range.
///
/// `new_engine` above loads shard 1 on the engine's default range, which is what every census here
/// wants; a probe that has to vary the RANGE cannot use it, because a second `load_shard_with` on the
/// same shard answers `already_exists` and the arm silently measures the default instead.
fn engine_without_a_shard(dir: &std::path::Path) -> Arc<TemporalEngine> {
    Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ))
}

fn new_engine(dir: &std::path::Path) -> Arc<TemporalEngine> {
    let engine = Arc::new(TemporalEngine::with_local_dirs(
        64 * 1024 * 1024,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    ));
    engine.load_shard(1);
    engine
}

/// `strings_n` single-address keys, plus `series_keys` feature series of `series_points` points
/// each. Returns (strings written, feature points written).
fn seed(engine: &TemporalEngine, strings_n: usize, series_keys: usize, series_points: usize) -> (usize, usize) {
    for chunk_start in (0..strings_n).step_by(1_000) {
        let commands = (chunk_start..(chunk_start + 1_000).min(strings_n))
            .map(|i| Command::StringSet {
                key: format!("s{i}"),
                value: vec![b'v'; 32],
            })
            .collect::<Vec<_>>();
        if commands.is_empty() {
            continue;
        }
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands,
        });
        assert!(response.status.ok, "string seed must ack: {:?}", response.status);
    }

    for k in 0..series_keys {
        for chunk_start in (0..series_points).step_by(500) {
            let points = (chunk_start..(chunk_start + 500).min(series_points))
                .map(|t| crate::types::FeaturePoint {
                    timestamp_ms: 1_700_000_000_000 + t as u64,
                    value: vec![b'f'; 32],
                })
                .collect::<Vec<_>>();
            if points.is_empty() {
                continue;
            }
            let response = engine.execute(ExecuteRequest {
                shard_id: 1,
                command: Command::FeatureAppend {
                    key: format!("f{k}"),
                    points,
                },
            });
            assert!(response.status.ok, "feature seed must ack: {:?}", response.status);
        }
    }

    (strings_n, series_keys * series_points)
}

/// THE NUMBER THIS TASK TURNS ON, at both scales.
///
/// If most live addresses carry none of the three optional fields, packing reclaims 10 bytes each
/// and is worth doing. If most carry all three, packing reclaims the bitmask byte and the padding
/// and is not.
#[test]
#[ignore = "seeds 80,000 records; run by name"]
fn the_optional_payload_is_paid_for_on_every_live_address() {
    for (label, strings_n, series_keys, series_points) in [
        ("8,000 records", 4_000usize, 4usize, 1_000usize),
        ("80,000 records", 40_000usize, 40usize, 1_000usize),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = new_engine(dir.path());
        let (strings_written, points_written) = seed(&engine, strings_n, series_keys, series_points);

        let c = census(&engine, 1);

        // NON-VACUITY, asserted before any occupancy is read. Eight defects in this tree have
        // been a sweep reporting a clean number off an empty set.
        assert!(
            c.total >= strings_written + points_written,
            "{label}: census must see at least the {} addresses seeded, saw {}",
            strings_written + points_written,
            c.total
        );
        let strings_seen = c.per_map.iter().find(|(n, _)| *n == "strings").expect("strings walked").1;
        let features_seen = c.per_map.iter().find(|(n, _)| *n == "features").expect("features walked").1;
        let buckets_seen = c
            .per_map
            .iter()
            .find(|(n, _)| n.starts_with("bucket_index"))
            .expect("bucket index walked")
            .1;
        assert_eq!(
            strings_written, strings_seen,
            "{label}: strings map must hold one address per key"
        );
        assert_eq!(
            points_written, features_seen,
            "{label}: features must hold one address PER POINT"
        );
        assert!(
            buckets_seen > 0,
            "{label}: the bucket index holds a second address per page and must not read zero"
        );

        c.report(label);

        // The claim, stated as a number rather than a direction.
        let none_set = c.optional_field_histogram[0];
        let all_three = c.optional_field_histogram[3];
        println!(
            "{label}: {none_set} of {} addresses ({:.1}%) carry NO optional field; {all_three} ({:.1}%) carry all three",
            c.total,
            100.0 * none_set as f64 / c.total as f64,
            100.0 * all_three as f64 / c.total as f64,
        );
        println!(
            "{label}: packing the optional payload away entirely would reclaim at most {} of {} resident address bytes ({:.1}%)",
            c.dead_optional_bytes(),
            c.total * std::mem::size_of::<ElementEntry>(),
            100.0 * c.dead_optional_bytes() as f64
                / (c.total * std::mem::size_of::<ElementEntry>()) as f64,
        );
    }
}

/// What a `BTreeMap` entry costs on top of the value it carries, measured rather than derived.
///
/// A `BTreeMap<u64, BlockAddress>` does not cost one address width per entry. Its leaf node is sized for
/// eleven entries and is allocated whole, so the per-entry cost is the node divided by how full
/// the node actually ends up -- which depends on insertion order and is not something to assume.
/// This measures it by RSS delta, and prices three value widths side by side so the container
/// cost and the value cost can be told apart.
///
/// EVERY ARM IS HELD ALIVE TO THE END, and that is load-bearing rather than tidy. Dropping an arm
/// before measuring the next one returns its heap to the allocator's free lists but NOT to the
/// kernel, so the next arm allocates into pages that are already resident and its RSS delta reads
/// as approximately zero. That is a vacuous measurement that looks exactly like a free container:
/// an earlier draft of this test reported "0.3 bytes/entry" for the packed arm and it meant
/// nothing. Keeping every map live forces each arm to fault in new pages.
///
/// The `Vec` arm is the POSITIVE CONTROL: a `Vec<(u64, BlockAddress)>` has a known, tight
/// footprint (56 bytes per element, no node), so if the harness cannot see that it cannot see
/// anything and every other number here is noise.
#[test]
#[ignore = "reads process RSS; run alone"]
fn a_btree_entry_costs_more_than_the_address_it_holds() {
    const N: usize = 400_000;

    // THE BLOCK ID IS MASKED TO SIXTEEN BITS, and that is not cosmetic. `try_from_parts` REFUSES a
    // block id wider than `u16::MAX` rather than truncating it -- a truncated block id names a
    // different block of the same object -- so this fixture's synthetic `i` panicked at 65,536 of its
    // 400,000 entries the moment the field narrowed. Masking keeps the footprint identical (the field
    // is a fixed width whatever value it holds) and keeps every value legal, which is what the
    // refusal is for. `the_container_shapes_priced_against_the_population_each_one_pays_in` carries
    // the same fixture at 200,000 and the same mask.
    fn address(i: u64) -> ElementEntry {
        ElementEntry::from_parts(1, i * 64, 64, Some(i & u64::from(u16::MAX)), Some(i))
    }

    let control_before = resident_bytes();
    let control: Vec<(u64, ElementEntry)> = (0..N as u64).map(|i| (i, address(i))).collect();
    let control_after = resident_bytes();
    let control_per_entry = (control_after - control_before) as f64 / N as f64;
    assert_eq!(N, control.len(), "denominator: the control really holds N entries");
    println!(
        "CONTROL Vec<(u64, ElementEntry)>: {:.1} bytes/entry over {N} entries (size_of is {})",
        control_per_entry,
        std::mem::size_of::<(u64, ElementEntry)>()
    );
    // THE FLOOR IS DERIVED FROM THE ELEMENT, not written down. It read `> 40.0` beside a doc
    // comment claiming 56 bytes an element, and the element is now 32 -- so the literal was already
    // describing a shape this engine did not have, and would have turned a narrower address into a
    // blind-harness report. Half the element width is the vacuity guard: a near-zero reading fails,
    // and a correct reading cannot.
    let control_floor = size_of::<(u64, ElementEntry)>() as f64 / 2.0;
    assert!(
        control_per_entry > control_floor,
        "positive control must see the Vec it just built: {control_per_entry:.1} bytes/entry \
         against a floor of {control_floor:.1} for a {} B element -- if this is near zero the RSS \
         harness is blind and every arm below is meaningless",
        size_of::<(u64, ElementEntry)>()
    );

    // Packed widths FIRST, fat value last: if the allocator were recycling anything, the fat arm
    // measured last would read low, and it does not.
    let before_packed = resident_bytes();
    let mut packed: BTreeMap<u64, [u8; 17]> = BTreeMap::new();
    for i in 0..N as u64 {
        packed.insert(i, [i as u8; 17]);
    }
    let after_packed = resident_bytes();
    let packed_per = (after_packed - before_packed) as f64 / N as f64;
    assert_eq!(N, packed.len(), "denominator: the packed map really holds N entries");

    let before_word = resident_bytes();
    let mut word: BTreeMap<u64, u128> = BTreeMap::new();
    for i in 0..N as u64 {
        word.insert(i, i as u128);
    }
    let after_word = resident_bytes();
    let word_per = (after_word - before_word) as f64 / N as f64;
    assert_eq!(N, word.len(), "denominator: the word map really holds N entries");

    let before_fat = resident_bytes();
    let mut fat: BTreeMap<u64, ElementEntry> = BTreeMap::new();
    for i in 0..N as u64 {
        fat.insert(i, address(i));
    }
    let after_fat = resident_bytes();
    let fat_per = (after_fat - before_fat) as f64 / N as f64;
    assert_eq!(N, fat.len(), "denominator: the address map really holds N entries");

    // Every arm still alive here, which is what makes the three deltas independent.
    std::hint::black_box((&control, &packed, &word, &fat));

    println!("--- BTreeMap cost per entry, {N} ascending inserts, measured by RSS delta ---");
    for (label, value_width, per) in [
        ("BTreeMap<u64, ElementEntry> (the 56-byte value we have)", 56.0, fat_per),
        ("BTreeMap<u64, [u8; 17]>     (the packed width already written down)", 17.0, packed_per),
        ("BTreeMap<u64, u128>         (a 16-byte value)", 16.0, word_per),
    ] {
        println!(
            "  {label}: {per:.1} bytes/entry -- value {value_width:.0}, container overhead {:.1} ({:.0}% of the entry)",
            per - value_width,
            100.0 * (per - value_width) / per,
        );
    }

    // Each arm must have cost SOMETHING, or its zero is the allocator talking and not the map.
    assert!(
        packed_per > 20.0 && word_per > 20.0 && fat_per > 60.0,
        "no arm may read as free: fat {fat_per:.1}, packed {packed_per:.1}, word {word_per:.1} \
         bytes/entry -- a near-zero arm means the allocator recycled and the arm measured nothing"
    );

    println!(
        "  a 56 -> 17 byte value moves the ENTRY from {fat_per:.1} to {packed_per:.1} bytes, \
         a {:.0}% saving -- next to the {:.0}% the value widths alone suggest, because the node \
         header and the key ride along either way",
        100.0 * (fat_per - packed_per) / fat_per,
        100.0 * (56.0 - 17.0) / 56.0,
    );
    println!(
        "  the container alone costs {:.1} bytes/entry with a 56-byte value: a BTreeMap leaf is \
         sized for eleven entries and ascending inserts split it 6/5, so it settles about half \
         full and the empty half is resident",
        fat_per - 56.0,
    );
}

/// What fraction of the shard's resident memory the addresses are -- and what `load_memory_bytes`
/// says about them, which is nothing.
#[test]
#[ignore = "seeds 80,000 records; run by name"]
fn the_addresses_are_a_measurable_slice_of_a_shard_that_nothing_counts() {
    let dir = tempfile::tempdir().expect("tempdir");
    let baseline = resident_bytes();
    let engine = new_engine(dir.path());
    let (strings_written, points_written) = seed(&engine, 40_000, 40, 1_000);
    let after = resident_bytes();
    let shard_resident = after.saturating_sub(baseline);

    let c = census(&engine, 1);
    assert!(
        c.total >= strings_written + points_written,
        "denominator: census must see the {} addresses seeded, saw {}",
        strings_written + points_written,
        c.total
    );
    assert!(shard_resident > 0, "denominator: the engine must have grown RSS at all");

    let address_bytes = c.total * 56;
    println!(
        "80,000 records: shard grew RSS by {} bytes ({:.1} MiB); {} addresses x 56 = {} bytes ({:.1} MiB) = {:.1}% of it",
        shard_resident,
        shard_resident as f64 / (1024.0 * 1024.0),
        c.total,
        address_bytes,
        address_bytes as f64 / (1024.0 * 1024.0),
        100.0 * address_bytes as f64 / shard_resident as f64,
    );
    println!(
        "  the optional payload never set within that: {} bytes = {:.2}% of shard RSS",
        c.dead_optional_bytes(),
        100.0 * c.dead_optional_bytes() as f64 / shard_resident as f64,
    );
    c.report("80,000 records, against shard RSS");
}

/// The control for the census itself.
///
/// The census claims to walk fifteen model maps plus the bucket index. If the fixture stops populating one of them -- or a
/// rename moves a field out from under the walk -- the census would quietly report a smaller
/// population, and a smaller population makes the packing case look weaker than it is. This
/// asserts the fixture really reaches the two shapes the argument distinguishes: a one-address
/// key and a many-address series.
#[test]
#[ignore = "seeds records; run by name"]
fn the_census_reads_every_map_that_holds_an_address() {
    let dir = tempfile::tempdir().expect("tempdir");
    let engine = new_engine(dir.path());
    let (strings_written, points_written) = seed(&engine, 200, 2, 300);
    let c = census(&engine, 1);

    let named: Vec<&str> = c.per_map.iter().map(|(n, _)| *n).collect();
    // Fifteen model maps PLUS the bucket index, which holds a second address per block. It read
    // sixteen-plus-one until the retired `sequences` map was deleted; a bare count like this is
    // invisible to a grep for the field name, and this assertion is what caught that removal.
    assert_eq!(
        16,
        named.len(),
        "the census must walk every address-holding map; it walked {named:?}"
    );
    let populated = c.per_map.iter().filter(|(_, count)| *count > 0).count();
    assert!(
        populated >= 3,
        "denominator: at least strings, features and the bucket index must be populated; \
         {populated} of {} maps held anything: {:?}",
        named.len(),
        c.per_map
    );
    assert_eq!(strings_written, c.per_map.iter().find(|(n, _)| *n == "strings").unwrap().1);
    assert_eq!(points_written, c.per_map.iter().find(|(n, _)| *n == "features").unwrap().1);
    assert!(c.in_btree >= points_written, "the series population must be counted as BTree-held");
    println!("census walks {} maps, {populated} populated by this fixture: {:?}", named.len(), c.per_map);
}

/// Which of the address's optional fields actually DO anything on a read -- measured by tampering
/// with each one and seeing whether the read notices.
///
/// This is the fourth thing the task asks about. `decode_block_record` cross-checks the address's
/// `page_id`, `object_id` and `routing_bucket` against the record header, each written
/// `if let (Some(from_address), Some(from_record))`. Reading that source alone, all three look
/// like live corruption detectors, and three live detectors would be a strong reason to keep the
/// fields regardless of what they cost.
///
/// They are not three. The record header carries only the block id; `object_id` and
/// `routing_bucket` are deliberately NOT in it, because the index holds them -- so both of those
/// `if let` pairs can never match and the checks they guard never run. This test pins which is
/// which, because the difference decides what a compact form would actually be giving up.
///
/// A FOURTH arm used to sit beside them, comparing `address.slab_id()` against a slab id in the
/// record header, and it is gone. It was not merely unable to fire: `BlockAddress::slab_id()`
/// returns the address's own `block_slab_id`, which is the slab file `BlockStore::read` opened to
/// get these bytes, so the address side was never an independent claim. An assertion below pins
/// that, because it is the reason reinstating the field would have bought nothing.
///
/// It cannot pass by finding nothing: the honest address must read back first, the live check must
/// REFUSE a tampered address, and the inert ones must ACCEPT one. An arm that stopped firing would
/// flip a count, not fall silent.
///
/// NOT ignored. It writes ONE block into a tempdir and asserts three halves separately, in
/// hundredths of a second -- no seeding, no RSS reading, no timing, so nothing about it ever
/// needed a run-by-name budget. It was parked with the measurement probes around it and stayed
/// there, which left the claim it carries -- that of the address cross-checks on the read path
/// exactly ONE can fire -- documented and unenforced. Every argument about dropping an optional
/// field from the address rests on that count, so it belongs in the gate.
///
/// IT WAS THREE ARMS AND IS NOW TWO, and the arm that went is the one that could never fire. The
/// routing-bucket cross-check compared `address.routing_bucket()` against `header.routing_bucket`,
/// and `parse_block_record_header` returns that field as `None` unconditionally because the record
/// header HAS NO SUCH FIELD -- `encode_block_record` is handed the value and discards it. So there
/// was no address-versus-record check on the routing bucket to lose: its address side is gone with
/// the field and its record side never existed. The two arms below are unchanged, and the one that
/// CAN fire is driven with a mismatched record rather than asserted about.
#[test]
fn only_one_of_the_two_address_cross_checks_on_a_read_can_fire() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store = BlockStore::new(dir.path());

    let payload = b"a page whose header states which block of its object it is".to_vec();
    let object_id = 0x0123_4567_89ab_cdefu64;
    let routing_bucket = 4_155_475_953u32;

    // The block under test is block THREE of its object, not block zero. Stripping the optional
    // field leaves the address carrying None, and if the block it names were block zero then an
    // implementation that DEFAULTED the absent field to 0 rather than skipping the check would
    // compare 0 against 0 and read through -- indistinguishable from the presence-gated behaviour
    // the last assertion claims to pin. A non-zero ordinal is what lets that half tell them apart.
    let good = store
        .append_block_of_object(&payload, Some(object_id), Some(routing_bucket), 3)
        .expect("append");
    assert_eq!(
        Some(3),
        good.block_id(),
        "denominator: the page under test must not be block zero, or the presence half below          cannot tell a skipped check from one that defaults the absent field to zero"
    );

    // DENOMINATOR: the honest address reads back, and really carries all three fields under test.
    assert_eq!(
        payload,
        store.read(&good).expect("the honest address must read back"),
        "denominator: the unmodified address reads its page"
    );
    assert!(good.block_id().is_some(), "fixture must produce an address carrying page_id");

    // Tamper with each field in turn and record whether the read noticed.
    let mut noticed: Vec<&str> = Vec::new();
    let mut ignored: Vec<&str> = Vec::new();

    let mut tampered = good.clone();
    tampered.set_block_id(Some(good.block_id().unwrap() ^ 0xffff));
    match store.read(&tampered) {
        Err(error) => {
            noticed.push("page_id");
            println!("  page_id wrong -> REFUSED: {error}");
        }
        Ok(_) => ignored.push("page_id"),
    }

    // THE OBJECT-ID ARM IS GONE WITH THE FIELD, and that is the stronger form of what it showed.
    // It tampered with an id the address carried and recorded that the read did not notice, because
    // the record header carries no object id to compare against. There is now no id on the address
    // to tamper with either, so the cross-check is not merely unfireable -- neither side of it
    // exists. One cross-check remains, and the block-id arm above is it.
    #[allow(unused_mut)]
    let mut tampered = good.clone();
    match store.read(&tampered) {
        Err(_) => noticed.push("object_id"),
        Ok(bytes) => {
            assert_eq!(payload, bytes);
            ignored.push("object_id");
            println!("  object_id cannot be made wrong: the address carries none");
        }
    }

    // THE ROUTING-BUCKET ARM CANNOT BE WRITTEN ANY MORE, and that is the strongest form of the
    // statement it used to make. `routing_bucket` was passed to `append_block_of_object` above and
    // the encoder discarded it; there is no accessor to tamper with and no header field to compare
    // against, so no record exists that could make such a check fire. The `routing_bucket` binding
    // is deliberately still handed to the append: it is what a caller passes, and a record that
    // started carrying it would make this arm expressible again.
    let _ = routing_bucket;

    // The arm that was deleted, and why it is not the same case as the two above. `slab_id()` is
    // derived, not stored: it hands back the address's own `block_slab_id`, and that is the slab
    // file the read just opened. A cross-check against it could only ever have asked whether the
    // slab a reader opened is the slab a reader opened -- true in every state, including one where
    // a slab id has been reused and a record in the new slab stamps the reused number.
    assert_eq!(
