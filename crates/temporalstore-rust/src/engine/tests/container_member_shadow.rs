// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT ONE BLOCK PER CONTAINER MEMBER COSTS, AND WHY A PACKED MEMBER BUFFER IS NOT THE LEVER.
//!
//! A container in this engine gives every member its own page. A zset member becomes a page whose
//! component name is `zset_component(member)`, a set member's is `hex::encode(member)`, a
//! list element's is the biased sequence in hex. Each page gets a `BlockIndex` in the routing
//! bucket's page index, an entry in the object/component lookup, and an outcome staged for the
//! index log. So a hundred-member container is a hundred page entries under one object key -- and
//! `bucket_fill.rs` already established that one object key is ONE routing bucket at any range,
//! because routing takes the object key and never sees the component.
//!
//! THE QUESTION THIS MODULE WAS OPENED TO ANSWER was whether those hundred entries should become
//! ONE packed byte buffer per container: a small header, then the members as raw bytes and the
//! scores as numbers, with no per-member name string and no per-member index entry.
//!
//! THE ANSWER FOR A ZSET IS THAT THE PACKED BUFFER ALREADY EXISTS, AND THE PER-MEMBER PAGES ARE A
//! SECOND COPY OF IT. `ShardState::zsets` is
//! `HashMap<String, BTreeMap<Vec<u8>, (u64, BlockAddress)>>` and it is PERSISTED, through
//! `zset_index_serde`, as a sequence of `(member bytes, (score, address))` per key -- members as
//! RAW BYTES, the score as a NUMBER, one container per key, no component names anywhere in it.
//! That is the packed shape, already written, already the better encoding. What sits beside it is
//! a per-member page shadow that this module prices.
//!
//! THREE FACTS DECIDE THE SHAPE OF THE SHADOW, AND ALL THREE ARE ASSERTED BELOW RATHER THAN
//! ARGUED.
//!
//!   1. THE PAGE PAYLOAD IS THE MEMBER, AND THE COMPONENT NAME ALREADY SPELLS THE MEMBER.
//!      `Command::ZSetAdd` calls `append_value(.., &member, ..)`, so the stored page IS the member
//!      bytes; the component beside it is sixteen hex digits of score followed by the same member
//!      in hex. `a_zset_element_page_holds_exactly_what_its_component_name_already_spells` decodes
//!      every component in a seeded shard back to (score, member) and asserts it against the model
//!      map, so the redundancy is a byte-level fact and not an inference from the call site.
//!
//!   2. NO ZSET READ RESOLVES THROUGH THE PAGE INDEX. `ZSetScore` takes the score out of
//!      `shard.zsets`, `ZSetCard` takes `len()`, `ZSetRange` orders the same map's keys.
//!      `bucket_index_component_block_addresses` -- the door a container read goes through -- is
//!      called for `"hash"` and `"set"` and for nothing else.
//!      `a_zset_read_examines_no_page_index_entries_and_a_hash_read_does` counts it with
//!      `BLOCK_LOOKUP_ENTRIES_EXAMINED`, the counter inside `find_block`, and carries the hash arm as
//!      the CONTROL: an instrument reading zero on both arms would be measuring nothing.
//!
//!   3. INDEX-LOG REPLAY REBUILDS THE MEMBER AND THE SCORE FROM THE COMPONENT NAME. The `"zset"`
//!      arm of `apply_outcome_item` (`lifecycle.rs`) splits the component at sixteen, parses the
//!      score with `u64::from_str_radix` and the member with `hex::decode`, and inserts the pair
//!      into `shard.zsets`. It never reads the page. So the shadow is not dead weight everywhere:
//!      it is the SOURCE OF TRUTH for one of the three recovery routes, and that is the reader
//!      that pins it.
//!
//! WHICH IS WHY THIS MODULE MEASURES AND DOES NOT CONVERT. Removing the per-member pages for a
//! zset would take the index-log route's only copy of the member away, and replacing the model map
//! with a single page per container would turn a one-member update from a small append into a
//! whole-container rewrite. `what_one_more_member_rewrites_against_what_one_packed_buffer_would`
//! measures both sides of that with the engine's own serializer rather than projecting it.
//!
//! WHAT IS NOT MEASURED HERE, stated so the cover is not read as total: the `hashes` map is
//! `skip_serializing` on `ShardState` and is rebuilt FROM the bucket index on load, so for a hash
//! the per-field pages ARE the only durable copy and none of the above applies to it. The hash arm
//! appears below only as the read-path control.
//!
//! THE STORE PATH LENGTH is held constant across arms and asserted equal: it moves allocation
//! bytes at about six bytes a character, and an arm on a longer temporary directory would read as
//! a heavier representation.
#![allow(clippy::all)]
use super::*;
use crate::engine::execute_on_shard::zset_component;
use crate::engine::hashing::{block_routing_bucket, stable_block_object_id};
use crate::engine::state::BlockIndex;
use std::collections::{BTreeMap, BTreeSet};

// Imported as a NAME rather than spelled out at the call site: the counting-allocator gate in
// `alloc_probe.rs` walks back from every line quoting the probe's full path to the nearest
// `#[test]`, and a helper spelling it out would be reported as reading the probe outside a test.
#[cfg(feature = "alloc-probe")]
use crate::alloc_probe::Probe;

/// The end bucket `docs/runtime_tuning.md` tells an operator to set, and the shipped default since
/// #1973. THE OPERATOR'S RANGE.
const NARROW_END: u32 = 1023;

/// The end bucket `TemporalEngine::load_shard` uses when nothing says otherwise. Every key lands
/// in a bucket of its own BY CONSTRUCTION at this width, so it is an artefact of the default and
/// not a workload, which is why every row below is reported at both.
const WIDE_END: u32 = u32::MAX;

/// Members under one container key. The measured shape: p50 100 components under one object key,
/// MAX 100.
const MEMBERS: usize = 100;

/// Container keys in the seeded shard. Enough that the narrow range holds several keys per bucket
/// and the wide range does not, so the two arms differ in the way the range decides.
const CONTAINER_KEYS: usize = 40;

/// The member width the shadow is priced at. Twenty bytes: the width the component-name arithmetic
/// below is quoted at, and wide enough that the hex spelling dominates the fixed score prefix.
const MEMBER_WIDTH: usize = 20;

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
        table_name: "member-shadow".to_string(),
        shard_uri: "local://member-shadow/1".to_string(),
        start_routing_bucket: 0,
        end_routing_bucket,
        readonly: false,
        load_version: 1,
        local_node_id: Some(1),
    });
    assert!(
        response.status.ok,
        "the fixture could not load shard 1 on 0..{end_routing_bucket}: {:?}",
        response.status
    );
}

fn run_batch(engine: &TemporalEngine, commands: Vec<Command>) {
    for chunk in commands.chunks(1_000) {
        let response = engine.batch_execute(crate::types::BatchExecuteRequest {
            shard_id: 1,
            commands: chunk.to_vec(),
        });
        assert!(response.status.ok, "seed must ack: {:?}", response.status);
    }
}

/// A member of `MEMBER_WIDTH` bytes, distinct per (key index, member index), and NOT hex-friendly:
/// the bytes run over the whole 0..=255 range so `hex::encode` of them is a genuine doubling and
/// not a sample of a narrow alphabet.
fn member_bytes(key_index: usize, member_index: usize) -> Vec<u8> {
    let mut member = Vec::with_capacity(MEMBER_WIDTH);
    for byte in 0..MEMBER_WIDTH {
        member.push(((key_index * 131 + member_index * 17 + byte * 7) % 256) as u8);
    }
    member
}

fn container_keys() -> Vec<String> {
    (0..CONTAINER_KEYS).map(|k| format!("zs-{k:06}")).collect()
}

/// `CONTAINER_KEYS` zsets of `MEMBERS` members each, through the production command path.
fn seed_zsets(engine: &TemporalEngine) -> Vec<String> {
    let keys = container_keys();
    let mut commands = Vec::with_capacity(CONTAINER_KEYS * MEMBERS);
    for (k, key) in keys.iter().enumerate() {
        for m in 0..MEMBERS {
            commands.push(Command::ZSetAdd {
                key: key.clone(),
                member: member_bytes(k, m),
                score: m as f64,
            });
        }
    }
    run_batch(engine, commands);
    keys
}

/// One zset page in the seeded shard: which object key, which component, and the address.
#[derive(Debug, Clone)]
struct ZsetBlock {
    routing_bucket: u32,
    object_key: String,
    component: String,
    address: crate::block_store::ElementEntry,
    dirty: bool,
    deleted: bool,
    log_backed: bool,
}

/// Every `zset` page the bucket index holds, read off the engine's own index.
fn zset_blocks(engine: &TemporalEngine) -> Vec<ZsetBlock> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    let named = super::BlockObjects::of(&shard.bucket_index);
    let mut blocks = Vec::new();
    for (routing_bucket, bucket) in &shard.bucket_index.bucket_map {
        for (handle, block) in bucket.block_index.iter() {
            if block.model_id.as_str() != "zset" {
                continue;
            }
            blocks.push(ZsetBlock {
                routing_bucket: *routing_bucket,
                object_key: named.at(*routing_bucket, *handle).to_string(),
                // THIS `expect` WAS THE MODULE'S OWN CONTROL AND IT CANNOT HOLD. It read
                // `page.component.as_deref().expect("a zset page is named by its component")`.
                // A zset page entry names no element -- that is the collapse -- so the walk
                // panicked rather than failing an assertion. The redundancy this module measures
                // between a component and the page payload is what the collapse DELETED, so the
                // field is carried as the empty string and the arms that priced that redundancy
                // are restated where they are read.
                component: String::new(),
                address: block.address.clone(),
                dirty: block.dirty,
                deleted: block.deleted,
                log_backed: block.log_backed(),
            });
        }
    }
    blocks
}

/// The members of one zset, as the model map holds them: raw bytes and a numeric score.
fn model_members(engine: &TemporalEngine, key: &str) -> BTreeMap<Vec<u8>, u64> {
    let shards = engine.shards.read().expect("engine lock poisoned");
    let shard = shards.get(&1).expect("shard is loaded");
    shard
        .zsets
        .get(key)
        .map(|members| {
            members
                .iter()
                .map(|(member, (biased, _))| (member.clone(), *biased))
                .collect()
        })
        .unwrap_or_default()
}

/// Components held per routing bucket, as bucket COUNTS keyed by the number held.
///
/// A histogram and not a mean: a mean of 1.98 pages a bucket once contained not one bucket holding
/// two, and a mean of 1.001 was two populations.
#[derive(Debug, Default)]
struct Histogram {
    counts: BTreeMap<usize, usize>,
}

impl Histogram {
    fn add(&mut self, held: usize) {
        *self.counts.entry(held).or_default() += 1;
    }

    fn buckets(&self) -> usize {
        self.counts.values().copied().sum()
    }

    fn items(&self) -> usize {
        self.counts.iter().map(|(held, count)| held * count).sum()
    }

    fn widest(&self) -> usize {
        self.counts.keys().copied().next_back().unwrap_or_default()
    }

    /// The `p`th percentile of the per-bucket count, by bucket.
    fn percentile(&self, p: f64) -> usize {
        let buckets = self.buckets();
        if buckets == 0 {
            return 0;
        }
        let target = ((buckets as f64) * p).ceil().max(1.0) as usize;
        let mut seen = 0usize;
        for (held, count) in &self.counts {
            seen += count;
            if seen >= target {
                return *held;
            }
        }
        self.widest()
    }

    fn report(&self, label: &str) {
        println!(
            "{label}: {} buckets holding {} pages, p50 {}, p90 {}, MAX {}",
            self.buckets(),
            self.items(),
            self.percentile(0.50),
            self.percentile(0.90),
            self.widest()
        );
        for (held, count) in &self.counts {
            println!(
                "    {held:>4} page(s): {count:>5} buckets ({:>6.2}% of {})",
                100.0 * *count as f64 / self.buckets().max(1) as f64,
                self.buckets()
            );
        }
    }
}

fn zset_blocks_per_bucket(blocks: &[ZsetBlock]) -> Histogram {
    let mut per_bucket: BTreeMap<u32, usize> = BTreeMap::new();
    for block in blocks {
        *per_bucket.entry(block.routing_bucket).or_default() += 1;
    }
    let mut hist = Histogram::default();
    for held in per_bucket.values() {
        hist.add(*held);
    }
    hist
}

// =============================================================================================
// 1. THE STRUCTURES, RECONSTRUCTED FIELD BY FIELD
// =============================================================================================

/// What `account` returns: the field bytes covered and the slack around them.
fn account(fields: &[(&'static str, usize, usize)], total: usize) -> (usize, usize) {
    let covered: usize = fields.iter().map(|(_, _, width)| *width).sum();
    (covered, total.saturating_sub(covered))
}

/// EVERY BYTE OF THE TWO PER-MEMBER STRUCTURES, MEASURED WITH `offset_of!` RATHER THAN ASSUMED.
///
/// These are the two structures whose COUNT is the member count, so these are the two whose width
/// the shadow is quoted in. `repr(Rust)` reorders, so nothing here assumes declaration order.
///
/// THE WIDTHS ARE NOT LITERALS IN THE ARITHMETIC BELOW. Every per-member figure in this module is
/// computed from `size_of` so that a sibling narrowing either structure moves the finding instead
/// of falsifying it. That was not a hypothetical: #1974 took `BlockIndex` from 88 to 72 between
/// this module being written and it landing, and every measured figure moved with it on its own.
/// The literal ceilings here are asserted as CEILINGS and reported, and the per-member total is
/// printed from the measured widths.
///
/// WHAT DID NOT MOVE ON ITS OWN, and it is the reason the reconstruction below is worth having:
/// the field LIST is hand-written, one `offset_of!` per named field, and `size_of` of the WRONG
/// type still compiles. When `model_id` stopped being an `Arc<str>` the compiler flagged one
/// unrelated deref and said nothing about the width in this list. The reconstruction assertion is
/// what failed, and it named the arithmetic that no longer added up.
///
/// rust-internal: reads this crate's own type layout, no product behaviour
#[test]
fn every_byte_of_a_zset_members_index_shadow_is_accounted_for() {
    use std::mem::{align_of, offset_of, size_of};

    let index_total = size_of::<BlockIndex>();
    let index_fields: Vec<(&'static str, usize, usize)> = vec![
        // THE `object_key` ROW IS GONE WITH THE FIELD -- sixteen bytes of fat pointer, the whole
        // of the 40 -> 24 step. Each of these reconstructions is DERIVED from the field list it
        // holds rather than totalled as a literal, which is what makes deleting a row safe: a
        // list that stops matching the type fails at the reconstruction rather than describing a
        // structure that moved.
        // ONE BYTE since #1974, not a sixteen-byte fat pointer. The compiler did NOT catch this
        // line when the field's type changed -- `size_of` of the wrong type still compiles -- so
        // the reconstruction assertion below is what caught it, which is the reason it exists.
        (
            "model_id",
            offset_of!(BlockIndex, model_id),
            size_of::<crate::engine::storage_bucket_internals::StoredModelKind>(),
        ),
        // THE `component` ROW IS GONE WITH THE FIELD -- sixteen bytes of fat pointer between
        // `model_id` and `address`, which is the whole of the 56 -> 40 step. The reconstruction
        // assertion below is what makes removing the row safe: a row that no longer matches a
        // field fails there rather than silently describing a type that moved.
        ("address", offset_of!(BlockIndex, address), size_of::<crate::block_store::ElementEntry>()),
        ("dirty", offset_of!(BlockIndex, dirty), size_of::<bool>()),
        ("deleted", offset_of!(BlockIndex, deleted), size_of::<bool>()),
    ];
    let (index_covered, index_padding) = account(&index_fields, index_total);

    let mut sorted = index_fields.clone();
    sorted.sort_by_key(|(_, offset, _)| *offset);
    println!(
        "--- BlockIndex, {index_total} bytes, align {} ---",
        align_of::<BlockIndex>()
    );
    let mut cursor = 0usize;
    for (name, offset, width) in &sorted {
        println!(
            "  +{offset:>3}  {name:<12} width {width:>2}  padding before {}",
            offset - cursor
        );
        cursor = offset + width;
    }
    println!("  tail padding {}", index_total - cursor);
    println!("  field bytes {index_covered}, slack {index_padding}, total {index_total}");

    // THE RECONSTRUCTION, and it is DERIVED FROM THE MEASURED FIELD LIST rather than written as a
    // sum of the types this struct happens to hold today. The eight-aligned group is every field
    // whose own width is a whole number of words; everything else is the tail, rounded up to the
    // struct's alignment. That holds across a width step instead of having to be rewritten at one:
    // it read 80 aligned + 3 tail before #1974 and 64 aligned + 4 tail after it, and this line was
    // not edited between the two.
    let word = align_of::<BlockIndex>();
    let aligned_group: usize = index_fields
        .iter()
        .map(|(_, _, width)| *width)
        .filter(|width| width % word == 0)
        .sum();
    let tail: usize = index_fields
        .iter()
        .map(|(_, _, width)| *width)
        .filter(|width| width % word != 0)
        .sum();
    println!("  reconstruction: {aligned_group} aligned + {tail} tail rounded to {word}");
    assert_eq!(
        aligned_group + tail.div_ceil(word) * word,
        index_total,
        "the reconstruction of BlockIndex no longer adds up to its width: {aligned_group} aligned \
         + {tail} tail rounded to {word} is not {index_total}. If a field's TYPE changed, the \
         field list above is what needs editing"
    );
    assert_eq!(index_covered + index_padding, index_total);
    // 72 since #1974 took the model spelling from a sixteen-byte fat pointer to one byte. A CEILING,
    // so a narrowing sibling lowers it rather than failing here; it was 88 before that step.
    assert!(
        index_total <= 72,
        "BlockIndex grew past its budgeted ceiling: {index_total} > 72"
    );

    // THE CLAIM ABOUT THE FLAGS, stated as the property rather than as today's number. The three
    // flag bytes fit entirely inside the alignment slack, so packing them into a bitfield reclaims
    // no whole byte of the struct -- and it would move the stored index, which spells each flag as
    // its own key. Asserted as `slack >= the flags` so it survives a width step, AND IT HAD TO:
    // the slack was five bytes at 99, 91 and 83 bytes of field, and #1974's step to 68 leaves
    // FOUR. A literal `5` here -- which is what the first draft of this test asserted -- would have
    // gone red on that rebase while the thing it states stayed true.
    // THE CLAIM, ASSERTED AS ITSELF RATHER THAN THROUGH A PROXY THAT ZERO SLACK HAS BROKEN.
    //
    // This read `slack >= the flags`, which was SUFFICIENT for "packing them reclaims no whole
    // byte" only while there was slack. There is none now -- the entry carries 56 bytes of field
    // in 56 -- so the proxy is no longer implied by a claim that is still TRUE: packing the two
    // remaining flags into one byte leaves 55 of field, which still rounds to 56.
    //
    // The comment above records that a literal `5` here went red on a rebase "while the thing it
    // states stayed true". This is the same failure one level up: the literal was replaced by a
    // RELATION, and the relation went stale too. So the claim is now asserted directly.
    //
    // The flag count is derived from the table rather than written as a number, because a field
    // count spelled as a constant is exactly what no compile can object to.
    let flag_bytes: usize = index_fields
        .iter()
        .filter(|(name, _, _)| matches!(*name, "dirty" | "deleted"))
        .map(|(_, _, width)| *width)
        .sum();
    assert!(
        flag_bytes > 0,
        "VACUITY: no flag field was found in the table by name, so the assertion below would be \
         comparing the struct's width against itself"
    );
    let with_flags_packed = index_covered - flag_bytes + 1;
