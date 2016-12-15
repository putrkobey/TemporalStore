// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Property/invariant conformance harness.
//!
//! Instead of sampling behavior with hand-written cases, this drives seeded random command
//! sequences through a real engine and asserts two invariants that catch the bug classes the
//! manual audits kept finding one at a time:
//!   1. NO-PANIC: no generated command (including adversarial reversed range bounds, huge counts,
//!      empty values, duplicate timestamps, cross-model deletes) may panic -- a panic under the
//!      shard write lock poisons it and takes the whole shard down (the reversed-bounds DoS class).
//!   2. RELOAD FIDELITY: the observable state after unload+reload from disk must be byte-identical
//!      to the state before unload -- no lost value, no resurrected delete, no reordering
//!      (the watermark / CP4 / delete_drop / membership-reconcile data-loss classes).
//!
//! Every failure reproduces from its integer seed, so a discovered bug is a deterministic repro.

use super::*;

/// Deterministic xorshift64* PRNG -- no external rand dependency, fully reproducible per seed.
struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15 | 1)
    }
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
    /// Model-namespaced key: each model owns a disjoint key space so a key is only ever ONE model
    /// (as real clients use them). This isolates single-model reload fidelity from the separate
    /// cross-model-collision question. `prefix` is the model tag; CommonDelete/Expire pass a union.
    fn model_key(&mut self, prefix: char) -> String {
        format!("{prefix}{}", self.below(4))
    }
    /// A key from ANY model's space, for the cross-model CommonDelete/CommonExpire commands.
    fn any_key(&mut self) -> String {
        let prefix = ['s', 'h', 'e', 'f', 'c', 'i'][self.below(6) as usize];
        self.model_key(prefix)
    }
    fn ts(&mut self) -> u64 {
        self.below(64)
    }
    fn small_bytes(&mut self) -> Vec<u8> {
        let len = self.below(5) as usize;
        (0..len).map(|_| (self.next() & 0xff) as u8).collect()
    }
    fn field(&mut self) -> String {
        format!("f{}", self.below(4))
    }
}

/// A possibly-reversed, possibly-degenerate time range to stress the no-panic invariant.
fn adversarial_bounds(rng: &mut Rng) -> (u64, u64) {
    let a = rng.ts();
    let b = rng.ts();
    match rng.below(4) {
        0 => (b, a),           // possibly reversed (start > end)
        1 => (0, u64::MAX),    // full
        2 => (a, a),           // single point
        _ => (a, b),           // arbitrary
    }
}

fn gen_command(rng: &mut Rng) -> Command {
    match rng.below(20) {
        0 => Command::StringSet {
            key: rng.model_key('s'),
            value: rng.small_bytes(),
        },
        1 => Command::StringDelete {
            key: rng.model_key('s'),
        },
        2 => Command::StringGet {
            key: rng.model_key('s'),
        },
        3 => Command::HashSet {
            key: rng.model_key('h'),
            field: rng.field(),
            value: rng.small_bytes(),
        },
        4 => Command::HashDelete {
            key: rng.model_key('h'),
            field: rng.field(),
        },
        5 => Command::HashIncrBy {
            key: rng.model_key('h'),
            field: rng.field(),
            increment: (rng.below(2001) as i64) - 1000,
        },
        6 => Command::SetAdd {
            key: rng.model_key('e'),
            member: rng.small_bytes(),
        },
        7 => Command::SetRemove {
            key: rng.model_key('e'),
            member: rng.small_bytes(),
        },
        8 => Command::FeatureAppend {
            key: rng.model_key('f'),
            points: (0..1 + rng.below(3))
                .map(|_| FeaturePoint {
                    timestamp_ms: rng.ts(),
                    value: rng.small_bytes(),
                })
                .collect(),
        },
        9 => {
            let (start_ms, end_ms) = adversarial_bounds(rng);
            Command::FeatureQuery {
                key: rng.model_key('f'),
                start_ms,
                end_ms,
                count: if rng.below(3) == 0 {
                    None
                } else {
                    Some(rng.below(10) as usize)
                },
            }
        }
        10 => Command::ControlStateIncrement {
            key: rng.model_key('c'),
            timestamp_ms: rng.ts(),
            amount: (rng.below(2001) as i64) - 1000,
        },
        11 => {
            let (start_ms, end_ms) = adversarial_bounds(rng);
            Command::ControlStateQuery {
                key: rng.model_key('c'),
                start_ms,
                end_ms,
                aggregator: ["sum", "count", "min", "max"][rng.below(4) as usize].to_string(),
            }
        }
        14 => Command::CommonDelete { key: rng.any_key() },
        15 => Command::SequenceAdd {
            key: rng.model_key('q'),
            rows: (0..1 + rng.below(3))
                .map(|_| SequenceFeatureRow {
                    timestamp_ms: rng.ts(),
                    gid: rng.below(1000),
                    action_type: rng.below(8) as u32,
                    duration: rng.below(100) as u32,
                    author_id: rng.below(1000),
                })
                .collect(),
        },
        16 => {
            let (start_ms, end_ms) = adversarial_bounds(rng);
            Command::FeatureReplace {
                key: rng.model_key('f'),
                start_ms,
                end_ms,
                points: (0..1 + rng.below(3))
                    .map(|_| FeaturePoint {
                        timestamp_ms: rng.ts(),
                        value: rng.small_bytes(),
                    })
                    .collect(),
            }
        }
        17 => Command::FeatureDelete {
            key: rng.model_key('f'),
        },
        18 => Command::ControlStateChangeAdd {
            key: rng.model_key('g'),
            timestamp_ms: rng.ts(),
            value: rng.small_bytes(),
            precision_ms: None,
            ttl_ms: None,
        },
        // Large TTL only: sets the expiry path without firing during the test, so the reload
        // fidelity check stays time-independent (active expiry is covered by its own tests).
        _ => Command::CommonExpire {
            key: rng.any_key(),
            ttl_ms: 1 << 40,
        },
    }
}

/// A deterministic, full-range read of every model for every key -- the observable state used to
/// compare pre-unload vs post-reload. Stringified so any response shape is comparable.
fn read_snapshot(engine: &TemporalEngine) -> Vec<String> {
    let mut snapshot = Vec::new();
    let mut probe = |label: &str, command: Command| {
        let resp = engine.execute(ExecuteRequest {
            shard_id: 1,
            command,
        });
        snapshot.push(format!("{label}={:?}", resp.response));
    };
    for n in 0..4 {
        probe(&format!("s{n}"), Command::StringGet { key: format!("s{n}") });
        probe(&format!("h{n}"), Command::HashGetAll { key: format!("h{n}") });
        probe(&format!("e{n}"), Command::SetMembers { key: format!("e{n}") });
        probe(
            &format!("f{n}"),
            Command::FeatureQuery {
                key: format!("f{n}"),
                start_ms: 0,
                end_ms: u64::MAX,
                count: None,
            },
        );
        probe(
            &format!("c{n}"),
            Command::ControlStateQuery {
                key: format!("c{n}"),
                start_ms: 0,
                end_ms: u64::MAX,
                aggregator: "sum".to_string(),
            },
        );
        probe(
            &format!("q{n}"),
            Command::SequenceQuery {
                key: format!("q{n}"),
                start_ms: 0,
                end_ms: u64::MAX,
                count: 1_000_000,
                filters: Vec::new(),
            },
        );
        probe(
            &format!("g{n}"),
            Command::ControlStateQuery {
                key: format!("g{n}"),
                start_ms: 0,
                end_ms: u64::MAX,
                aggregator: "change".to_string(),
            },
        );
    }
    drop(probe);
    snapshot
}

fn run_one_sequence(seed: u64, maintenance: bool) -> Result<(), String> {
    let dir = tempfile::tempdir().unwrap();
    let blocks = dir.path().join("blocks");
    let indexes = dir.path().join("indexes");
    let engine =
        TemporalEngine::with_local_dirs(1 << 20, dir.path().join("cache-a"), &blocks, &indexes);
    engine.load_shard(1);

    let mut rng = Rng::new(seed);
    let op_count = 20 + rng.below(50);
    for _ in 0..op_count {
        // Maintenance ops (real dump/compaction work) are expensive, so they are OFF by default
        // (fast suite run) and enabled for deep hunts via CONFORMANCE_MAINTENANCE=1.
        match if maintenance { rng.below(12) } else { 2 + rng.below(10) } {
            // Interleave storage-lifecycle maintenance so the fuzzer also exercises the dump /
            // clear-dirty / reclaim / compaction paths against reload fidelity (the CP4 / watermark
            // / dump-scheduling bug class) -- not only command execution.
            0 => {
                let _ = engine.apply_storage_lifecycle(StorageLifecycleRequest {
                    shard_id: 1,
                    purge_delayed_destroy: true,
                    ..Default::default()
                });
            }
            1 => {
                let _ = engine.compact_shard_blocks(1);
            }
            _ => {
                let command = gen_command(&mut rng);
                // A returned error status is fine (e.g. not-an-integer HINCRBY); a PANIC is not --
                // caught by catch_unwind in the caller as a no-panic-invariant violation.
                let _ = engine.execute(ExecuteRequest {
                    shard_id: 1,
                    command,
                });
            }
        }
    }

    let before = read_snapshot(&engine);
    engine.unload_shard(1);

    // Two reload cycles: reload-on-load runs reconcile, which may itself persist a rebuilt index.
    // A single reload can hide a bug where reconstruction PERSISTS a subtly-wrong index that only a
    // SECOND reload then reads back wrong (accumulation / non-idempotent reconcile). Every reload's
    // observable state must equal the pre-unload state.
    let mut prior = before.clone();
    for (cycle, cache) in ["cache-b", "cache-c"].into_iter().enumerate() {
        let reloaded =
            TemporalEngine::with_local_dirs(1 << 20, dir.path().join(cache), &blocks, &indexes);
        reloaded.load_shard(1);
        let after = read_snapshot(&reloaded);
        if prior != after {
            let diff = prior
                .iter()
                .zip(after.iter())
                .filter(|(b, a)| b != a)
                .map(|(b, a)| format!("\n  before: {b}\n  after:  {a}"))
                .collect::<String>();
            return Err(format!("reload fidelity mismatch (cycle {cycle}):{diff}"));
        }
        reloaded.unload_shard(1);
        prior = after;
    }
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Reference-model oracle: a naive, obviously-correct in-Rust model of the engine's INTENDED
// semantics. Diffing the engine against it after a random sequence catches WRONG-RESULT bugs
// (a query returning the wrong value), which the reload/no-panic invariants cannot see. Scoped to
// the commands with unambiguous semantics (string/hash/set/feature point storage +
// cross-model CommonDelete); the model mirrors the engine's CURRENT documented behavior (e.g.
// HINCRBY parses stored ints strtoll-style skipping leading whitespace, and errors -> no change).
#[derive(Default)]
struct RefModel {
    strings: HashMap<String, Vec<u8>>,
    hashes: HashMap<String, std::collections::BTreeMap<String, Vec<u8>>>,
    sets: HashMap<String, std::collections::BTreeSet<Vec<u8>>>,
    features: HashMap<String, std::collections::BTreeMap<u64, Vec<u8>>>,
    sequences: HashMap<String, std::collections::BTreeMap<u64, SequenceFeatureRow>>,
    // ControlStateIncrement accumulates `amount` into a raw-timestamp bucket.
    control_state: HashMap<String, std::collections::BTreeMap<u64, i64>>,
    // ControlStateChangeAdd inserts distinct values into a raw-timestamp bucket; "change" queries
    // return the distinct-value count over the range (the current, deliberately-distinct semantics).
    control_state_changes:
        HashMap<String, std::collections::BTreeMap<u64, std::collections::BTreeSet<Vec<u8>>>>,
}

impl RefModel {
    fn apply(&mut self, command: &Command) {
        match command {
            Command::StringSet { key, value } => {
                self.strings.insert(key.clone(), value.clone());
            }
            Command::StringDelete { key } => {
                self.strings.remove(key);
            }
            Command::HashSet { key, field, value } => {
                self.hashes
                    .entry(key.clone())
                    .or_default()
                    .insert(field.clone(), value.clone());
            }
            Command::HashDelete { key, field } => {
                if let Some(fields) = self.hashes.get_mut(key) {
                    fields.remove(field);
                    if fields.is_empty() {
                        self.hashes.remove(key); // emptying a hash removes the key (Redis + engine)
                    }
                }
            }
            Command::HashIncrBy {
                key,
                field,
                increment,
            } => {
                let fields = self.hashes.entry(key.clone()).or_default();
                match fields.get(field) {
                    // Absent field increments from 0; only i64 overflow errors.
                    None => {
                        if let Some(next) = 0i64.checked_add(*increment) {
                            fields.insert(field.clone(), next.to_string().into_bytes());
                        }
                    }
                    // Present field: parse like the engine (strtoll -> skip leading ASCII
                    // whitespace). Non-integer or overflow => the engine errors and leaves the
                    // value unchanged.
                    Some(bytes) => {
                        let parsed = std::str::from_utf8(bytes)
                            .ok()
                            .map(|text| text.trim_start_matches(|c: char| c.is_ascii_whitespace()))
                            .and_then(|text| text.parse::<i64>().ok());
                        if let Some(current) = parsed {
                            if let Some(next) = current.checked_add(*increment) {
                                fields.insert(field.clone(), next.to_string().into_bytes());
                            }
                        }
                    }
                }
            }
            Command::SetAdd { key, member } => {
                self.sets.entry(key.clone()).or_default().insert(member.clone());
            }
            Command::SetRemove { key, member } => {
                if let Some(members) = self.sets.get_mut(key) {
                    members.remove(member);
                }
            }
            Command::FeatureAppend { key, points } => {
                let series = self.features.entry(key.clone()).or_default();
                // Last-write-wins per timestamp (matches sorted_feature_points + BTreeMap upsert).
                for point in points {
                    series.insert(point.timestamp_ms, point.value.clone());
                }
            }
            Command::FeatureReplace {
                key,
                start_ms,
                end_ms,
                points,
            } => {
                let series = self.features.entry(key.clone()).or_default();
                // Remove existing points in [start, end] (empty if start > end, matching the engine's
                // reversed-bounds guard), then upsert the new points (which may fall outside the
                // range). feature_max_size trim not modeled -- the generator stays far under 5000.
                if start_ms <= end_ms {
                    series.retain(|timestamp, _| !(*timestamp >= *start_ms && *timestamp <= *end_ms));
                }
                for point in points {
                    series.insert(point.timestamp_ms, point.value.clone());
                }
            }
            Command::FeatureDelete { key } => {
                self.features.remove(key);
            }
            Command::ControlStateIncrementWithOptions {
                key,
                timestamp_ms,
                amount,
                precision_ms,
                ..
            } => {
                let bucket = precision_ms
                    .filter(|precision| *precision > 0)
                    .map(|precision| timestamp_ms - timestamp_ms % precision)
                    .unwrap_or(*timestamp_ms);
                let slot = self
                    .control_state
                    .entry(key.clone())
                    .or_default()
                    .entry(bucket)
                    .or_default();
                *slot = slot.wrapping_add(*amount);
            }
            Command::SequenceAdd { key, rows } => {
                let series = self.sequences.entry(key.clone()).or_default();
                // Last-write-wins per timestamp (matches sorted_feature_points collapse).
                for row in rows {
                    series.insert(row.timestamp_ms, row.clone());
                }
            }
            Command::ControlStateIncrement {
                key,
                timestamp_ms,
                amount,
            } => {
                let bucket = self
                    .control_state
                    .entry(key.clone())
                    .or_default()
                    .entry(*timestamp_ms)
                    .or_default();
                *bucket = bucket.wrapping_add(*amount);
            }
            Command::ControlStateChangeAdd {
                key,
