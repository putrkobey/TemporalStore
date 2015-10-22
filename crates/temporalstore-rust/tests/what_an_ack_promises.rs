// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A RECORD CARRIES, PER WRITE SHAPE, AT THE SHIPPED DEFAULTS.
//!
//! An acknowledged write must survive the loss of this node. After a crash, replay can put an
//! acked value back from exactly three places in the record that acked it:
//!
//!   1. an OPERATION to re-run,
//!   2. a PAGE carried inside the record,
//!   3. a RESULT that states itself -- the bytes, a removal, or a new deadline -- and so needs no
//!      page at all.
//!
//! A result naming only a block-store ADDRESS is none of those three, because the data-page
//! fdatasync is deferred past the ack: the address can name bytes a power cut left unwritten. So
//! "what does an ack promise" reduces to a count -- per write shape, how many acked results have
//! none of the three. This file takes that count in all eight cells and prints it.
//!
//! # The three choices, and why the eighth cell is not reachable by a page
//!
//!   * SEPARATE or BATCHED. These are different appends. A separate write reaches
//!     `append_for_group_commit`, whose rule is `!staged_blocks.is_empty()` over the single record
//!     it is writing; a batch asks `wal::every_result_can_be_found_again`, the same question per
//!     RESULT. The batch rule used to be `sync || !staged_blocks.is_empty()`, which was wrong
//!     twice over: it asked a per-result question at BATCH granularity, where one carried page
//!     answers for every result in the batch, and `sync` is not a durability fact about a page at
//!     all -- it is a fact about the LOG's barrier, which the page fsync is deferred past.
//!   * SYNCHRONOUS or ASYNCHRONOUS -- `Config::async_storage`, the per-shard durability choice.
//!   * A PAGE CARRIED IN THE RECORD, or NO PAGE AT ALL. There is no third option in production:
//!     every `append_value` call site passes `Some(object_id)`, and `append_value_inner` stages
//!     unconditionally for those, so a page that reaches a slab is always also in its record. The
//!     cell "page in the block store but not in the record" does not exist, and the no-page cell
//!     is a REMOVAL.
//!
//! # Why the count is per RESULT and not per record
//!
//! A batch is ONE record carrying many results. A per-record count calls that record covered as
//! soon as any one of its results is -- which is also the number a batch that lost half its keys
//! would produce. So each address-only result is matched against the pages its own record carries,
//! on (object id, element). That match UNDER-matches by design: a page's object id is derived
//! without the element and a timestamped result's with it, so an uncovered count can only ever be
//! too HIGH. A zero from it is therefore trustworthy, which is the direction a durability claim
//! needs.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use temporalstore_rust::{
    BatchExecuteRequest, Command, Config, ExecuteRequest, FeaturePoint, SetConfigRequest,
    TemporalEngine,
};

/// What the records of one cell are carrying.
///
/// `blocks_in_the_record` is the column the durability note on `TS_WAL_DATA_ONLY` did not have. It
/// counted operations and result values, found zero of each, and concluded there was nothing to
/// rebuild from -- while the pages were travelling inside those same records the whole time.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
struct Carried {
    records: usize,
    records_with_an_operation: usize,
    results: usize,
    results_carrying_bytes: usize,
    results_stating_a_removal: usize,
    results_stating_a_deadline: usize,
    results_naming_only_an_address: usize,
    blocks_in_the_record: usize,
    results_with_no_way_back: usize,
}

impl Carried {
    /// Results that state their own outcome, so no page has to survive for them.
    fn results_stating_themselves(&self) -> usize {
        self.results_carrying_bytes
            + self.results_stating_a_removal
            + self.results_stating_a_deadline
    }

    /// Every record kept the operation that produced it.
    fn every_record_kept_its_operation(&self) -> bool {
        self.records_with_an_operation == self.records
    }
}

#[derive(Debug, Clone, Copy)]
struct Shape {
    label: &'static str,
    synchronous: bool,
    batched: bool,
    /// An upsert produces a page, so its record carries one. A removal has no page to carry.
    upsert: bool,
}

const SHAPES: &[Shape] = &[
    Shape { label: "sync  / separate / upsert ", synchronous: true,  batched: false, upsert: true  },
    Shape { label: "sync  / batched  / upsert ", synchronous: true,  batched: true,  upsert: true  },
    Shape { label: "async / separate / upsert ", synchronous: false, batched: false, upsert: true  },
    Shape { label: "async / batched  / upsert ", synchronous: false, batched: true,  upsert: true  },
    Shape { label: "sync  / separate / removal", synchronous: true,  batched: false, upsert: false },
    Shape { label: "sync  / batched  / removal", synchronous: true,  batched: true,  upsert: false },
    Shape { label: "async / separate / removal", synchronous: false, batched: false, upsert: false },
    Shape { label: "async / batched  / removal", synchronous: false, batched: true,  upsert: false },
];

fn key_of(i: u64) -> String {
    format!("ack-{i:06}")
}

fn value_of(i: u64) -> Vec<u8> {
    format!("ackv-{i:06}-{}", "y".repeat(40)).into_bytes()
}

/// The workload each upsert cell runs.
///
/// Mixed on purpose. One command kind makes a whole cell look like whichever path that kind takes,
/// and these are five different page shapes through the same append: a whole-object value, a hash
/// element, a set member, a scored member, and a packed timestamped series.
fn upsert_workload(base: u64) -> Vec<Command> {
    let mut commands = Vec::new();
    for i in base..base + 4 {
        commands.push(Command::StringSet {
            key: key_of(i),
            value: value_of(i),
        });
        commands.push(Command::HashSet {
            key: format!("h-{i:06}"),
            field: "f".to_string(),
            value: value_of(i),
        });
        commands.push(Command::SetAdd {
            key: format!("s-{i:06}"),
            member: value_of(i),
        });
        commands.push(Command::ZSetAdd {
            key: format!("z-{i:06}"),
            member: value_of(i),
            score: i as f64,
        });
        commands.push(Command::FeatureAppend {
            key: format!("f-{i:06}"),
            points: vec![FeaturePoint {
                timestamp_ms: (i + 1) * 1_000,
                value: value_of(i),
            }],
        });
    }
    commands
}

/// Removals of keys the setup window already wrote, so each one really removes something. A
/// removal of an absent key is not a write and appends no record, which would leave the cell
/// measuring an empty log and agreeing with everything.
fn removal_workload(base: u64) -> Vec<Command> {
    let mut commands = Vec::new();
    for i in base..base + 4 {
        commands.push(Command::StringDelete { key: key_of(i) });
        commands.push(Command::CommonDelete {
            key: format!("s-{i:06}"),
        });
        commands.push(Command::CommonExpire {
            key: format!("h-{i:06}"),
            ttl_ms: 600_000,
        });
    }
    commands
}

fn open_engine(root: &Path) -> TemporalEngine {
    TemporalEngine::with_local_dirs(
        256,
        root.join("cache"),
        root.join("blocks"),
        root.join("indexes"),
    )
}

fn log_store(root: &Path) -> temporalstore_rust::LocalWriteAheadLogStore {
    let wals = root.join("indexes").join("wals");
    temporalstore_rust::LocalWriteAheadLogStore::new(&wals)
}

/// Every log id in the log right now, so the measured window is "the records that are not among
/// these".
///
/// A SET of ids rather than a count to subtract. A count difference also moves when the setup
/// window appends nothing and the measured one appends twice, and a number two different stories
/// produce is not evidence.
fn log_ids(root: &Path) -> HashSet<u64> {
    log_store(root)
        .scan(1, 0, u64::MAX, u64::MAX)
        .expect("scan the log")
        .into_iter()
        .map(|(log_id, _)| log_id)
        .collect()
}

/// What the records appended since `before` carry.
fn carried_since(root: &Path, before: &HashSet<u64>) -> Carried {
    let records = log_store(root)
        .scan(1, 0, u64::MAX, u64::MAX)
        .expect("scan the log");
    let mut counts = Carried::default();
    for (log_id, bytes) in &records {
        if before.contains(log_id) {
            continue;
        }
        let record = temporalstore_rust::wal::decode_wal_line(bytes)
            .expect("every record this store wrote must decode");
        counts.records += 1;
        // An operation covers every result in its record: re-running it reproduces all of them.
        let has_operation = record.command.is_some();
        if has_operation {
            counts.records_with_an_operation += 1;
        }
        counts.blocks_in_the_record += record.staged_blocks.len();
        // Which pages this record carries, keyed as the page itself spells its identity.
        let mut blocks: HashMap<(u64, Option<String>), usize> = HashMap::new();
        for block in &record.staged_blocks {
            *blocks
                .entry((
                    block.object_id,
                    block.component.as_ref().map(|c| c.to_string()),
                ))
                .or_default() += 1;
        }
        for item in &record.outcomes {
            counts.results += 1;
            let carries_bytes = item.value.as_ref().map(|v| !v.is_empty()).unwrap_or(false);
            if carries_bytes {
                counts.results_carrying_bytes += 1;
            }
            if item.deleted {
                counts.results_stating_a_removal += 1;
            }
            if item.ttl.is_some() {
                counts.results_stating_a_deadline += 1;
            }
            let states_itself = carries_bytes || item.deleted || item.ttl.is_some();
            if !states_itself && item.address.is_some() {
                counts.results_naming_only_an_address += 1;
            }
            if states_itself || has_operation {
                continue;
            }
            // Nothing in the result says what the value is and there is no operation to re-run,
            // so the only way back is a page for THIS result inside THIS record.
            let carried = blocks.contains_key(&(item.object_id, item.component.clone()))
                || blocks.contains_key(&(item.object_id, None));
            if !carried {
                counts.results_with_no_way_back += 1;
            }
        }
    }
    counts
}

/// Drive one cell and report what its records carry.
///
/// The setup window is always the DEFAULT shape -- synchronous, separate, upserting -- so the keys
/// a removal cell removes exist, and the config change an asynchronous cell needs lands before the
/// measured window opens. Nothing the setup appended is counted.
fn measure(shape: Shape) -> Carried {
    let dir = tempfile::tempdir().expect("tempdir");
