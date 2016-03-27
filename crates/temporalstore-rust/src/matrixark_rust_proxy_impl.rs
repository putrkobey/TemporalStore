// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

// Shared implementation body for the thin proxy entrypoints
// (matrixark_rust_proxy, matrixark_rust_direct_sdk), which `include!` this file.
// It deliberately lives under src/ (not src/bin/) so it is NOT compiled as a
// standalone bin, and it carries no crate-level inner attributes: each includer
// sets its own `#![recursion_limit = "256"]` (required for a large `json!`
// literal below). An inner attribute here would be illegal once `include!`d.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::env;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{json, Value};
use temporalstore_rust::{
    BatchExecuteRequest, BatchExecuteResponse, BlockStoreOptions, Command, CommandResponse,
    ExecuteRequest, ExecuteResponse, ShardId, Status, TemporalEngine, TemporalStoreClient,
    TemporalStoreTable,
};
use temporalstore_rust::{Config, SetConfigRequest};

const DEFAULT_SHARD_ID: u64 = 1;
const LATENCY_BUCKETS_MS: [u128; 9] = [1, 2, 5, 10, 25, 50, 100, 250, 1000];
const DIRECT_RECORD_LOG_SHARD_SIZE: usize = 256;

/// Treat an explicit JSON `null` as the type's default. `#[serde(default)]` only
/// covers *absent* fields, so agent clients that serialize an empty list as `null`
/// (common from Python) would otherwise fail request parsing with
/// "invalid type: null, expected a sequence". Applied to the plain `Vec` request
/// fields so the proxy tolerates null lists uniformly across agents.
fn deserialize_null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Default + serde::Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

#[derive(Clone, Debug, Deserialize)]
struct RecordLogRequest {
    op: String,
    #[serde(default)]
    metaserver: String,
    #[serde(default)]
    namespace: String,
    #[serde(default)]
    table: String,
    #[serde(default)]
    key: String,
    #[serde(default)]
    field: String,
    #[serde(default)]
    value: String,
    #[serde(default)]
    storage_prefix: String,
    #[serde(default)]
    query: String,
    #[serde(default)]
    max_selected_refs: usize,
    /// The query's embedding, so ranking can happen HERE instead of in the caller.
    ///
    /// Without it this packer can only score `score_lowered_text` -- lexical substring matching
    /// over the query terms -- and it says so in its own output as `ranking_uses_vectors: false`.
    /// That is why the caller does not use it: ranking here would silently turn semantic
    /// retrieval into keyword matching, where a paraphrase scores exactly 0.0.
    ///
    /// With it, every candidate is scored against the query by cosine and only the selected refs
    /// need cross the lane. Measured on this store, a scan returns 2,954 records of which the
    /// caller keeps a few dozen, and the vectors and text of the rest are 35% of 11 MB.
    ///
    /// Absent means score lexically, exactly as before, so a caller that does not send one is
    /// unaffected.
    #[serde(default)]
    query_vector: Option<Vec<f32>>,
    /// The score a candidate must BEAT to be returned at all.
    ///
    /// The caller has been sending this on every request and the engine has never read it -- it was
    /// not even a field here, so serde dropped it silently. With dense retrieval it is the control
    /// that matters: a candidate the query embedding does not reach is not ranked low, it is not
    /// returned.
    ///
    /// Absent means 0.0, which returns everything the query can score at all and excludes only what
    /// scores exactly zero -- an opposed vector, or one this query cannot place.
    #[serde(default)]
    min_score: Option<f64>,
    /// Total token budget for the pack. 0 or absent means no budget, and the slot count decides.
    ///
    /// The caller computes this and sends it; the engine has only ever read it in test fixtures.
    /// Selection counted REFS and never summed tokens, so a pack of 24 long refs and a pack of 24
    /// short ones were treated as the same size.
    #[serde(default)]
    max_context_tokens: Option<u64>,
    /// The fewest refs each memory layer must get before the rest of the budget is filled by score.
    ///
    /// Without this every layer competes in one flat contest, so a large shared_context corpus
    /// (skills and resources) and a handful of session memories fight for the same slots and the
    /// bigger corpus wins on sheer count. A floor makes each layer's best survive to the pack, and
    /// the remaining budget still goes to whatever scores highest.
    ///
    /// Absent means no floors, which is the flat behaviour this engine has always had.
    #[serde(default)]
    layer_min_refs: Option<std::collections::BTreeMap<String, u64>>,
    /// How to combine the dense and lexical scores, and what an index hint is worth.
    ///
    /// The CALLER owns ranking policy and the engine executes it. That is deliberate: the caller
    /// already computes these weights from its own configuration -- on the one-box they are 1.00
    /// dense and 0.00 lexical, elsewhere 0.72 and 0.28 -- and an engine that hardcoded its own
    /// would silently rank differently from the caller that used to do it, which is the failure
    /// this whole change has to avoid.
    ///
    /// Absent means dense-only (1.0 / 0.0 / 0.0), which is what this engine already did.
    #[serde(default)]
    ranking_weights: Option<RankingWeights>,
    /// Send record payloads as sub-documents instead of JSON strings (see `RecordPayload`).
    /// Absent means the historical string shape, so an older reader is unaffected.
    #[serde(default)]
    records_inline_json: bool,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    entries: Vec<HashEntry>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    entries_compact: Vec<CompactHashEntry>,
    #[serde(default)]
    append_options: Value,
    #[serde(default)]
    count_key: Option<String>,
    #[serde(default)]
    record_hash_key: Option<String>,
    #[serde(default)]
    shard_size: Option<u64>,
    #[serde(default)]
    record_types: Option<Vec<String>>,
    /// Keep only records whose `status` is one of these, when the caller supplies any.
    ///
    /// A consumer that acts on a narrow set of statuses -- the pre-retrieval idle-commit flush reads
    /// three of the seven a pipeline task can carry -- otherwise receives every row of the type and
    /// discards most of them after they have been decoded. Absent or empty means no status
    /// filtering, so a caller that wants the whole type is unaffected.
    #[serde(default)]
    record_statuses: Option<Vec<String>>,
    /// Cap the scan to the newest N locations of a given record type, by append order.
    ///
    /// For a consumer that only ever looks at the tail of a type -- prior context reads the newest
    /// eight events and stops -- fetching the whole type is work whose result is discarded. Capping
    /// is per TYPE on purpose: the same scan also carries tombstones and retention cutoffs, and a
    /// cap on the union would drop the very records that make deleted memories stay deleted.
    #[serde(default)]
    newest_by_type: Option<BTreeMap<String, usize>>,
    /// Return only these top-level fields of each record.
    ///
    /// A scan returns whole records, and a caller uses a handful of fields. Measured on a
    /// production store, the average record is 673 bytes of which the TEXT is 2.4%:
    /// `storage_options` is 17.5%, `envelope` 11.9%, `scope` 10.7%, `embedding_meta` 9.6% and
    /// `vector` 4.4% -- and the packer that asks for these records reads none of those five, since
    /// it ranks lexically and reports `ranking_uses_vectors: false`. About nine tenths of every
    /// record crossing the lane is never looked at, and the gateway spends 53.5% of its CPU
    /// decoding it.
    ///
    /// The CALLER names the fields rather than the engine guessing them: the engine cannot know
    /// what a caller will read, and a projection that guesses wrong is a missing field rather than
    /// a slow response. Absent means the whole record, so nothing that does not ask is affected.
    ///
    /// Filtering happens BEFORE projection, so a scan can still filter on a field it does not
    /// return -- otherwise asking for less data would silently change which records match.
    #[serde(default)]
    record_fields: Option<Vec<String>>,
    /// Identity ids to remove, for `matrixark_delete_records`. Sent by the caller, which owns the
    /// decision about what a delete covers; the engine only matches and removes.
    #[serde(default)]
    record_ids: Option<Vec<String>>,
    #[serde(default)]
    selected_node_hashes: Option<Vec<u64>>,
    #[serde(default)]
    secondary_index_groups: Option<Vec<Vec<String>>>,
    #[serde(default)]
    scope: Option<Value>,
    #[serde(default)]
    return_index_records: bool,
    #[serde(default)]
    record: Option<Value>,
    #[serde(default, deserialize_with = "deserialize_null_default")]
    visibility_keys: Vec<String>,
    #[serde(default)]
    top_level_response: bool,
    /// Byte offset for `matrixark_resource_blob_fetch` (0 = start).
    #[serde(default)]
    blob_offset: Option<u64>,
    /// Byte count for `matrixark_resource_blob_fetch` (0/absent = to the end).
    #[serde(default)]
    blob_length: Option<u64>,
    /// Content hashes (16-digit hex) the caller's resource records still name, for
    /// `matrixark_resource_blob_sweep` -- everything else older than the age floor goes.
    #[serde(default)]
    blob_referenced_hashes: Option<Vec<String>>,
    /// Minimum age before an unreferenced blob is eligible for the sweep.
    #[serde(default)]
    blob_min_age_ms: Option<u64>,
    /// Client-chosen correlation id, echoed verbatim on the response. The serve loop answers
    /// requests strictly in order on one stdout, so a client that abandons a slow request (its
    /// own timeout) and keeps the process alive would otherwise read the ABANDONED request's
    /// late response as the answer to its next request -- every later reply shifted one back,
    /// silently serving the wrong data (observed as one scope's scan answered with another
    /// scope's records). The echo lets the client discard late responses instead of
    /// mis-attributing them.
    #[serde(default)]
    client_request_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct HashEntry {
    key: String,
    field: String,
    #[serde(default)]
    value: String,
}

#[derive(Clone, Debug, Deserialize)]
struct CompactHashEntry(String, String, String);

/// A record payload on the wire.
///
/// `Text` is the shape this lane has always used: the stored record's JSON as a STRING. That
/// costs twice. Serializing escapes every quote in the payload into a new allocation here, and
/// the reader then parses the envelope AND parses each record's string a second time -- the
/// stored bytes are already JSON, so both sides are converting JSON to JSON.
///
/// `Inline` embeds the stored bytes verbatim as a sub-document. Nothing is escaped on the way
/// out and the reader parses once. Only a caller that asked for it gets it, so a reader that
/// still expects a string keeps getting one.
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum RecordPayload {
    Inline(Box<RawValue>),
    Text(String),
}

/// Wrap a stored payload for the wire, inline when the caller asked and the bytes really are JSON.
///
/// The validation is not optional: `RawValue` is emitted VERBATIM, so handing it a value that is
/// not JSON would produce a malformed response for the whole batch rather than one bad record.
/// A payload that does not parse falls back to the string shape, which is exactly what this lane
/// did before.
fn record_payload(value: String, inline: bool) -> RecordPayload {
    if inline && serde_json::from_str::<serde::de::IgnoredAny>(&value).is_ok() {
        // The clone is a memcpy on a string we have already scanned, and it buys the caller a
        // whole parse. `from_string` can only reject invalid JSON, which the check above has
        // excluded, but falling through rather than unwrapping keeps a surprise from costing
        // the record.
        if let Ok(raw) = RawValue::from_string(value.clone()) {
            return RecordPayload::Inline(raw);
        }
    }
    RecordPayload::Text(value)
}

#[derive(Debug, Serialize)]
struct HashReadRecord {
    key: String,
    field: String,
    value: RecordPayload,
}

#[derive(Clone, Debug)]
struct CachedRetrieveCandidate {
    selected_ref: Value,
    ref_type: String,
    /// The lowercased text, built ON FIRST USE and never under a dense-only policy.
    ///
    /// This was an eager `String`. The snapshot is cached behind an `Arc` and reused across
    /// requests, so that copy was RESIDENT for the life of the cache entry, duplicating the text
    /// already in `selected_ref` -- the largest field a serving record has. It also defeated the
    /// dense-only skip: mx#1390 made the lexical SCORE lazy, but the string it scores was still
    /// built for every candidate, so the one-box default still paid for it.
    ///
    /// Searching the original text case-insensitively instead was tried and MEASURED SLOWER --
    /// 7.05 ms to 25.63 ms over 4,000 candidates, because `str::contains` is a SIMD-accelerated
    /// two-way search and a hand-rolled case-folding scan is not. Deferring the copy keeps that
    /// search and still costs nothing when it is never needed.
    lower_text: OnceLock<String>,
    /// The record's own embedding, kept so ranking can be dense.
    ///
    /// Held on the candidate rather than re-read from the record at scoring time because the
    /// snapshot outlives the records it was built from -- it is cached and reused across
    /// requests, and the records are dropped once it exists.
    vector: Option<Vec<f32>>,
}

/// What building a snapshot cost, by stage, in milliseconds.
///
/// A retrieve after a write is nearly all rebuild, and the rebuild is three unrelated pieces of
/// work: reading and parsing the shards, counting the inventory, and turning records into
/// candidates. Each is fixed by a different change, so a total is not enough to act on. A snapshot
/// served from cache reports the build it came from.
#[derive(Clone, Copy, Debug, Default)]
struct RetrieveBuildCost {
    read_ms: f64,
    inventory_ms: f64,
    candidates_ms: f64,
    /// Shards whose candidates were reused, and shards in total. Records are appended, so a write
    /// should leave every shard but the newest reusable -- this says whether it does.
    prepared_hits: usize,
    prepared_shards: usize,
    /// Which scope this snapshot's candidates were prepared for. If it moves between rebuilds, the
    /// per-shard candidate cache cannot hit however well it works.
    scope_digest: u64,
}

#[derive(Clone, Debug)]
struct RetrieveCandidateSnapshot {
    /// The candidates, as one list per shard rather than one list.
    ///
    /// A shard's candidates are cached and shared, and a candidate costs about 4.6 us to clone --
    /// concatenating them here would hand back most of what caching them saves. `first_of_segment`
    /// holds each segment's starting ordinal so a candidate can still be addressed by one number,
    /// which is what the scoring pass and the budget closures use.
    segments: Vec<Arc<Vec<CachedRetrieveCandidate>>>,
    first_of_segment: Vec<usize>,
    memory_inventory: Value,
    scanned_records: usize,
    placement_partitions_touched: usize,
    index_postings_read: usize,
    build: RetrieveBuildCost,
}

impl RetrieveCandidateSnapshot {
    fn from_segments(
        segments: Vec<Arc<Vec<CachedRetrieveCandidate>>>,
        memory_inventory: Value,
        scanned_records: usize,
        shard_count: usize,
        build: RetrieveBuildCost,
    ) -> Self {
        let mut first_of_segment = Vec::with_capacity(segments.len());
        let mut running = 0usize;
        for segment in &segments {
            first_of_segment.push(running);
            running = running.saturating_add(segment.len());
        }
        Self {
            segments,
            first_of_segment,
            memory_inventory,
            scanned_records,
            placement_partitions_touched: shard_count,
            index_postings_read: shard_count,
            build,
        }
    }

    fn candidates(&self) -> impl Iterator<Item = &CachedRetrieveCandidate> {
        self.segments.iter().flat_map(|segment| segment.iter())
    }

    fn candidate_count(&self) -> usize {
        self.segments.iter().map(|segment| segment.len()).sum()
    }

    /// How many candidates carry a vector, and how wide the widest is.
    ///
    /// The snapshot is resident for the life of its cache entry, and a 1024-dim `Vec<f32>` is
    /// 4 KiB -- so this is the difference between a snapshot costing a few megabytes and costing
    /// tens of them. Nothing reported it, which is why nobody could say.
    fn candidates_holding_vectors(&self) -> (usize, usize) {
        let mut holding = 0usize;
        let mut widest = 0usize;
        for candidate in self.candidates() {
            if let Some(vector) = candidate.vector.as_ref() {
                holding += 1;
                widest = widest.max(vector.len());
            }
        }
        (holding, widest)
    }

    /// The candidate at an ordinal across every segment.
    ///
    /// The ordinal is what the scoring pass records and what the budget closures look up later, so
    /// it has to mean the same thing whether the candidates came from one list or twenty.
    fn candidate(&self, ordinal: usize) -> Option<&CachedRetrieveCandidate> {
        let segment = match self.first_of_segment.binary_search(&ordinal) {
            Ok(exact) => exact,
            Err(0) => return None,
            Err(after) => after - 1,
        };
        let within = ordinal - self.first_of_segment[segment];
        self.segments.get(segment)?.get(within)
    }
}

struct NativeScoredCandidate {
    score: f64,
    record: Value,
    text: String,
    tokens: u64,
    context_class: String,
    session_continuity: String,
    continuity_boost_value: f64,
    cross_session_rerank_boost_value: f64,
}

fn native_query_contains_any(lower: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| lower.contains(needle))
}

fn infer_native_question_type(query: &str) -> &'static str {
    let lower = query.to_ascii_lowercase();
    if native_query_contains_any(
        &lower,
        &[
            "profile memory",
            "user profile",
            "long term memory",
            "long-term memory",
            "cross session memory",
            "cross-session memory",
            "session memory",
            "memory feature",
            "mem0",
        ],
    ) {
        return "profile_memory";
    }
    if native_query_contains_any(
        &lower,
        &[
            "benchmark",
            "workload",
            "latency",
            "p50",
            "p90",
            "p95",
            "p99",
            "throughput",
            "qps",
            "ops/s",
            "req/s",
            "hit rate",
            "read hit",
            "memory quality",
            "locomo",
            "longmemeval",
        ],
    ) {
        return "benchmark_quality";
    }
    if native_query_contains_any(
        &lower,
        &[
            "both",
            "together",
            "across",
            "between",
            "compare",
            "combine",
            "sessions",
            "multi-hop",
            "multi session",
            "multi-session",
            "cross session",
            "cross-session",
            "previous sessions",
            "other sessions",
        ],
    ) {
        return "multi_hop";
    }
    if native_query_contains_any(
        &lower,
        &[
            "what date",
            "which date",
            "yesterday",
            "tomorrow",
            "last week",
            "next week",
            "before",
            "after",
            "as of",
            "valid as of",
        ],
    ) || lower.split_whitespace().any(|term| matches!(term, "when" | "day" | "month" | "year"))
    {
        return "date";
    }
    if native_query_contains_any(
        &lower,
        &[
            "current",
            "currently",
            "latest",
            "now",
            "still",
            "today",
            "valid",
            "status",
            "preference",
            "prefer",
            "likes",
            "where does",
            "where is",
            "goal",
            "task",
            "requirement",
            "user request",
            "asked codex",
            "what did we decide",
            "what was decided",
            "who owns",
            "owner",
            "decision",
            "decided",
        ],
    ) {
        return "current_state";
    }
    if (lower.contains("assistant") || lower.contains("codex"))
        && native_query_contains_any(
            &lower,
            &[
                "decide",
                "decided",
                "decision",
                "done",
                "implemented",
                "fixed",
                "pushed",
                "push",
                "committed",
                "commit",
                "changed",
                "updated",
                "validated",
                "verified",
            ],
        )
    {
        return "current_state";
    }
    if native_query_contains_any(
        &lower,
        &["why", "reason", "because", "feel", "felt", "emotion", "happy", "sad", "angry", "worried", "excited"],
    ) {
        return "why_emotion";
    }
    if native_query_contains_any(
        &lower,
        &["overview", "summarize", "summary", "explore", "broad", "what is in", "what do we know", "topics", "map", "inventory"],
    ) {
        return "broad_exploration";
    }
    if native_query_contains_any(
        &lower,
        &["evidence", "quote", "exactly", "what did", "conversation", "dialogue", "message"],
    ) {
        return "evidence";
    }
    if native_query_contains_any(
        &lower,
        &["procedure", "step", "steps", "how to", "troubleshoot", "rollback", "runbook", "playbook", "checklist", "fix", "remediate", "mitigate"],
    ) {
        return "procedure";
    }
    "fact"
}

#[derive(Debug, Serialize)]
struct RecordLogResponse {
    ok: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    value: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    entries: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    records: Vec<HashReadRecord>,
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<usize>,
    #[serde(skip_serializing_if = "String::is_empty")]
    op: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    root: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    status: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    mode: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    append_path: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    raw_storage_backend: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    prometheus: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    cached_clients: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    elapsed_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rust_engine_time_ms: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    serialization_time_ms: Option<u128>,
    #[serde(skip_serializing_if = "String::is_empty")]
    error: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    error_code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    retryable: Option<bool>,
    /// The request's correlation id, echoed verbatim (see RecordLogRequest::client_request_id).
    #[serde(skip_serializing_if = "Option::is_none")]
    client_request_id: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, Value>,
}

#[derive(Debug)]
struct RecordLogOutput {
    value: String,
    entries: BTreeMap<String, String>,
    records: Vec<HashReadRecord>,
    count: Option<usize>,
    root: PathBuf,
    status: String,
    mode: String,
    append_path: String,
    raw_storage_backend: String,
    prometheus: String,
    cached_clients: Option<usize>,
    extra: BTreeMap<String, Value>,
}

fn default_true() -> bool {
    true
}

fn main() {
    let args: Vec<String> = env::args().collect();
    // `--serve-http <addr>`, or the env var, so a deployment can switch transport without
    // changing its command line. Checked BEFORE `--serve`, because "--serve-http" would
    // otherwise never be reached if anything ever passes both.
    if let Some(addr) = http_serve_addr(&args) {
        std::process::exit(serve_http(&addr));
    }
    if args.iter().any(|arg| arg == "--serve") {
        std::process::exit(serve());
    }
    if !single_shot_debug_enabled(&args) {
        eprintln!(
            "matrixark_rust_proxy single-shot mode is debug-only. Use --serve for MatrixArk \
             production and benchmark workloads, or set MATRIXARK_RUST_PROXY_SINGLE_SHOT_DEBUG=1 \
             / pass --debug-single-shot for diagnostics."
        );
        std::process::exit(64);
    }
    let started = Instant::now();
    let mut response = response_from_result(run(), started.elapsed().as_millis());
    println!("{}", serialize_response_with_metrics(&mut response));
    if !response.ok {
        std::process::exit(1);
    }
}

/// The HTTP listen address, from `--serve-http <addr>` or `MATRIXARK_RUST_PROXY_HTTP_ADDR`.
fn http_serve_addr(args: &[String]) -> Option<String> {
    if let Some(index) = args.iter().position(|arg| arg == "--serve-http") {
        if let Some(addr) = args.get(index + 1) {
            if !addr.trim().is_empty() && !addr.starts_with("--") {
                return Some(addr.trim().to_string());
            }
        }
    }
    std::env::var("MATRIXARK_RUST_PROXY_HTTP_ADDR")
        .ok()
        .map(|addr| addr.trim().to_string())
        .filter(|addr| !addr.is_empty())
}

fn single_shot_debug_enabled(args: &[String]) -> bool {
    args.iter().any(|arg| arg == "--debug-single-shot")
        || temporalstore_rust::env_flag::env_bool("MATRIXARK_RUST_PROXY_SINGLE_SHOT_DEBUG", false)
}

fn response_from_result(
    result: Result<(String, RecordLogOutput), (String, String)>,
    elapsed_ms: u128,
) -> RecordLogResponse {
    match result {
        Ok((op, output)) => RecordLogResponse {
            ok: true,
            value: output.value,
            entries: output.entries,
            records: output.records,
            count: output.count,
            op,
            root: output.root.display().to_string(),
            status: output.status,
            mode: output.mode,
            append_path: output.append_path,
            raw_storage_backend: output.raw_storage_backend,
            prometheus: output.prometheus,
            cached_clients: output.cached_clients,
            elapsed_ms: Some(elapsed_ms),
            rust_engine_time_ms: Some(elapsed_ms),
            serialization_time_ms: None,
            error: String::new(),
            error_code: String::new(),
            retryable: None,
            client_request_id: None,
            extra: output.extra,
        },
        Err((op, error)) => {
            let (error_code, retryable) = classify_error(&error);
            RecordLogResponse {
                ok: false,
                value: String::new(),
                entries: BTreeMap::new(),
                records: Vec::new(),
                count: None,
                op,
                root: String::new(),
                status: String::new(),
                mode: String::new(),
                append_path: String::new(),
                raw_storage_backend: String::new(),
                prometheus: String::new(),
                cached_clients: None,
                elapsed_ms: Some(elapsed_ms),
                rust_engine_time_ms: Some(elapsed_ms),
                serialization_time_ms: None,
                error,
                error_code,
                retryable: Some(retryable),
                client_request_id: None,
                extra: BTreeMap::new(),
            }
        }
    }
}

/// Render a response in the codec its request arrived in, timing the render as JSON does.
///
/// Binary falls back to JSON when it cannot encode. Dropping the reply would hang the caller on
/// its deadline, and the client tells the two apart by the first byte -- the same rule the
/// request side uses -- so a fallback is understood rather than being a second failure.
fn encode_lane_response(response: &mut RecordLogResponse, binary: bool) -> Vec<u8> {
    if binary {
        let started = Instant::now();
        if let Ok(body) = rmp_serde::to_vec_named(&*response) {
            response.serialization_time_ms = Some(started.elapsed().as_millis());
            return body;
        }
    }
    serialize_response_with_metrics(response).into_bytes()
}

fn serialize_response_with_metrics(response: &mut RecordLogResponse) -> String {
    let started = Instant::now();
    let serialized = serde_json::to_string(response)
        .unwrap_or_else(|error| json!({"ok": false, "error": error.to_string()}).to_string());
    response.serialization_time_ms = Some(started.elapsed().as_millis());
    serialized
}

fn classify_error(error: &str) -> (String, bool) {
    let lower = error.to_ascii_lowercase();
    if lower.contains("missing ")
        || lower.contains("invalid json")
        || lower.contains("unsupported op")
        || lower.contains("utf-8")
    {
        return ("invalid_argument".to_string(), false);
    }
    if lower.contains("bucket not found")
        || lower.contains("partition info not found")
        || lower.contains("partition no primary")
        || lower.contains("timed out")
        || lower.contains("timeout")
    {
        return ("temporarily_unavailable".to_string(), true);
    }
    if lower.contains("failed to create")
        || lower.contains("failed to read")
        || lower.contains("failed to serialize")
    {
        return ("internal_io_error".to_string(), true);
    }
    ("internal_error".to_string(), true)
}

fn run() -> Result<(String, RecordLogOutput), (String, String)> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input).map_err(|error| {
        (
            "unknown".to_string(),
            format!("failed to read request: {error}"),
        )
    })?;
    let request: RecordLogRequest = serde_json::from_str(&input).map_err(|error| {
        (
            "unknown".to_string(),
            format!("invalid JSON request: {error}"),
        )
    })?;
    run_request(request)
}

/// Frame marker for a binary lane response. A JSON line can never start with this byte, so a
/// reader that somehow sees the wrong codec fails loudly instead of parsing garbage.
const LANE_BINARY_MAGIC: u8 = 0xB5;

/// Is this process speaking msgpack on the lane?
///
/// Decided ONCE, from the environment the process was spawned with -- never per request. The
/// lane is a single pipe carrying a stream of responses, so a codec that changed partway would
/// leave the reader mid-frame with no way back. The spawner chooses; an older spawner sets
/// nothing and gets the JSON lines it has always got.
fn lane_binary_enabled() -> bool {
    env::var("MATRIXARK_LANE_CODEC")
        .map(|value| value.trim().eq_ignore_ascii_case("msgpack"))
        .unwrap_or(false)
}

/// Write one response in whichever codec this process speaks.
///
/// Binary frames are length-prefixed rather than delimited: a msgpack body can contain any byte,
/// including the newline the text lane uses as its terminator, so a delimiter cannot be trusted
/// here. Header is the magic byte plus a little-endian u32 length.
fn write_lane_response<W: Write>(
    out: &mut W,
    binary: bool,
    response: &RecordLogResponse,
    json_text: &str,
) {
    if binary {
        match rmp_serde::to_vec_named(response) {
            Ok(body) => {
                let mut header = [0_u8; 5];
                header[0] = LANE_BINARY_MAGIC;
                header[1..].copy_from_slice(&(body.len() as u32).to_le_bytes());
                let _ = out.write_all(&header);
                let _ = out.write_all(&body);
            }
            // Encoding a response that JSON accepted should not be possible, and dropping the
            // reply would hang the caller on its deadline. Fall back to the text line: the
            // reader can tell them apart by the first byte.
            Err(_) => {
                let _ = writeln!(out, "{json_text}");
            }
        }
    } else {
        let _ = writeln!(out, "{json_text}");
    }
    let _ = out.flush();
}

/// Bytes of request and response handled since the allocator was last asked for pages back.
fn trim_ledger() -> &'static std::sync::atomic::AtomicU64 {
    static LEDGER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    &LEDGER
}

/// Payload bytes that must pass through before a trim is worth its walk. 0 switches it off.
///
/// 8 MiB, the same figure the Python bridge used for the same job before this process replaced
/// it. `TS_MALLOC_TRIM=0` still switches the trim off underneath, so there are two levers and the
/// one nearer the allocator wins.
fn trim_threshold_bytes() -> u64 {
    std::env::var("MATRIXARK_RUST_PROXY_TRIM_BYTES")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(8 * 1024 * 1024)
}

fn note_payload_bytes(bytes: usize) {
    trim_ledger().fetch_add(bytes as u64, std::sync::atomic::Ordering::Relaxed);
}

/// Hand freed heap back to the OS, on a thread of its own.
///
/// Rust's `free` returns memory to the allocator, not to the kernel, so a process that encodes and
/// decodes whole record batches keeps every peak it has ever reached. That is not a theory here:
/// the datanode calls `release_free_heap_to_os` after each GC and sits at 385 MB while holding the
/// actual store; this proxy holds NO engine at all -- it is in remote mode, forwarding to that
/// datanode -- and reached 2,941 MB, because nothing on this side ever asked. The Python bridge
/// this process replaced had the same function for the same reason, and its comment records the
/// same 2.9 GB.
///
/// On its own thread, deliberately. `memory_trim`'s own guidance is that a trim walks the free
/// lists and does not belong on the serving path, so the request path pays one relaxed atomic add
/// and nothing else; the trim happens where no caller is waiting on it.
fn start_heap_trimmer() {
    let threshold = trim_threshold_bytes();
    if threshold == 0 {
        return;
    }
    std::thread::spawn(move || loop {
        std::thread::sleep(std::time::Duration::from_millis(2000));
        let handled = trim_ledger().swap(0, std::sync::atomic::Ordering::Relaxed);
        if handled >= threshold {
            let _ = temporalstore_rust::memory_trim::release_free_heap_to_os();
        } else {
            // Put it back rather than discarding it: a steady trickle of small requests retains
            // just as much as one large one, it only takes longer to get there. Zeroing here
            // would mean a workload of small requests never trimmed at all.
            trim_ledger().fetch_add(handled, std::sync::atomic::Ordering::Relaxed);
        }
    });
}

/// Counters shared by both serving modes.
///
/// Extracted so `--serve` and `--serve-http` cannot drift: the Prometheus output is the same
/// output whichever transport produced the requests, and a second hand-rolled copy of this
/// accounting would be wrong in a way nothing would notice.
struct ServeMetrics {
    started_at_ms: u128,
    command_count: u64,
    failed_count: u64,
    records_written: u64,
    records_read: u64,
    latency_sum_ms: u128,
    latency_max_ms: u128,
    latency_buckets: [u64; LATENCY_BUCKETS_MS.len()],
}

impl ServeMetrics {
    fn new() -> Self {
        Self {
            started_at_ms: unix_ms(),
            command_count: 0,
            failed_count: 0,
            records_written: 0,
            records_read: 0,
            latency_sum_ms: 0,
            latency_max_ms: 0,
            latency_buckets: [0; LATENCY_BUCKETS_MS.len()],
        }
    }

    fn observe(&mut self, response: &RecordLogResponse, elapsed_ms: u128) {
        self.command_count += 1;
        let observed_elapsed_ms = response.elapsed_ms.unwrap_or(elapsed_ms);
        self.latency_sum_ms += observed_elapsed_ms;
        self.latency_max_ms = self.latency_max_ms.max(observed_elapsed_ms);
        for (idx, upper_bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
            if observed_elapsed_ms <= *upper_bound {
                self.latency_buckets[idx] += 1;
            }
        }
        if !response.ok {
            self.failed_count += 1;
        }
        self.records_written += match response.op.as_str() {
            "put_string" | "hset" => 1,
            "batch_hset" | "matrixark_append_records" | "matrixark_batch_append_records" => {
                response.count.unwrap_or(0) as u64
            }
            _ => 0,
        };
        self.records_read += match response.op.as_str() {
            "get_string" | "hget" => 1,
            "batch_hget" | "hgetall" | "scan_hash" => response.count.unwrap_or(0) as u64,
            _ => 0,
        };
    }

    fn render_prometheus(&self) -> String {
        render_prometheus_metrics(
            self.started_at_ms,
            self.command_count,
            self.failed_count,
            self.records_written,
            self.records_read,
            self.latency_sum_ms,
            self.latency_max_ms,
            &self.latency_buckets,
            cached_engine_count(),
        )
    }
}

/// Whether HTTP requests may run at the same time.
///
/// OFF by default, which reproduces exactly the serialization the pipe had: `--serve` handled one
/// request at a time, and the daemon in front of it held a single lock over a single process, so
/// nothing in this engine has ever had two requests in flight. Removing the middleman is a
/// transport change and is worth having on its own; letting requests overlap is a concurrency
/// change and belongs behind its own switch so it can be measured, and reverted, separately.
fn http_concurrent_enabled() -> bool {
    temporalstore_rust::env_flag::env_bool("MATRIXARK_RUST_PROXY_HTTP_CONCURRENT", false)
}

/// Serve the same requests over HTTP instead of over a pipe.
///
/// `--serve` reads one request per line from stdin, so something has to own that pipe. In the
/// one-box that owner was a Python daemon: it accepted a unix socket, parsed the request,
/// re-serialised it onto this process's stdin, read the answer back and re-serialised that too --
/// four full JSON operations per request, on the largest payloads in the system, to move bytes
/// between two file descriptors. Measured over 1,215 s of production-corpus soak that cost 21.9%
/// of a core and 72 MB of RSS, and it added a process to a box that was already OOM-killing.
///
/// The wire format is deliberately unchanged: the request body is the same `RecordLogRequest`
/// JSON the pipe carried and the reply is the same `RecordLogResponse`, so a client changes only
/// WHERE it sends, never what it sends -- which is what makes this swappable under a running
/// gateway and comparable in an A/B.
/// Whether a body is a msgpack map rather than JSON text.
///
/// The first byte settles it and cannot be ambiguous: a msgpack map begins with a fixmap
/// (0x80-0x8f), map16 (0xde) or map32 (0xdf), and a JSON object begins with `{` (0x7b) or
/// whitespace. This is the same way the stdio lane distinguishes its two framings, and it means
/// the codec needs no header, no negotiation and no shared state -- a request carries its own
/// answer, so a client and a proxy that disagree about the setting still understand each other.
fn looks_like_msgpack_map(body: &[u8]) -> bool {
    matches!(body.first(), Some(&first) if (0x80..=0x8f).contains(&first) || first == 0xde || first == 0xdf)
}

/// Decode a lane request in whichever codec it arrived in.
///
/// Returns the request and whether it was binary, because the reply must go back in the SAME
/// codec: the caller decodes what it sent, and answering JSON to a msgpack request would be
/// understood by nobody.
fn decode_lane_request(body: &[u8]) -> (Result<RecordLogRequest, String>, bool) {
    if looks_like_msgpack_map(body) {
        return (
            rmp_serde::from_slice::<RecordLogRequest>(body)
                .map_err(|error| format!("invalid msgpack request: {error}")),
            true,
        );
    }
    (
        serde_json::from_slice::<RecordLogRequest>(body)
            .map_err(|error| format!("invalid JSON request: {error}")),
        false,
    )
}

fn serve_http(addr: &str) -> i32 {
    let metrics = Arc::new(Mutex::new(ServeMetrics::new()));
    let gate = Arc::new(Mutex::new(()));
    let concurrent = http_concurrent_enabled();
    start_heap_trimmer();
    eprintln!("matrixark_rust_proxy serving http on {addr} (concurrent={concurrent})");
    let result = temporalstore_rust::http::serve(addr, move |http_request| {
        let started = Instant::now();
        let (parsed, binary_lane) = decode_lane_request(&http_request.body);
        let client_request_id = parsed
            .as_ref()
            .ok()
            .and_then(|request| request.client_request_id.clone());
        let result = match parsed {
            Ok(request) if request.op == "metrics_prometheus" => {
                let rendered = match metrics.lock() {
                    Ok(metrics) => metrics.render_prometheus(),
                    Err(_) => String::new(),
                };
                Ok((
                    "metrics_prometheus".to_string(),
                    RecordLogOutput {
                        prometheus: rendered,
                        mode: matrixark_rust_service_mode().to_string(),
                        cached_clients: Some(cached_engine_count()),
                        ..empty_output(PathBuf::new())
                    },
                ))
            }
            Ok(request) => {
                // The guard lives exactly as long as the call it serializes. Taking it around
                // the JSON work as well would serialize parsing too, for no safety gained.
                let _guard = if concurrent {
                    None
                } else {
                    Some(gate.lock().unwrap_or_else(|poisoned| poisoned.into_inner()))
                };
                run_request(request)
            }
            Err(error) => Err(("unknown".to_string(), error)),
        };
        let elapsed_ms = started.elapsed().as_millis();
        let mut response = response_from_result(result, elapsed_ms);
        response.client_request_id = client_request_id;
        let body = encode_lane_response(&mut response, binary_lane);
        if let Ok(mut metrics) = metrics.lock() {
            metrics.observe(&response, elapsed_ms);
        }
        note_payload_bytes(http_request.body.len() + body.len());
        // 200 even for an application-level failure: `ok` in the body is the contract the pipe
        // had, and a client that switched transports must not start seeing transport errors for
        // the same answers.
        (200, body)
    });
    match result {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("matrixark_rust_proxy http serve failed: {error}");
            1
        }
    }
}

fn serve() -> i32 {
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    let lane_binary = lane_binary_enabled();
    let mut metrics = ServeMetrics::new();
    start_heap_trimmer();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(value) => value,
            Err(error) => {
                metrics.failed_count += 1;
                let response = response_from_result(
                    Err((
                        "unknown".to_string(),
                        format!("failed to read request: {error}"),
                    )),
                    0,
                );
                let _ = writeln!(
                    stdout,
                    "{}",
                    serde_json::to_string(&response).expect("record-log response should serialize")
                );
                let _ = stdout.flush();
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        let started = Instant::now();
        let request: Result<RecordLogRequest, _> = serde_json::from_str(&line);
        // Captured before the request moves into its handler; echoed on EVERY response line
        // (including shutdown), so the client can match responses to requests and discard the
        // late answer of a request it abandoned instead of shifting every later reply back one.
        let client_request_id = request
            .as_ref()
            .ok()
            .and_then(|request| request.client_request_id.clone());
        let result = match request {
            Ok(request) if request.op == "shutdown" => {
                let cached_clients = cached_engine_count();
                clear_engine_cache();
                let output = RecordLogOutput {
                    status: "shutting_down".to_string(),
                    mode: matrixark_rust_service_mode().to_string(),
                    cached_clients: Some(cached_clients),
                    ..empty_output(PathBuf::new())
                };
                let mut response = response_from_result(
                    Ok(("shutdown".to_string(), output)),
                    started.elapsed().as_millis(),
                );
                response.client_request_id = client_request_id;
                let json_text = serialize_response_with_metrics(&mut response);
                write_lane_response(&mut stdout, lane_binary, &response, &json_text);
                return 0;
            }
            Ok(request) if request.op == "metrics_prometheus" => {
                let output = RecordLogOutput {
                    prometheus: metrics.render_prometheus(),
                    mode: matrixark_rust_service_mode().to_string(),
                    cached_clients: Some(cached_engine_count()),
                    ..empty_output(PathBuf::new())
                };
                Ok(("metrics_prometheus".to_string(), output))
            }
            Ok(request) => run_request(request),
            Err(error) => Err((
                "unknown".to_string(),
                format!("invalid JSON request: {error}"),
            )),
        };
        let elapsed_ms = started.elapsed().as_millis();
        let mut response = response_from_result(result, elapsed_ms);
        response.client_request_id = client_request_id;
        let response_json = serialize_response_with_metrics(&mut response);
        metrics.observe(&response, elapsed_ms);
        note_payload_bytes(line.len() + response_json.len());
        write_lane_response(&mut stdout, lane_binary, &response, &response_json);
    }
    0
}

fn run_request(request: RecordLogRequest) -> Result<(String, RecordLogOutput), (String, String)> {
    let op = request.op.clone();
    validate_request(&request).map_err(|error| (op.clone(), error))?;
    if matches!(request.op.as_str(), "health" | "readiness" | "preflight") {
        return Ok((
            op,
            RecordLogOutput {
                value: "ready".to_string(),
                count: Some(0),
                root: record_log_root(&request),
                status: "ready".to_string(),
                mode: matrixark_rust_service_mode().to_string(),
                cached_clients: Some(cached_engine_count()),
                ..empty_output(PathBuf::new())
            },
        ));
    }
    let root = record_log_root(&request);
    let engine = open_engine(&request).map_err(|error| (op.clone(), error))?;
    let mut output =
        execute_record_log_request(&engine, request, root).map_err(|error| (op.clone(), error))?;
    output.cached_clients = Some(cached_engine_count());
    Ok((op, output))
}

/// Whether this binary is the direct SDK bridge.
///
/// Each bin that includes this file declares `DIRECT_SDK_BRIDGE`, so the answer is settled when
/// the binary is built. It used to be asked of `MATRIXARK_RUST_SDK_MODE` and of a substring of
/// argv[0]; the variable let a proxy process report itself as the bridge, and argv[0] is
/// whatever the file was last renamed to. Neither can contradict the binary now, and a new bin
/// that forgets to declare it does not compile.
fn matrixark_rust_sdk_mode_is_direct() -> bool {
    DIRECT_SDK_BRIDGE
}

fn matrixark_rust_storage_mode() -> &'static str {
    if matrixark_rust_sdk_mode_is_direct() {
        "rust-direct-sdk-bridge"
    } else {
        "rust-proxy"
    }
}

fn matrixark_rust_service_mode() -> &'static str {
    if matrixark_rust_sdk_mode_is_direct() {
        "long_lived_rust_direct_sdk_bridge"
    } else {
        "rust_proxy_stdio"
    }
}

fn render_prometheus_metrics(
    started_at_ms: u128,
    command_count: u64,
    failed_count: u64,
    records_written: u64,
    records_read: u64,
    latency_sum_ms: u128,
    latency_max_ms: u128,
    latency_buckets: &[u64; LATENCY_BUCKETS_MS.len()],
    cached_clients: usize,
) -> String {
    let uptime_seconds = ((unix_ms().saturating_sub(started_at_ms)) as f64 / 1000.0).max(0.001);
    let qps = command_count as f64 / uptime_seconds;
    let storage_mode = matrixark_rust_storage_mode();
    let mut output = format!(
        concat!(
            "# HELP matrixark_rust_proxy_process_start_time_ms Unix millisecond timestamp when this Rust proxy process started.\n",
            "# TYPE matrixark_rust_proxy_process_start_time_ms gauge\n",
            "matrixark_rust_proxy_process_start_time_ms {}\n",
            "# HELP matrixark_rust_proxy_commands_total Total MatrixArk Rust proxy commands.\n",
            "# TYPE matrixark_rust_proxy_commands_total counter\n",
            "matrixark_rust_proxy_commands_total {}\n",
            "# HELP matrixark_rust_proxy_commands_failed_total Total failed MatrixArk Rust proxy commands.\n",
            "# TYPE matrixark_rust_proxy_commands_failed_total counter\n",
            "matrixark_rust_proxy_commands_failed_total {}\n",
            "# HELP matrixark_rust_proxy_records_written_total Total MatrixArk records/hash entries written by the Rust proxy bridge.\n",
            "# TYPE matrixark_rust_proxy_records_written_total counter\n",
            "matrixark_rust_proxy_records_written_total {}\n",
            "# HELP matrixark_rust_proxy_records_read_total Total MatrixArk records/hash entries read by the Rust proxy bridge.\n",
            "# TYPE matrixark_rust_proxy_records_read_total counter\n",
            "matrixark_rust_proxy_records_read_total {}\n",
            "# HELP matrixark_rust_proxy_qps Current process-lifetime average command QPS.\n",
            "# TYPE matrixark_rust_proxy_qps gauge\n",
            "matrixark_rust_proxy_qps {:.6}\n",
            "# HELP matrixark_backend_qps Backend-normalized process-lifetime average command QPS.\n",
            "# TYPE matrixark_backend_qps gauge\n",
            "matrixark_backend_qps{{backend=\"rust\"}} {:.6}\n",
            "# HELP matrixark_backend_commands_total Backend-normalized total commands.\n",
            "# TYPE matrixark_backend_commands_total counter\n",
            "matrixark_backend_commands_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_backend_errors_total Backend-normalized failed commands.\n",
            "# TYPE matrixark_backend_errors_total counter\n",
            "matrixark_backend_errors_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_backend_timeouts_total Backend-normalized timeout count.\n",
            "# TYPE matrixark_backend_timeouts_total counter\n",
            "matrixark_backend_timeouts_total{{backend=\"rust\"}} 0\n",
            "# HELP matrixark_backend_info MatrixArk backend identity and storage mode.\n",
            "# TYPE matrixark_backend_info gauge\n",
            "matrixark_backend_info{{backend=\"rust\",storage_mode=\"{}\"}} 1\n",
            "# HELP matrixark_backend_ready MatrixArk backend readiness state, 1 for ready and 0 for not ready.\n",
            "# TYPE matrixark_backend_ready gauge\n",
            "matrixark_backend_ready{{backend=\"rust\",storage_mode=\"{}\",status=\"ready\"}} 1\n",
            "# HELP matrixark_backend_records_written_total Backend-normalized records/hash entries written.\n",
            "# TYPE matrixark_backend_records_written_total counter\n",
            "matrixark_backend_records_written_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_backend_records_read_total Backend-normalized records/hash entries read.\n",
            "# TYPE matrixark_backend_records_read_total counter\n",
            "matrixark_backend_records_read_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_context_records_total MatrixArk context record count observed by backend.\n",
            "# TYPE matrixark_context_records_total gauge\n",
            "matrixark_context_records_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_backend_audit_buffered_records MatrixArk buffered audit records awaiting flush.\n",
            "# TYPE matrixark_backend_audit_buffered_records gauge\n",
            "matrixark_backend_audit_buffered_records{{backend=\"rust\"}} 0\n",
            "# HELP matrixark_backend_audit_flush_failures_total MatrixArk audit flush failure count.\n",
            "# TYPE matrixark_backend_audit_flush_failures_total counter\n",
            "matrixark_backend_audit_flush_failures_total{{backend=\"rust\"}} 0\n",
            "# HELP matrixark_rust_proxy_cached_clients Cached TemporalEngine clients in the long-lived Rust gateway.\n",
            "# TYPE matrixark_rust_proxy_cached_clients gauge\n",
            "matrixark_rust_proxy_cached_clients {}\n",
            "# HELP matrixark_rust_proxy_clients_created_total TemporalEngine clients created by the long-lived Rust proxy.\n",
            "# TYPE matrixark_rust_proxy_clients_created_total counter\n",
            "matrixark_rust_proxy_clients_created_total {}\n",
            "# HELP matrixark_backend_cached_clients Backend-normalized cached client/connection count.\n",
            "# TYPE matrixark_backend_cached_clients gauge\n",
            "matrixark_backend_cached_clients{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_backend_records_written_total Backend-normalized MatrixArk records written.\n",
            "# TYPE matrixark_backend_records_written_total counter\n",
            "matrixark_backend_records_written_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_backend_records_read_total Backend-normalized MatrixArk records read.\n",
            "# TYPE matrixark_backend_records_read_total counter\n",
            "matrixark_backend_records_read_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_context_records_total MatrixArk context records visible through the backend adapter.\n",
            "# TYPE matrixark_context_records_total gauge\n",
            "matrixark_context_records_total{{backend=\"rust\"}} {}\n",
            "# HELP matrixark_backend_audit_buffered_records Backend-normalized buffered audit records.\n",
            "# TYPE matrixark_backend_audit_buffered_records gauge\n",
            "matrixark_backend_audit_buffered_records{{backend=\"rust\"}} 0\n",
            "# HELP matrixark_backend_audit_flush_failures_total Backend-normalized audit flush failures.\n",
            "# TYPE matrixark_backend_audit_flush_failures_total counter\n",
            "matrixark_backend_audit_flush_failures_total{{backend=\"rust\"}} 0\n",
            "# HELP matrixark_rust_proxy_command_latency_ms Command latency histogram in milliseconds.\n",
            "# TYPE matrixark_rust_proxy_command_latency_ms histogram\n",
            "# HELP matrixark_backend_command_latency_ms Backend-normalized command latency histogram in milliseconds.\n",
            "# TYPE matrixark_backend_command_latency_ms histogram\n"
        ),
        started_at_ms,
        command_count,
        failed_count,
        records_written,
        records_read,
        qps,
        qps,
        command_count,
        failed_count,
        storage_mode,
        storage_mode,
        records_written,
        records_read,
        records_written,
        cached_clients,
        cached_clients,
        cached_clients,
        records_written,
        records_read,
        records_written.saturating_add(records_read)
    );
    output.push_str("# HELP matrixark_backend_command_latency_ms Backend-normalized command latency quantiles in milliseconds.\n");
    output.push_str("# TYPE matrixark_backend_command_latency_ms gauge\n");
    for (quantile, value) in [
        (
            "0.50",
            bucket_quantile(latency_buckets, command_count, 0.50),
        ),
        (
            "0.95",
            bucket_quantile(latency_buckets, command_count, 0.95),
        ),
        (
            "0.99",
            bucket_quantile(latency_buckets, command_count, 0.99),
        ),
    ] {
        output.push_str(&format!(
            "matrixark_backend_command_latency_ms{{backend=\"rust\",quantile=\"{}\"}} {}\n",
            quantile, value
        ));
    }
    for (idx, upper_bound) in LATENCY_BUCKETS_MS.iter().enumerate() {
        output.push_str(&format!(
            "matrixark_rust_proxy_command_latency_ms_bucket{{le=\"{}\"}} {}\n",
            upper_bound, latency_buckets[idx]
        ));
        output.push_str(&format!(
            "matrixark_backend_command_latency_ms_bucket{{backend=\"rust\",le=\"{}\"}} {}\n",
            upper_bound, latency_buckets[idx]
        ));
    }
    output.push_str(&format!(
        "matrixark_rust_proxy_command_latency_ms_bucket{{le=\"+Inf\"}} {}\n",
        command_count
    ));
    output.push_str(&format!(
        "matrixark_backend_command_latency_ms_bucket{{backend=\"rust\",le=\"+Inf\"}} {}\n",
        command_count
    ));
    output.push_str(&format!(
        "matrixark_rust_proxy_command_latency_ms_sum {}\n",
        latency_sum_ms
    ));
    output.push_str(&format!(
        "matrixark_backend_command_latency_ms_sum{{backend=\"rust\"}} {}\n",
        latency_sum_ms
    ));
    output.push_str(&format!(
        "matrixark_rust_proxy_command_latency_ms_count {}\n",
        command_count
    ));
    output.push_str(&format!(
        "matrixark_backend_command_latency_ms_count{{backend=\"rust\"}} {}\n",
        command_count
    ));
    output.push_str(&format!(
        "# HELP matrixark_rust_proxy_command_latency_max_ms Max observed command latency in milliseconds.\n\
         # TYPE matrixark_rust_proxy_command_latency_max_ms gauge\n\
         matrixark_rust_proxy_command_latency_max_ms {}\n\
         # HELP matrixark_backend_command_latency_max_ms Backend-normalized max observed command latency in milliseconds.\n\
         # TYPE matrixark_backend_command_latency_max_ms gauge\n\
         matrixark_backend_command_latency_max_ms{{backend=\"rust\"}} {}\n",
        latency_max_ms, latency_max_ms
    ));
    // The engine's own series, which include the page-cache counters. Appended rather than
    // re-rendered: see engine_prometheus_metrics.
    output.push_str(&serving_cache_prometheus_metrics());
    output.push_str(&engine_prometheus_metrics());
    output
}

fn bucket_quantile(
    latency_buckets: &[u64; LATENCY_BUCKETS_MS.len()],
    total: u64,
    quantile: f64,
) -> u128 {
    if total == 0 {
        return 0;
    }
    let target = ((total as f64) * quantile).ceil().max(1.0) as u64;
    let mut previous = 0;
    for (idx, count) in latency_buckets.iter().enumerate() {
        if *count >= target {
            return LATENCY_BUCKETS_MS[idx];
        }
        previous = LATENCY_BUCKETS_MS[idx];
    }
    previous
}

fn unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn required_option(value: Option<String>, name: &str) -> Result<String, String> {
    value
        .filter(|item| !item.is_empty())
        .ok_or_else(|| format!("missing {name}"))
}

include!("matrixark_rust_proxy_impl/serving_caches.rs");


fn retrieve_candidate_cache_key(
    storage_prefix: &str,
    count: usize,
    scope: Option<&Value>,
    secondary_groups: &[Vec<String>],
    keep_vectors: bool,
) -> String {
    let scope_key = scope
        .and_then(|value| serde_json::to_string(value).ok())
        .unwrap_or_default();
    let secondary_key = serde_json::to_string(secondary_groups).unwrap_or_default();
    // `keep_vectors` is part of the key, not a detail of the value: a lexical snapshot and a
    // dense one hold different candidates, and handing a dense request the lexical snapshot would
    // silently rank it without vectors -- the one failure this cannot be allowed to have.
    let vectors = if keep_vectors { "v" } else { "-" };
    format!("{storage_prefix}:candidate_snapshot:{count}:{scope_key}:{secondary_key}:{vectors}")
}

fn storage_prefix_from_key(key: &str) -> Option<String> {
    if let Some(prefix) = key.strip_suffix(":record_count") {
        return Some(prefix.to_string());
    }
    key.split_once(":records:")
        .map(|(prefix, _)| prefix.to_string())
}

fn storage_prefix_from_request(request: &RecordLogRequest) -> String {
    if !request.storage_prefix.trim().is_empty() {
        return request.storage_prefix.trim().to_string();
    }
    request
        .count_key
        .as_deref()
        .and_then(storage_prefix_from_key)
        .unwrap_or_default()
}

fn invalidate_retrieve_candidate_cache(storage_prefix: &str) {
    if storage_prefix.trim().is_empty() {
        return;
    }
    let prefix = format!("{storage_prefix}:candidate_snapshot:");
    if let Ok(mut cache) = retrieve_candidate_cache().lock() {
        cache.retain(|key, _| !key.starts_with(&prefix));
    }
}

fn invalidate_retrieve_candidate_cache_for_keys<'a>(keys: impl IntoIterator<Item = &'a String>) {
    let prefixes = keys
        .into_iter()
        .filter_map(|key| storage_prefix_from_key(key))
        .collect::<HashSet<_>>();
    invalidate_retrieve_candidate_cache_for_prefixes(prefixes);
}

fn invalidate_retrieve_candidate_cache_for_prefixes(prefixes: HashSet<String>) {
    for prefix in prefixes {
        invalidate_retrieve_candidate_cache(&prefix);
    }
}

/// Per-type append version. Changes when records OF THAT TYPE are appended, and not otherwise.
/// Keep only the named top-level fields of a record.
///
/// `None` returns the record untouched. A record that is not an object is returned untouched too:
/// projecting one would mean inventing a shape the caller did not ask for.
///
/// Nested paths are deliberately not supported. Every field a caller was measured to need is
/// top-level, and a path syntax would be a second query language to get wrong in a place where
/// being wrong means a silently missing field.
fn project_record(record: Value, fields: Option<&std::collections::BTreeSet<String>>) -> Value {
    let Some(fields) = fields else {
        return record;
    };
    let Value::Object(map) = record else {
        return record;
    };
    Value::Object(
        map.into_iter()
            .filter(|(key, _)| fields.contains(key))
            .collect(),
    )
}

fn type_version_key(record_hash_key: &str, record_type: &str) -> String {
    format!("{record_hash_key}:type_version:{record_type}")
}

/// Per-base delete epoch, bumped by any removal.
///
/// Deletes get an epoch rather than a per-type version because of a hole that a per-type version
/// alone cannot close: removing the last records of a type whose version key does not exist yet
/// would leave the token unchanged, and the next scan would serve an answer that still contained
/// them. An epoch is coarse -- it invalidates every scan for the base -- which is the right trade
/// exactly because deletes are rare and appends are not.
fn scan_delete_epoch_key(record_hash_key: &str) -> String {
    format!("{record_hash_key}:scan_delete_epoch")
}

/// The token that decides whether a cached scan is still the right answer.
///
/// It used to be the storage prefix's TOTAL record count, which is correct but maximally coarse:
/// every append changed it, so every scan of every type missed. Measured on a production-corpus
/// soak that is 229 records written per user message, so the hit rate under ingest was
/// effectively zero and each miss re-fetched the whole corpus of the requested types --
/// `matrixark_scan_candidates` was 87% of all time spent inside lane calls.
///
/// A scan that names its types can only be changed by those types, so it is keyed on their
/// versions plus the delete epoch. A scan that names no types could be changed by anything, so it
/// keeps the total count. Narrowing WHEN a cached answer is reused, never WHAT a scan returns.
fn scan_freshness_token(
    engine: &RecordStore,
    command: &RecordLogRequest,
    record_hash_key: &str,
    count: u64,
) -> String {
    let Some(types) = command.record_types.as_ref().filter(|types| !types.is_empty()) else {
        return format!("count:{count}");
    };
    let epoch = read_record_count(engine, &scan_delete_epoch_key(record_hash_key))
        .unwrap_or_default();
    let mut parts = Vec::with_capacity(types.len());
    for record_type in types {
        // A missing key is a DISTINCT token, not a zero: a type with no records yet reads as
        // absent, and its first append writes a version, so the token changes and the cached
        // empty answer is dropped. Reading it as 0 would collide with a real version of 0.
        let version = match read_record_count(engine, &type_version_key(record_hash_key, record_type)) {
            Ok(value) if !value.trim().is_empty() => value.trim().to_string(),
            _ => "-".to_string(),
        };
        parts.push(format!("{record_type}={version}"));
    }
    parts.sort();
    format!("t:{}|d:{}", parts.join(","), epoch.trim())
}

fn matrixark_scan_cache_key(command: &RecordLogRequest, freshness: &str) -> String {
    serde_json::to_string(&json!({
        "count_key": command.count_key,
        "record_hash_key": command.record_hash_key,
        "shard_size": command.shard_size.unwrap_or(DEFAULT_RECORD_LOG_SHARD_SIZE).max(1),
        "freshness": freshness,
        "record_types": command.record_types,
        // A status-filtered scan is a SUBSET of the same question, exactly like
        // `newest_by_type` below -- sharing an entry would serve the subset to a
        // caller that asked for the whole type.
        "record_statuses": command.record_statuses,
        "record_ids": command.record_ids,
        "selected_node_hashes": command.selected_node_hashes,
        "secondary_index_groups": command.secondary_index_groups,
        "scope": command.scope,
        "return_index_records": command.return_index_records,
        // A capped scan and an uncapped one are different answers to the same question, so they
        // must not share a cache entry: the capped answer is a SUBSET.
        "newest_by_type": command.newest_by_type,
        // Same rule for a projection: a caller that asked for five fields must never be served
        // the cached whole-record answer, and -- far worse -- a caller that asked for everything
        // must never be served a projected one, which would look like data loss.
        "record_fields": command.record_fields,
    }))
    .unwrap_or_else(|_| format!("fallback:{freshness}"))
}

/// Stamp a cached scan result as a hit.
///
/// `cache_entries` is passed in rather than read here on purpose: the only caller is already
/// holding the scan-cache guard when it hands us the cached value, and `std::sync::Mutex` is not
/// reentrant, so locking again here blocks forever on a guard this very thread owns. That is what
/// used to happen on every cache hit.
fn mark_scan_cache_hit(mut value: Value, cache_entries: usize) -> Value {
    if let Some(object) = value.as_object_mut() {
        object.insert("cache_hit".to_string(), json!(true));
        if let Some(stats) = object.get_mut("scan_stats").and_then(Value::as_object_mut) {
            stats.insert("candidate_cache_hit".to_string(), json!(true));
            stats.insert("cache_hit".to_string(), json!(true));
            stats.insert("candidate_cache_scope".to_string(), json!("process_global"));
            stats.insert("native_placement_candidate_cache_hit".to_string(), json!(true));
            stats.insert("native_placement_candidate_cache_entries".to_string(), json!(cache_entries));
            stats.insert("native_candidate_cache_key_shape".to_string(), json!("storage_prefix+count+scope+record_types+selected_node_hashes+secondary_index_groups+return_index_records"));
            stats.insert("native_candidate_cache_payload".to_string(), json!("compact_struct"));
            stats.insert("serving_memory_cache_layer".to_string(), json!("rust_proxy_scan_cache"));
            stats.insert("serving_memory_promoted".to_string(), json!(true));
        }
    }
    value
}

fn invalidate_hgetall_snapshot(key: &str) {
    if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
        cache.remove(key);
    }
}

fn hgetall_snapshot_contains(key: &str) -> bool {
    hgetall_snapshot_cache()
        .lock()
        .map(|cache| cache.contains_key(key))
        .unwrap_or(false)
}

/// Drop named fields from a cached shard snapshot, keeping the rest of it.
///
/// The write side has always patched snapshots in place; deletes dropped the whole snapshot for
/// the key, which is the same as discarding a shard's worth of reads because one field went away.
/// It showed up as `update` being far slower than `add` on the same subject: an update purges the
/// superseded version, and every index and record snapshot that purge touched had to be rebuilt
/// cold by the very next scan -- one page read per member. A `HashDelete` names ONE field, so the
/// exact post-delete snapshot is the snapshot minus that field.
fn remove_hgetall_snapshot_fields(key: &str, fields: &[String]) {
    if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
        cache.patch(key, |snapshot| {
            for field in fields {
                snapshot.remove(field);
            }
            true
        });
        // Deleting the LAST field of a hash removes the whole key in the engine, and a cached
        // empty map for a key that no longer exists is the shape that once pinned a served view
        // at zero rows until restart. `hgetall_map` refuses to cache an empty read for the same
        // reason; a removal must not create through the back door what the read path declines to
        // store. Drop the snapshot instead and let the next read decide.
    }
}

fn update_hgetall_snapshot_fields(key: &str, entries: &[(String, Vec<u8>)]) {
    if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
        cache.patch(key, |snapshot| {
            for (field, value) in entries {
                let Ok(text) = String::from_utf8(value.clone()) else {
                    // A non-UTF-8 value means the snapshot can no longer represent the hash,
                    // so the whole key goes.
                    return false;
                };
                snapshot.insert(field.clone(), text);
            }
            true
        });
    }
}

/// Fetch the payload values at `locations` ("{shard:06}:{field}") through the shard hash
/// snapshots, in append order (lexical location order = shard, then zero-padded field).
///
/// Reads whole shard hashes via hgetall_map on purpose: HashMultiGet re-reads each field's page
/// uncached on every call (measured ~0.5 ms/field, never warming), while the snapshot is read
/// once and then kept current by the write runtimes -- the same coherence the shard walk has
/// always relied on. A location whose field is missing or empty is a stale index entry: the
/// record was physically removed after its entry was written, and skipping it is the contract.
fn fetch_indexed_payload_values(
    engine: &RecordStore,
    record_hash_key: &str,
    locations: &std::collections::BTreeSet<String>,
) -> Result<(Vec<String>, u64), String> {
    let mut fields_by_shard: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for location in locations {
        if let Some((shard, field)) = location.split_once(':') {
            fields_by_shard
                .entry(shard.to_string())
                .or_default()
                .push(field.to_string());
        }
    }
    let shards_touched = fields_by_shard.len() as u64;
    let mut values = Vec::with_capacity(locations.len());
    for (shard, fields) in fields_by_shard {
        let snapshot = hgetall_shared(engine, format!("{record_hash_key}:{shard}"))?;
        for field in fields {
            match snapshot.get(&field) {
                Some(value) if !value.is_empty() => values.push(value.clone()),
                _ => {}
            }
        }
    }
    Ok((values, shards_touched))
}

/// Read named fields of one record shard, without paying for the rest of it.
///
/// Deliberately NOT used by the sweep paths. There, whole-shard reads are the right call and the
/// comment on `fetch_indexed_payload_values` explains why: a named-field read costs about half a
/// millisecond per field and never warms, while a shard snapshot is read once and then kept
/// current by the write runtimes, so a repeated sweep pays nothing after the first. Narrowing
/// those measurably made them slower.
///
/// The purge is the opposite case, and measurably so. It names a handful of fields the locator
/// already identified, and it MUTATES those shards immediately after, which invalidates any
/// snapshot it would have populated -- so the snapshot is pure cost. Measured deleting an
/// identical freshly-created memory (same closure: 4 ids, 96 records scanned, 5 fields rewritten)
/// on two stores: 41.7 ms against a 20 MB store, 385.7 ms against a 249 MB one. Identical work,
/// nine times the time, because a shard on the larger store is full and the whole thing was being
/// decoded to reach five fields.
fn fetch_shard_fields(
    engine: &RecordStore,
    key: String,
    fields: &[String],
) -> Result<BTreeMap<String, String>, String> {
    if fields.is_empty() {
        return Ok(BTreeMap::new());
    }
    // An already-cached snapshot is free and current, so prefer it when one happens to be in hand.
    if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
        if let Some(cached) = cache.get(&key) {
            let mut subset = BTreeMap::new();
            for field in fields {
                if let Some(value) = cached.get(field) {
                    subset.insert(field.clone(), value.clone());
                }
            }
            return Ok(subset);
        }
    }
    let response = engine.execute_durable(ExecuteRequest {
        shard_id: DEFAULT_SHARD_ID,
        command: Command::HashMultiGet {
            key,
            fields: fields.to_vec(),
        },
    });
    if !response.status.ok {
        return Err(format!(
            "{}: {}",
            response.status.code, response.status.message
        ));
    }
    match response.response {
        CommandResponse::Values { values } => {
            let mut decoded = BTreeMap::new();
            for (field, value) in fields.iter().zip(values.into_iter()) {
                let Some(bytes) = value else { continue };
                let text = String::from_utf8(bytes)
                    .map_err(|error| format!("stored value is not UTF-8: {error}"))?;
                if !text.is_empty() {
                    decoded.insert(field.clone(), text);
                }
            }
            Ok(decoded)
        }
        other => Err(format!("unexpected response for hmget: {other:?}")),
    }
}

/// An OWNED snapshot, for callers that consume or mutate what they get.
///
/// Prefer `hgetall_shared` wherever the caller only reads: this one exists to copy.
/// A shard's records, parsed once and kept beside the payloads they came from.
///
/// The rebuild that calls this re-reads every shard on every write, and parsing dominates it --
/// 429 us per record against 23 us to copy one. Records are appended, so every shard but the
/// newest parses to exactly what it parsed last time.
///
/// An uncached shard (an empty read is deliberately not cached) simply parses every time, which is
/// what it did before.
fn hgetall_decoded(engine: &RecordStore, key: String) -> Result<Arc<Vec<Value>>, String> {
    if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
        if let Some(decoded) = cache.decoded(&key) {
            return Ok(decoded);
        }
    }
    let payloads = hgetall_shared(engine, key.clone())?;
    let mut records = Vec::new();
    for payload in payloads.values() {
        if payload.trim().is_empty() {
            continue;
        }
        flatten_context_payload(payload, &mut records);
    }
    let records = Arc::new(records);
    if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
        cache.set_decoded(&key, Arc::clone(&records));
    }
    Ok(records)
}

fn hgetall_map(engine: &RecordStore, key: String) -> Result<BTreeMap<String, String>, String> {
    Ok((*hgetall_shared(engine, key)?).clone())
}

fn hgetall_shared(
    engine: &RecordStore,
    key: String,
) -> Result<Arc<BTreeMap<String, String>>, String> {
    if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
        if let Some(cached) = cache.get(&key) {
            return Ok(cached);
        }
    }
    let response = engine.execute_durable(ExecuteRequest {
        shard_id: DEFAULT_SHARD_ID,
        command: Command::HashGetAll { key: key.clone() },
    });
    if !response.status.ok {
        return Err(format!(
            "{}: {}",
            response.status.code, response.status.message
        ));
    }
    match response.response {
        CommandResponse::HashEntries { entries } => {
            let mut decoded = BTreeMap::new();
            let mut skipped_non_utf8 = 0usize;
            let mut first_skipped = String::new();
            for (field, value) in entries {
                // A value that is not text is not corruption: byte payloads are written in a
                // carried raw shape, so embeddings and their kin are legitimately not UTF-8.
                // Failing here would cost the whole pack -- and the caller turns an error into
                // an empty pack, which a hook emits as `{}` while exiting 0. Leave the value out
                // of the selection instead, and say how many were left out.
                let value = match String::from_utf8(value) {
                    Ok(value) => value,
                    Err(error) => {
                        if skipped_non_utf8 == 0 {
                            let bytes = error.as_bytes();
                            let head: String = bytes
                                .iter()
                                .take(16)
                                .map(|byte| format!("{byte:02x}"))
                                .collect::<Vec<_>>()
                                .join(" ");
                            first_skipped = format!(
                                "key={key}, field={field}, len={}, first bytes: {head}",
                                bytes.len()
                            );
                        }
                        skipped_non_utf8 += 1;
                        continue;
                    }
                };
                decoded.insert(field, value);
            }
            if skipped_non_utf8 > 0 {
                eprintln!(
                    "context_pack skipped {skipped_non_utf8} value(s) that are not UTF-8 \
                     (first: {first_skipped})"
                );
            }
            // An empty read must stay a question, not become an answer: caching it would pin
            // "no data" for a key no write may ever touch again (observed once as a pinned
            // get_all stuck at 0 rows after a cold start until restart). An actually-empty hash
            // re-reads at map-miss cost, no page reads.
            let decoded = Arc::new(decoded);
            if !decoded.is_empty() {
                if let Ok(mut cache) = hgetall_snapshot_cache().lock() {
                    cache.insert(key, Arc::clone(&decoded));
                }
            }
            Ok(decoded)
        }
        other => Err(format!("unexpected response for hgetall: {other:?}")),
    }
}

fn json_output(value: Value, root: PathBuf) -> Result<RecordLogOutput, String> {
    let mut extra = BTreeMap::new();
    if let Some(object) = value.as_object() {
        for (key, item) in object {
            extra.insert(key.clone(), item.clone());
        }
    } else {
        extra.insert("value_json".to_string(), value.clone());
    }
    let count = extra
        .get("count")
        .and_then(Value::as_u64)
        .map(|value| value as usize)
        .or_else(|| {
            extra
                .get("records")
                .and_then(Value::as_array)
                .map(|items| items.len())
        });
    Ok(RecordLogOutput {
        value: String::new(),
        entries: BTreeMap::new(),
        records: Vec::new(),
        count,
        root,
        status: String::new(),
        mode: String::new(),
        append_path: String::new(),
        raw_storage_backend: String::new(),
        prometheus: String::new(),
        cached_clients: None,
        extra,
    })
}

fn json_field<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    let mut current = value;
    for part in path {
        current = current.get(*part)?;
    }
    Some(current)
}

include!("matrixark_rust_proxy_impl/scope_scoring.rs");

include!("matrixark_rust_proxy_impl/budget_parsing.rs");
fn record_ref_hash(record: &Value) -> Option<String> {
    for field in [
        "ref_hash",
        "chunk_hash",
        "section_hash",
        "skill_hash",
        "event_id_hash",
        "entity_hash",
        "summary_hash",
    ] {
        if let Some(value) = record.get(field) {
            if let Some(number) = value.as_u64() {
                return Some(number.to_string());
            }
            if let Some(text) = value.as_str().filter(|text| !text.is_empty()) {
                return Some(text.to_string());
            }
        }
    }
    // `ref_hashes` fallback, last so nothing that resolves today changes branch.
    //
    // The posting builder writes the singular `ref_hash` ONLY when the posting carries exactly one
    // ref; a multi-ref posting has the array and no singular field. Without this a posting like
    // that returns None here, and two serving sites drop the record outright rather than score it
    // (`let Some(profile_hash) = record_ref_hash(..) else { continue; }`), with nothing logged.
    //
    // No caller passes more than one ref today, which is precisely why the day one does the loss
    // would be silent.
    if let Some(Value::Array(refs)) = record.get("ref_hashes") {
        for value in refs {
            if let Some(number) = value.as_u64() {
                return Some(number.to_string());
            }
            if let Some(text) = value.as_str().filter(|text| !text.is_empty()) {
                return Some(text.to_string());
            }
        }
    }
    None
}

fn record_node_hash(record: &Value) -> Option<u64> {
    record.get("node_hash").and_then(Value::as_u64)
}

fn record_index_terms(
    record: &Value,
    index_terms_by_batch: &HashMap<String, HashSet<String>>,
    index_terms_by_node: &HashMap<u64, HashSet<String>>,
    index_terms_by_ref: &HashMap<String, HashSet<String>>,
) -> HashSet<String> {
    let mut terms = HashSet::new();
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    if let Some(batch) = record.get("batch_id_hash").and_then(Value::as_u64) {
        if let Some(values) = index_terms_by_batch.get(&batch.to_string()) {
            terms.extend(values.iter().cloned());
        }
    }
    if let Some(node_hash) = record_node_hash(record) {
        if let Some(values) = index_terms_by_node.get(&node_hash) {
            terms.extend(values.iter().cloned());
        }
    }
    if let Some(ref_hash) = record_ref_hash(record) {
        if let Some(values) = index_terms_by_ref.get(&ref_hash) {
            terms.extend(values.iter().cloned());
        }
    }
    match record_type {
        "context_event" => {
            terms.insert("source_type:message".to_string());
            if let Some(event_type) =
                json_field(record, &["internal_extraction", "event_type"]).and_then(Value::as_str)
            {
                if !event_type.is_empty() {
                    terms.insert(format!("event_type:{event_type}"));
                }
            }
        }
        "context_entity" => {
            if let Some(entity_type) = record.get("entity_type").and_then(Value::as_str) {
                if !entity_type.is_empty() {
                    terms.insert(format!("entity_type:{entity_type}"));
                }
            }
        }
        "resource_chunk" => {
            terms.insert("source_type:resource".to_string());
            if let Some(resource_type) = record.get("resource_type").and_then(Value::as_str) {
                if !resource_type.is_empty() {
                    terms.insert(format!("resource_type:{resource_type}"));
                }
            }
        }
        "skill_manifest" | "skill_section" => {
            terms.insert("source_type:skill".to_string());
            terms.insert("resource_type:skill".to_string());
            if record_type == "skill_manifest" {
                if let Some(name) = record.get("name").and_then(Value::as_str) {
                    if !name.is_empty() {
                        terms.insert(format!("skill_name:{}", name.to_ascii_lowercase()));
                    }
                }
            }
        }
        _ => {}
    }
    terms
}

fn passes_secondary_groups(terms: &HashSet<String>, groups: &[Vec<String>]) -> bool {
    if groups.is_empty() {
        return true;
    }
    let mode_any = groups.len() > 1;
    if mode_any {
        groups
            .iter()
            .any(|group| group.iter().any(|term| terms.contains(term)))
    } else {
        groups
            .iter()
            .all(|group| group.iter().any(|term| terms.contains(term)))
    }
}

/// Just the fields index maintenance needs, borrowed from the payload rather than copied.
///
/// The write path used to parse every appended record into a full `Value` tree to read two
/// things from it -- the record type, and a scope key that may sit in any of five places. That
/// allocates a `String` per key and a `Value` per node for a whole record, then drops them all.
/// Under a production-corpus soak the proxy was burning 68.8% of a core to the gateway's 22.0%,
/// and `Value` handling plus the allocator traffic it causes was about 40% of it.
///
/// Deserializing into borrowed `&str` fields skips the tree: serde walks the same bytes but
/// materialises nothing except what is named here, and these point into the caller's buffer.
#[derive(Deserialize)]
struct ScopeKeyOnly<'a> {
    #[serde(borrow, default)]
    scope_key: Option<&'a str>,
}

#[derive(Deserialize)]
struct MetadataFacts<'a> {
    #[serde(borrow, default)]
    access_scope: Option<ScopeKeyOnly<'a>>,
}

#[derive(Deserialize)]
struct EnvelopeFacts<'a> {
    #[serde(borrow, default)]
    scope: Option<ScopeKeyOnly<'a>>,
}

#[derive(Deserialize)]
struct IndexFacts<'a> {
    #[serde(borrow, default)]
    record_type: Option<&'a str>,
    #[serde(borrow, default)]
    scope_key: Option<&'a str>,
    #[serde(borrow, default)]
    access_scope: Option<ScopeKeyOnly<'a>>,
    #[serde(borrow, default)]
    scope: Option<ScopeKeyOnly<'a>>,
    #[serde(borrow, default)]
    metadata: Option<MetadataFacts<'a>>,
    #[serde(borrow, default)]
    envelope: Option<EnvelopeFacts<'a>>,
    #[serde(borrow, default)]
    record_bundle: Option<Vec<IndexFacts<'a>>>,
}

impl<'a> IndexFacts<'a> {
    /// The scope key, in the same source order `candidate_scope_key` uses. Order is the whole
    /// contract: a record carrying two of these must resolve to the same bucket either way.
    fn scope_key(&self) -> &'a str {
        let sources = [
            self.scope_key,
            self.access_scope.as_ref().and_then(|s| s.scope_key),
            self.metadata
                .as_ref()
                .and_then(|m| m.access_scope.as_ref())
                .and_then(|s| s.scope_key),
            self.scope.as_ref().and_then(|s| s.scope_key),
            self.envelope
                .as_ref()
                .and_then(|e| e.scope.as_ref())
                .and_then(|s| s.scope_key),
        ];
        for source in sources {
            if let Some(text) = source {
                if !text.is_empty() {
                    return text;
                }
            }
        }
        ""
    }
}

/// Index facts for one stored payload, or None when the borrowed parse cannot answer.
///
/// None is not "no facts" -- it means fall back to the `Value` path, which is what a payload
/// whose shape this struct does not model (a field typed differently than expected) must do.
/// Getting that wrong would silently mis-index a record rather than merely cost time.
fn payload_index_facts(value: &str) -> Option<Vec<IndexFacts<'_>>> {
    let facts: IndexFacts = serde_json::from_str(value).ok()?;
    if let Some(bundle) = facts.record_bundle {
        return Some(bundle);
    }
    Some(vec![IndexFacts {
        record_type: facts.record_type,
        scope_key: facts.scope_key,
        access_scope: facts.access_scope,
        scope: facts.scope,
        metadata: facts.metadata,
        envelope: facts.envelope,
        record_bundle: None,
    }])
}

fn decode_matrixark_payload(value: &str) -> Vec<Value> {
    let Ok(mut decoded) = serde_json::from_str::<Value>(value) else {
        return Vec::new();
    };
    // Take the bundle rather than copying it. `decoded` is ours -- it was just parsed here and
    // nothing else can see it -- so cloning each record deep-copied every map and vector in it
    // for no one. Sampling the proxy under sustained ingest put `BTreeMap::clone_subtree` and
    // `Vec<Value>::clone` among the hottest frames; this is where they came from.
    if let Some(bundle) = decoded
        .get_mut("record_bundle")
        .and_then(Value::as_array_mut)
    {
        return std::mem::take(bundle)
            .into_iter()
            .filter(Value::is_object)
            .collect();
    }
    if decoded.is_object() {
        vec![decoded]
    } else {
        Vec::new()
    }
}

fn type_index_key(record_hash_key: &str, record_type: &str) -> String {
    format!("{record_hash_key}:type_index:{record_type}")
}

fn type_index_ready_key(record_hash_key: &str) -> String {
    format!("{record_hash_key}:type_index_ready")
}

/// `Some((record_hash_key, shard6))` when `key` is a record-shard key (`...:records:NNNNNN`).
///
/// The append op sees every hash entry a write carries -- latest-state rows, side-index rows,
/// counters -- and only the record shards may feed the type index: an index-served scan fetches
/// whatever the index names, and naming a non-record key would make it return rows the walk it
/// replaces could never have seen.
/// A stored location, in either shape, as `(shard, field)` within `record_hash_key`.
///
/// The long shape spells the whole thing out -- `{"key":"<base>:000003","field":"000...014"}` --
/// and the compact shape is `"3:14"`, shard and offset in decimal. Measured over 300 ingests, the
/// long shape was 87% of every byte written to a page, because the base is one deployment-wide
/// string repeated in every entry and the offset is a twenty-digit rendering of a small number.
///
/// A compact entry is always relative to the reader's own record log, which is the same thing the
/// long shape's base check enforces: an entry under another base is not this log's business, and
/// the writer leaves those in the long shape precisely because the compact one cannot say them.
/// Every location a ref's locator holds, head chunk and continuations together.
///
/// A locator list longer than one chunk keeps its head under the ref's own field and continues in
/// `"{ref}#1"`, `"{ref}#2"`, with the head naming how many follow. A reader that stops at the head
/// sees a truncated list, and here that is not a slow answer but a wrong one: one of these callers
/// decides which records a delete touches, so a missed chunk leaves records undeleted.
fn locator_location_values(
    engine: &RecordStore,
    locator_key: &str,
    id: &str,
) -> Result<Vec<Value>, String> {
    let mut out: Vec<Value> = Vec::new();
    let raw = read_bytes(
        engine,
        Command::HashGet {
            key: locator_key.to_string(),
            field: id.to_string(),
        },
    )?;
    if raw.is_empty() {
        return Ok(out);
    }
    let Ok(decoded) = serde_json::from_str::<Value>(&raw) else {
        return Ok(out);
    };
    if let Some(items) = decoded.get("locations").and_then(Value::as_array) {
        out.extend(items.iter().cloned());
    }
    let chunks = decoded
        .get("location_chunks")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    for index in 1..=chunks {
        let chunk_raw = read_bytes(
            engine,
            Command::HashGet {
                key: locator_key.to_string(),
                field: format!("{id}#{index}"),
            },
        )?;
        if chunk_raw.is_empty() {
            continue;
        }
        if let Ok(chunk) = serde_json::from_str::<Value>(&chunk_raw) {
            if let Some(items) = chunk.get("locations").and_then(Value::as_array) {
                out.extend(items.iter().cloned());
            }
        }
    }
    Ok(out)
}

fn location_shard_and_field(location: &Value, record_hash_key: &str) -> Option<(String, String)> {
    if let Some(compact) = location.as_str() {
        let (shard, offset) = compact.split_once(':')?;
        let shard: u64 = shard.parse().ok()?;
        let offset: u64 = offset.parse().ok()?;
        return Some((format!("{shard:06}"), format!("{offset:020}")));
    }
    let key = location.get("key").and_then(Value::as_str).unwrap_or("");
    let field = location.get("field").and_then(Value::as_str).unwrap_or("");
    if key.is_empty() || field.is_empty() {
        return None;
    }
    // Only locations in THIS record log: the locator is shared per prefix, and a location under
    // another base must not leak into this scan.
    let (base, shard) = record_shard_key_parts(key)?;
    if base != record_hash_key {
        return None;
    }
    Some((shard.to_string(), field.to_string()))
}

fn record_shard_key_parts(key: &str) -> Option<(&str, &str)> {
    let (base, shard) = key.rsplit_once(':')?;
    if shard.len() != 6 || !shard.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    if !base.ends_with(":records") {
        return None;
    }
    Some((base, shard))
}

/// The record types stored in one shard-field payload, bundle-expanded.
/// Distinct `record_type`s across already-decoded records.
///
/// Split out of `payload_record_types` so a caller that has already decoded the payload -- the
/// batch-append handler, which also needs the records for the scope index -- does not decode it
/// a second time just to read one field.
fn records_record_types(records: &[Value]) -> Vec<String> {
    let mut types: Vec<String> = Vec::new();
    for record in records {
        if let Some(record_type) = record.get("record_type").and_then(Value::as_str) {
            if !record_type.is_empty() && !types.iter().any(|t| t == record_type) {
                types.push(record_type.to_string());
            }
        }
    }
    types
}

fn payload_record_types(value: &str) -> Vec<String> {
    records_record_types(&decode_matrixark_payload(value))
}

/// `records_record_types` over the borrowed parse. The `&str`s point into the caller's buffer.
fn facts_record_types<'a>(facts: &[IndexFacts<'a>]) -> Vec<&'a str> {
    let mut types: Vec<&'a str> = Vec::new();
    for fact in facts {
        if let Some(record_type) = fact.record_type {
            if !record_type.is_empty() && !types.contains(&record_type) {
                types.push(record_type);
            }
        }
    }
    types
}

/// Payload values for the requested types via the type index, in append order.
///
/// `Ok(None)` when the index cannot answer -- no ready-marker yet -- and the caller must walk.
/// Locations that no longer resolve (a field physically removed after a partial-cleanup path)
/// are skipped: the caller re-decodes and re-filters everything it is handed, so a stale entry
/// can cost a read but never change an answer.
/// The newest `limit` locations, or all of them when there is no cap.
///
/// A location is "{shard:06}:{field}" with both parts zero-padded, so lexical order IS append
/// order and the newest are the last. Sorting is not assumed of the input: the type index is read
/// into a map whose iteration order is its own business, and a cap that trusted the wrong order
/// would silently keep the OLDEST records instead -- a wrong answer rather than a slow one.
fn newest_locations(mut locations: Vec<String>, limit: Option<usize>) -> Vec<String> {
    match limit {
        Some(limit) if locations.len() > limit => {
            locations.sort();
            locations.split_off(locations.len() - limit)
        }
        _ => locations,
    }
}

fn type_index_payloads(
    engine: &RecordStore,
    record_hash_key: &str,
    allowed_types: &HashSet<String>,
    newest_by_type: Option<&BTreeMap<String, usize>>,
) -> Result<Option<(Vec<String>, u64)>, String> {
    let ready = read_record_count(engine, &type_index_ready_key(record_hash_key))?;
    if ready.trim() != "1" {
        return Ok(None);
    }
    let mut locations: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for record_type in allowed_types {
        // Per-type cap, the same rule the pinned-scope path applies: a location is
        // "{shard:06}:{field}" with both parts zero-padded, so lexical order IS append order and
        // the newest N are the last N. A type with no cap keeps every position it had.
        //
        // Without this, a scan that carries a cap but no scope lands here and cap the type index
        // ignores it -- so a caller asking for one record of a type is handed every record of
        // that type, and what it pays grows with the store.
        let limit = newest_by_type
            .and_then(|caps| caps.get(record_type))
            .copied()
            .filter(|limit| *limit > 0);
        let of_type: Vec<String> =
            hgetall_shared(engine, type_index_key(record_hash_key, record_type))?
                .keys()
                .cloned()
                .collect();
        locations.extend(newest_locations(of_type, limit));
    }
    // BTreeSet order is lexical: shard6 then the zero-padded record field = append order.
    let (values, shards_touched) =
        fetch_indexed_payload_values(engine, record_hash_key, &locations)?;
    Ok(Some((values, shards_touched)))
}

fn scope_index_key(record_hash_key: &str, bucket: &str) -> String {
    format!("{record_hash_key}:scope_index:{bucket}")
}

fn scope_index_ready_key(record_hash_key: &str) -> String {
    format!("{record_hash_key}:scope_index_ready")
}

/// Bumped when the bucket layout changes; a store whose marker holds an older value re-walks
/// once and rebuilds. Version 2 = the type-partitioned scopeless bucket.
const SCOPE_INDEX_LAYOUT_VERSION: &str = "2";

/// The scope buckets a record files its field under, from its OWN scope_key -- the same source
/// the scope matcher reads.
///
/// Scopeless records match EVERY query under the matcher's rules, so they must reach every
/// index-served scan -- but ingest bundles a scoped event with scopeless system records in ONE
/// field, so a single master bucket would put nearly every field in the store there (measured:
/// 380 of ~460) and the fetch degenerates to a walk. A scopeless record therefore files under
/// "none" (for untyped queries) AND "none:{record_type}" (so a typed query only drags in
/// scopeless records of the types it asked for). "partial" = a scope_key lacking a tenant or
/// user part: the matcher rejects those against any pinned query, so nothing fetches the bucket.
fn record_scope_buckets(record: &Value) -> Vec<String> {
    let scope_key = candidate_scope_key(record);
    if scope_key.is_empty() {
        let record_type = record
            .get("record_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        return vec!["none".to_string(), format!("none:{record_type}")];
    }
    let parts = parse_scope_key(&scope_key);
    match (parts.get("t"), parts.get("u")) {
        (Some(tenant), Some(user)) if *tenant != 0 && *user != 0 => {
            vec![format!("t={tenant}|u={user}")]
        }
        _ => vec!["partial".to_string()],
    }
}

/// `record_scope_buckets` over the borrowed parse. Must agree with it bucket-for-bucket, since
/// the two run against the same store: a record filed under a different bucket by one path than
/// the other is a record an index-served scan cannot find.
fn facts_scope_buckets(fact: &IndexFacts<'_>) -> Vec<String> {
    let scope_key = fact.scope_key();
    if scope_key.is_empty() {
        let record_type = fact.record_type.unwrap_or("");
        return vec!["none".to_string(), format!("none:{record_type}")];
    }
    let parts = parse_scope_key(scope_key);
    match (parts.get("t"), parts.get("u")) {
        (Some(tenant), Some(user)) if *tenant != 0 && *user != 0 => {
            vec![format!("t={tenant}|u={user}")]
        }
        _ => vec!["partial".to_string()],
    }
}

/// The bucket a query pins, or None when the query is not pinned enough for the scope index.
///
/// Pinned = non-zero tenant and user hashes with the user marked explicit. A tenant-wide query
/// would need every user's bucket, and a session-mode refinement is applied by the shared filter
/// loop after the fetch -- the index only has to be a superset.
fn query_scope_bucket(query_scope: Option<&Value>) -> Option<String> {
    let query = query_scope.filter(|value| value.is_object())?;
    let tenant = query.get("tenant_hash").and_then(Value::as_u64).unwrap_or(0);
    let user = query.get("user_hash").and_then(Value::as_u64).unwrap_or(0);
    if tenant == 0 || user == 0 || !scope_key_explicit(query, "user_id") {
        return None;
    }
    Some(format!("t={tenant}|u={user}"))
}

/// Payload values for a pinned-scope scan, in append order: the bucket's locations plus the
/// scopeless bucket, intersected with the requested types' locations when the type index can
/// answer. `Ok(None)` = the scope index cannot answer; the caller walks (and backfills).
fn scope_index_payloads(
    engine: &RecordStore,
    record_hash_key: &str,
    allowed_types: &HashSet<String>,
    bucket: &str,
    newest_by_type: Option<&BTreeMap<String, usize>>,
) -> Result<Option<(Vec<String>, u64)>, String> {
    let ready = read_record_count(engine, &scope_index_ready_key(record_hash_key))?;
    if ready.trim() != SCOPE_INDEX_LAYOUT_VERSION {
        return Ok(None);
    }
    let mut source_buckets: Vec<String> = vec![bucket.to_string()];
    // Tenant-scoped records (a scope_key with a tenant but no user) live in "partial". Consumers
    // differ on them -- get_all wants exact tenant AND user equality and drops them, while prior
    // context accepts a tenant-wide summary for a user in that tenant -- so the fetch includes
    // them and the shared filter loop applies each consumer's real predicate. The index stays a
    // pre-filter; widening it can cost a wasted read, never a wrong answer, and an unpinned scan
    // was already returning these rows.
    source_buckets.push("partial".to_string());
    if allowed_types.is_empty() {
        source_buckets.push("none".to_string());
    } else {
        for record_type in allowed_types {
            source_buckets.push(format!("none:{record_type}"));
        }
    }
    let mut positions: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for source_bucket in &source_buckets {
        for location in
            hgetall_shared(engine, scope_index_key(record_hash_key, source_bucket))?.keys()
        {
            positions.insert(location.clone());
        }
    }
    if !allowed_types.is_empty() {
        let type_ready = read_record_count(engine, &type_index_ready_key(record_hash_key))?;
        if type_ready.trim() == "1" {
            let mut type_positions: std::collections::BTreeSet<String> =
                std::collections::BTreeSet::new();
            for record_type in allowed_types {
                for location in
                    hgetall_shared(engine, type_index_key(record_hash_key, record_type))?.keys()
                {
                    type_positions.insert(location.clone());
                }
            }
            positions = positions
                .intersection(&type_positions)
                .cloned()
                .collect();
            // Per-type cap, applied AFTER the scope intersection so "newest" means newest within
            // this scope rather than newest in the store. A location is "{shard:06}:{field}" with
            // both parts zero-padded, so lexical order IS append order and the newest N are the
            // last N. Types without a cap keep every position they had.
            if let Some(caps) = newest_by_type {
                let mut capped: std::collections::BTreeSet<String> = positions.clone();
                for (record_type, limit) in caps {
                    if *limit == 0 || !allowed_types.contains(record_type) {
                        continue;
                    }
                    let mut of_type: Vec<String> = Vec::new();
                    for location in
                        hgetall_shared(engine, type_index_key(record_hash_key, record_type))?.keys()
                    {
                        if positions.contains(location) {
                            of_type.push(location.clone());
                        }
                    }
                    if of_type.len() <= *limit {
                        continue;
                    }
                    of_type.sort();
                    let keep: std::collections::BTreeSet<String> =
                        of_type[of_type.len() - *limit..].iter().cloned().collect();
                    for location in of_type {
                        if !keep.contains(&location) {
                            capped.remove(&location);
                        }
                    }
                }
                positions = capped;
            }
        }
    }
    let (values, shards_touched) =
        fetch_indexed_payload_values(engine, record_hash_key, &positions)?;
    Ok(Some((values, shards_touched)))
}

/// Persist a walk-built scope index and its ready-marker, once. Returns whether it wrote.
fn persist_scope_index_backfill(
    engine: &RecordStore,
    record_hash_key: &str,
    entries_by_index_key: BTreeMap<String, Vec<(String, Vec<u8>)>>,
) -> Result<bool, String> {
    let ready = read_record_count(engine, &scope_index_ready_key(record_hash_key))?;
    if ready.trim() == SCOPE_INDEX_LAYOUT_VERSION {
        return Ok(false);
    }
    let mut commands: Vec<Command> = entries_by_index_key
        .into_iter()
        .map(|(key, entries)| Command::HashMultiSet { key, entries })
        .collect();
    commands.push(Command::StringSet {
        key: scope_index_ready_key(record_hash_key),
        value: SCOPE_INDEX_LAYOUT_VERSION.as_bytes().to_vec(),
    });
    execute_empty_batch_runtime(engine, commands, true)?;
    Ok(true)
}

/// Is this record about one of `ids` -- carrying it, targeting it, or created by superseding it?
///
/// `record_addressable_ids` covers what a record CARRIES (its own identity and its ref hashes);
/// history also needs the records that POINT at an id: a tombstone's `target_memory_id`, and the
/// supersede link `superseded_by` that marks the successor's creation.
fn record_id_linked(record: &Value, ids: &HashSet<String>) -> bool {
    if record_carries_wanted_id(record, |id| ids.contains(id)) {
        return true;
    }
    for field in ["target_memory_id", "superseded_by", "source_event_hash"] {
        match record.get(field) {
            Some(Value::String(text)) if ids.contains(text.as_str()) => return true,
            Some(Value::Number(number)) if ids.contains(number.to_string().as_str()) => {
                return true
            }
            _ => {}
        }
    }
    // Provenance arrays: a derivative points at its sources through these, without carrying
    // them as addressable ids -- exactly the records a get-by-id must return alongside the event.
    for field in ["source_event_ids", "source_refs"] {
        if let Some(Value::Array(items)) = record.get(field) {
            for item in items {
                match item {
                    Value::String(text) if ids.contains(text.as_str()) => return true,
                    Value::Number(number) if ids.contains(number.to_string().as_str()) => {
                        return true
                    }
                    _ => {}
                }
            }
        }
    }
    false
}

/// Payload values for an id-scoped scan, in append order: the ids' locator locations plus the
/// type-index locations of every requested type except `context_event` (events carry their own
/// id, so the locator covers them; tombstones and feedback point at an id without carrying it,
/// and they are sparse). `Ok(None)` = compose cannot answer; the caller walks.
fn id_scoped_payloads(
    engine: &RecordStore,
    record_hash_key: &str,
    allowed_types: &HashSet<String>,
    requested_ids: &[String],
) -> Result<Option<(Vec<String>, u64)>, String> {
    let ready = read_record_count(engine, &type_index_ready_key(record_hash_key))?;
    if ready.trim() != "1" {
        return Ok(None);
    }
    // The record hash key is `{prefix}:records`, so the locator key derives from it -- callers
    // do not reliably send storage_prefix, and the id mode must not depend on an optional field.
    let Some(prefix) = record_hash_key.strip_suffix(":records") else {
        return Ok(None);
    };
    let locator_key = format!("{prefix}:context_ref_locator");
    // A store whose locator was fed pointed ids (provenance + targets) from its FIRST append
    // marks itself; on such stores the locator alone answers "records about these ids", and the
    // type-index compose below -- which would fetch every record of each requested type -- is
    // skipped. Unmarked (pre-existing) stores keep the composed behavior unchanged.
    let locator_covers_pointed_ids = hgetall_map(engine, format!("{locator_key}_meta"))
        .ok()
        .map(|meta| {
            meta.get("provenance_from_start")
                .map(|value| value.trim() == "1")
                .unwrap_or(false)
        })
        .unwrap_or(false);
    let mut positions: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for id in requested_ids {
        let mut found = 0_usize;
        for location in locator_location_values(engine, &locator_key, id)? {
            let Some((shard, field)) = location_shard_and_field(&location, record_hash_key) else {
                continue;
            };
            positions.insert(format!("{shard}:{field}"));
            found += 1;
        }
        if found == 0 {
            // An old store predating the side index, or an id that never existed. The walk is
            // the correct answer for both; guessing "no records" here would erase real history.
            return Ok(None);
        }
    }
    if !locator_covers_pointed_ids {
        for record_type in allowed_types {
            if record_type == "context_event" {
                continue;
            }
            for location in
                hgetall_shared(engine, type_index_key(record_hash_key, record_type))?.keys()
            {
                positions.insert(location.clone());
            }
        }
    }
    let (values, shards_touched) =
        fetch_indexed_payload_values(engine, record_hash_key, &positions)?;
    Ok(Some((values, shards_touched)))
}

/// Persist a walk-built index and its ready-marker, once. Returns whether it wrote.
fn persist_type_index_backfill(
    engine: &RecordStore,
    record_hash_key: &str,
    entries_by_index_key: BTreeMap<String, Vec<(String, Vec<u8>)>>,
) -> Result<bool, String> {
    let ready = read_record_count(engine, &type_index_ready_key(record_hash_key))?;
    if ready.trim() == "1" {
        return Ok(false);
    }
    let mut commands: Vec<Command> = entries_by_index_key
        .into_iter()
        .map(|(key, entries)| Command::HashMultiSet { key, entries })
        .collect();
    commands.push(Command::StringSet {
        key: type_index_ready_key(record_hash_key),
        value: b"1".to_vec(),
    });
    execute_empty_batch_runtime(engine, commands, true)?;
    Ok(true)
}

fn scan_matrixark_candidates(
    engine: &RecordStore,
    command: &RecordLogRequest,
) -> Result<Value, String> {
    let count_key = required_option(command.count_key.clone(), "count_key")?;
    let record_hash_key = required_option(command.record_hash_key.clone(), "record_hash_key")?;
    // 256, matching the writer. Guessing 1024 against a store written at 256 computes
    // max_shard = (count - 1) / 1024, enumerates a quarter of the shards, and returns a
    // partial store as though it were the whole one. Over-enumerating is the safe direction:
    // extra shard keys simply do not exist and read back empty.
    let shard_size = command.shard_size.unwrap_or(DEFAULT_RECORD_LOG_SHARD_SIZE).max(1);
    let count_text = read_record_count(engine, &count_key)?;
    let count = count_text.parse::<u64>().unwrap_or(0);
    let freshness = scan_freshness_token(engine, command, &record_hash_key, count);
    let scan_cache_key = matrixark_scan_cache_key(command, &freshness);
    // Take everything needed from the cache under one guard, then drop it before stamping the
    // result -- stamping used to re-lock this same mutex and hang the request.
    let cached_hit = match matrixark_scan_cache().lock() {
        Ok(cache) => {
            let entries = cache.len();
            cache.get(&scan_cache_key).cloned().map(|value| (value, entries))
        }
        Err(_) => None,
    };
    if let Some((value, entries)) = cached_hit {
        return Ok(mark_scan_cache_hit(value, entries));
    }
    let allowed_types: HashSet<String> = command
        .record_types
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let allowed_statuses: HashSet<String> = command
        .record_statuses
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let selected_nodes: HashSet<u64> = command
        .selected_node_hashes
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    let secondary_groups = command.secondary_index_groups.clone().unwrap_or_default();
    let max_shard = if count == 0 {
        0
    } else {
        (count - 1) / shard_size
    };
    let mut placement_partitions_touched = if count == 0 { 0 } else { max_shard + 1 };
    let mut scanned_records = 0_u64;
    let mut dropped_by_type = 0_u64;
    let mut dropped_by_status = 0_u64;
    let mut dropped_by_scope = 0_u64;
    let mut selected_node_dropped = 0_u64;
    // Collect payloads first -- from the type index when it can answer, from the shard walk
    // otherwise -- then run one shared filter loop, so the two paths cannot drift.
    let mut payload_values: Vec<String> = Vec::new();
    let requested_ids: Vec<String> = command.record_ids.clone().unwrap_or_default();
    let requested_id_set: HashSet<String> = requested_ids.iter().cloned().collect();
    let mut id_scoped_used = false;
    let mut type_index_used = false;
    if !requested_ids.is_empty() && count > 0 {
        if let Some((values, shards_touched)) = id_scoped_payloads(
            engine,
            &record_hash_key,
            &allowed_types,
            &requested_ids,
        )? {
            payload_values = values;
            placement_partitions_touched = shards_touched;
            id_scoped_used = true;
        }
    }
    let query_bucket = query_scope_bucket(command.scope.as_ref());
    let mut scope_index_used = false;
    if !id_scoped_used && count > 0 {
        if let Some(bucket) = &query_bucket {
            if let Some((values, shards_touched)) = scope_index_payloads(
                engine,
                &record_hash_key,
                &allowed_types,
                bucket,
                command.newest_by_type.as_ref(),
            )? {
                payload_values = values;
                placement_partitions_touched = shards_touched;
                scope_index_used = true;
            }
        }
    }
    // A pinned query whose scope index is not ready takes the WALK on purpose -- the walk
    // backfills the scope index, while the type path would serve this scan and leave the scope
    // index unbuilt forever on stores that predate it.
    if !id_scoped_used
        && !scope_index_used
        && query_bucket.is_none()
        && !allowed_types.is_empty()
        && count > 0
    {
        if let Some((values, shards_touched)) = type_index_payloads(
            engine,
            &record_hash_key,
            &allowed_types,
            command.newest_by_type.as_ref(),
        )? {
            payload_values = values;
            placement_partitions_touched = shards_touched;
            type_index_used = true;
        }
    }
    let mut type_index_backfilled = false;
    if !id_scoped_used && !scope_index_used && !type_index_used && count > 0 {
        // The walk this scan pays anyway sees every payload, so it can build the index for every
        // type in the store as a side effect; the marker makes the next scan's miss authoritative.
        let mut backfill: BTreeMap<String, Vec<(String, Vec<u8>)>> = BTreeMap::new();
        let mut scope_backfill: BTreeMap<String, Vec<(String, Vec<u8>)>> = BTreeMap::new();
        for shard in 0..=max_shard {
            let key = format!("{}:{:06}", record_hash_key, shard);
            let shard6 = format!("{shard:06}");
            for (field, value) in hgetall_map(engine, key.clone())? {
                for record_type in payload_record_types(&value) {
                    backfill
                        .entry(type_index_key(&record_hash_key, &record_type))
                        .or_default()
                        .push((format!("{shard6}:{field}"), b"1".to_vec()));
                }
                for record in decode_matrixark_payload(&value) {
                    for bucket in record_scope_buckets(&record) {
                        scope_backfill
                            .entry(scope_index_key(&record_hash_key, &bucket))
                            .or_default()
                            .push((format!("{shard6}:{field}"), b"1".to_vec()));
                    }
                }
                payload_values.push(value);
            }
        }
        type_index_backfilled =
            persist_type_index_backfill(engine, &record_hash_key, backfill)?;
        persist_scope_index_backfill(engine, &record_hash_key, scope_backfill)?;
    }
    // Built once, outside the record loop: a set per record would allocate per record, on the
    // very path this exists to make cheaper.
    let projection: Option<std::collections::BTreeSet<String>> = command
        .record_fields
        .as_ref()
        .filter(|fields| !fields.is_empty())
        .map(|fields| fields.iter().cloned().collect());
    let mut records = Vec::new();
    {
        for value in &payload_values {
            for record in decode_matrixark_payload(value) {
                scanned_records += 1;
                let record_type = record
                    .get("record_type")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if !allowed_types.is_empty() && !allowed_types.contains(record_type) {
                    dropped_by_type += 1;
                    continue;
                }
                if !allowed_statuses.is_empty() {
                    let record_status = record
                        .get("status")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if !allowed_statuses.contains(record_status) {
                        dropped_by_status += 1;
                        continue;
                    }
                }
                if !requested_id_set.is_empty() && !record_id_linked(&record, &requested_id_set) {
                    dropped_by_scope += 1; // id-filtered, counted with scope drops
                    continue;
                }
                if !scope_matches_record(&record, command.scope.as_ref()) {
                    dropped_by_scope += 1;
                    continue;
                }
                if !selected_nodes.is_empty() {
                    let keep_index = matches!(record_type, "context_index" | "context_embedding");
                    let keep_node = record_node_hash(&record)
                        .map(|node| selected_nodes.contains(&node))
                        .unwrap_or(false);
                    if !keep_index && !keep_node {
                        selected_node_dropped += 1;
                        continue;
                    }
                }
                records.push(project_record(record, projection.as_ref()));
            }
        }
    }

    let mut index_terms_by_batch: HashMap<String, HashSet<String>> = HashMap::new();
    let mut index_terms_by_node: HashMap<u64, HashSet<String>> = HashMap::new();
    let mut index_terms_by_ref: HashMap<String, HashSet<String>> = HashMap::new();
    for record in &records {
        if record.get("record_type").and_then(Value::as_str) != Some("context_index") {
            continue;
        }
        let Some(index_name) = record
            .get("index_name")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        if let Some(batch) = record.get("batch_id_hash").and_then(Value::as_u64) {
            index_terms_by_batch
                .entry(batch.to_string())
                .or_default()
                .insert(index_name.to_string());
        }
        if let Some(ref_hash) = record_ref_hash(record) {
            index_terms_by_ref
                .entry(ref_hash)
                .or_default()
                .insert(index_name.to_string());
        } else if let Some(node_hash) = record_node_hash(record) {
            index_terms_by_node
                .entry(node_hash)
                .or_default()
                .insert(index_name.to_string());
        }
    }

    let mut secondary_dropped = 0_u64;
    let mut secondary_matched = 0_u64;
    let filtered = if secondary_groups.is_empty() {
        records
    } else {
        records
            .into_iter()
            .filter(|record| {
                let terms = record_index_terms(
                    record,
                    &index_terms_by_batch,
                    &index_terms_by_node,
                    &index_terms_by_ref,
                );
                if !terms.is_empty() && !passes_secondary_groups(&terms, &secondary_groups) {
                    secondary_dropped += 1;
                    return false;
                }
                if !terms.is_empty() {
                    secondary_matched += 1;
                }
                true
            })
            .collect::<Vec<_>>()
    };
    let mut non_serving_dropped = 0_u64;
    let returned_records = if command.return_index_records {
        filtered
    } else {
        filtered
            .into_iter()
            .filter(|record| {
                let record_type = record
                    .get("record_type")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let drop = matches!(
                    record_type,
                    "context_index"
                        | "context_embedding"
                        | "resource_manifest"
                        | "skill_registry_update"
                );
                if drop {
                    non_serving_dropped += 1;
                }
                !drop
            })
            .collect::<Vec<_>>()
    };

    let dropped_ref_count = dropped_by_type
        + dropped_by_scope
        + selected_node_dropped
        + secondary_dropped
        + non_serving_dropped;
    let scan_cache_entries_before_store = matrixark_scan_cache()
        .lock()
        .map(|cache| cache.len())
        .unwrap_or(0);
    let output = json!({
        "ok": true,
        "count": returned_records.len(),
        "records": returned_records,
        "native_candidate_prefilter": true,
        "scan_count": scanned_records,
        "cache_hit": false,
        "selected_ref_count": 0,
        "dropped_ref_count": dropped_ref_count,
        "scan_stats": {
            "execution_mode": "rust_proxy_native_candidate_prefilter",
            "native_prefix_scan": true,
            "native_secondary_index_prefilter": !secondary_groups.is_empty(),
            "candidate_cache_hit": false,
            "candidate_cache_scope": "process_global",
            "cache_hit": false,
            "native_placement_candidate_cache_hit": false,
            "native_placement_candidate_cache_entries": scan_cache_entries_before_store,
            "native_candidate_cache_key_shape": "storage_prefix+count+scope+record_types+selected_node_hashes+secondary_index_groups+return_index_records",
            "native_candidate_cache_payload": "compact_struct",
            "serving_memory_cache_layer": "rust_proxy_scan_cache",
            "serving_memory_promoted": true,
            "serving_memory_promoted_record_count": returned_records.len(),
            "placement_partitions_touched": placement_partitions_touched,
            "index_postings_read": placement_partitions_touched,
            "scanned_records": scanned_records,
            "returned_records": returned_records.len(),
            "non_serving_record_dropped_count": non_serving_dropped,
            "return_index_records": command.return_index_records,
            "dropped_by_type": dropped_by_type,
            "dropped_by_status": dropped_by_status,
            "dropped_by_scope": dropped_by_scope,
            "selected_node_dropped_candidate_count": selected_node_dropped,
            "secondary_index_groups_supplied": secondary_groups.len(),
            "id_scoped_used": id_scoped_used,
            "scope_index_used": scope_index_used,
            "type_index_used": type_index_used,
            "type_index_backfilled": type_index_backfilled,
            "secondary_index_matched_candidate_count": secondary_matched,
            "secondary_index_dropped_candidate_count": secondary_dropped,
            "native_pack_assembly": false,
            "pack_assembly_location": "python_reference_packer",
            "next_native_gap": "conformance ContextPack scoring and budget assembly APIs"
        }
    });
    if let Ok(mut cache) = matrixark_scan_cache().lock() {
        cache.insert(scan_cache_key, output.clone());
    }
    Ok(output)
}

/// Outcome of a native scope-forget: how many records were logically removed and how the
/// removal decomposed into per-field tombstone-deletes vs partial rewrites.
#[derive(Debug, Default, Clone, Copy)]
struct ForgetScopeStats {
    records_scanned: usize,
    records_removed: usize,
    fields_deleted: usize,
    fields_rewritten: usize,
    shards_scanned: u64,
    /// Fields whose payload was decoded. `records_scanned` alone cannot say whether a wide purge
    /// opened many fields or a few dense ones, and those have opposite fixes: too many fields
    /// opened is a question about where records live, too many records per field is a question
    /// about how they are packed.
    fields_visited: usize,
    /// Of those, the ones that held nothing to remove -- decoded, then discarded. A purge whose
    /// opened fields all match is reading a precise set of locations; a large count here means the
    /// locations being handed to it do not hold the ids it was asked for.
    fields_without_match: usize,
    /// Of the fields that held nothing to remove, the ones that still hold a record POINTING at a
    /// wanted id. Those locations are correctly filed and cannot be dropped -- deletion reaches a
    /// derivative through exactly that link. The remainder, `fields_without_match` minus this, is
    /// the part where nothing in the field relates to the ids at all.
    fields_pointed_only: usize,
    /// Located entries dropped because this call decoded the field they point at and found nothing
    /// filed under that id. They accumulate because the index is append-only: a purge that rewrites
    /// a field to remove an id's records leaves behind the entry that led it there, and every later
    /// purge pays to open that field again.
    locator_locations_dropped: usize,
}

/// A forget query must actually CONSTRAIN which subject's records it removes. Because
/// `scope_matches_record` only enforces an identity field when it is marked explicit (or when a
/// non-zero `tenant_hash` is present), a bare/empty scope would match EVERY record and silently
/// wipe the whole store. Refuse anything that does not pin at least one subject dimension, so a
/// misrouted or under-specified forget fails loudly instead of deleting every scope's memory.
fn forget_scope_is_specific(scope: &Value) -> bool {
    if !scope.is_object() {
        return false;
    }
    // Tenant isolation: a real tenant hash always narrows matching to that tenant.
    if scope.get("tenant_hash").and_then(Value::as_u64).unwrap_or(0) != 0 {
        return true;
    }
    // Otherwise require an explicit, non-empty subject dimension -- the same set of keys
    // `scope_matches_record` enforces only when they are marked explicit.
    for key in [
        "user_id",
        "session_id",
        "account_id",
        "tenant_id",
        "team",
        "project",
        "agent_name",
    ] {
        let present = scope
            .get(key)
            .and_then(Value::as_str)
            .map(|value| !value.is_empty())
            .unwrap_or(false);
        if present && scope_key_explicit(scope, key) {
            return true;
        }
    }
    false
}

/// Re-encode the survivors of a partially-forgotten field, preserving the original on-disk shape:
/// a `{"record_bundle":[...]}` envelope keeps its sibling metadata and just drops the forgotten
/// entries; a single-record field that survives is written back verbatim.
fn encode_forget_survivors(original: &str, survivors: Vec<Value>) -> String {
    if let Ok(mut decoded) = serde_json::from_str::<Value>(original) {
        if decoded
            .get("record_bundle")
            .and_then(Value::as_array)
            .is_some()
        {
            decoded["record_bundle"] = Value::Array(survivors);
            return decoded.to_string();
        }
    }
    if survivors.len() == 1 {
        return survivors.into_iter().next().unwrap().to_string();
    }
    json!({ "record_bundle": survivors }).to_string()
}

/// Native scope-forget: delete every record under a scope prefix as ONE logical, durable,
/// recovery-safe operation.
///
/// Records live in the same shard set as ingest (`{record_hash_key}:{shard:06}`, counted by
/// `count_key`) with many scopes co-resident; scope isolation is by filter, not by key partition.
/// So forget enumerates every shard, decodes each hash field's record(s), and removes ONLY the
/// records that match `scope` (reusing the exact `scope_matches_record` predicate the retrieve
/// scan uses, so "what retrieve would return for this subject" == "what forget deletes"):
///   * a field whose records ALL match becomes a `HashDelete` (durable tombstone),
///   * a field with a mix is rewritten to keep the survivors,
///   * a field with no match is left untouched.
/// The commands are applied as a single durable batch (WAL-committed, same path as ingest), so the
/// removal replicates (rides the WAL / checkpoint index) and survives WAL replay without
/// resurrecting -- the delete is a first-class WAL mutation, not a read-time filter. Leaving
/// `count_key` untouched keeps forget idempotent and other scopes intact.
fn forget_scope_records(
    engine: &RecordStore,
    record_hash_key: &str,
    count_key: &str,
    shard_size: u64,
    scope: &Value,
) -> Result<ForgetScopeStats, String> {
    if !scope.is_object() {
        return Err("forget requires a scope object".to_string());
    }
    if !forget_scope_is_specific(scope) {
        return Err(
            "forget scope must constrain a subject (a non-zero tenant_hash, or an explicit \
             user_id/session_id/account_id/tenant_id/team/project/agent_name); refusing an \
             under-specified scope that would match every record"
                .to_string(),
        );
    }
    let shard_size = shard_size.max(1);
    let count = read_record_count(engine, count_key)?
        .trim()
        .parse::<u64>()
        .unwrap_or(0);
    let mut stats = ForgetScopeStats::default();
    if count == 0 {
        return Ok(stats);
    }
    let max_shard = (count - 1) / shard_size;
    let mut commands = Vec::new();
    for shard in 0..=max_shard {
        stats.shards_scanned += 1;
        let key = format!("{}:{:06}", record_hash_key, shard);
        for (field, value) in hgetall_map(engine, key.clone())? {
            let records = decode_matrixark_payload(&value);
            if records.is_empty() {
                // Undecodable / non-record field (e.g. a counter): never touch it.
                continue;
            }
            stats.records_scanned += records.len();
            stats.fields_visited += 1;
            let mut survivors = Vec::with_capacity(records.len());
            let mut removed_here = 0_usize;
            for record in records {
                if scope_matches_record(&record, Some(scope)) {
                    removed_here += 1;
                } else {
                    survivors.push(record);
                }
            }
            if removed_here == 0 {
                stats.fields_without_match += 1;
                continue;
            }
            stats.records_removed += removed_here;
            if survivors.is_empty() {
                // The field is going away entirely: its type-index entries go in the same batch.
                // A partial rewrite leaves its entries alone -- an index-served fetch re-filters
                // everything it loads, so a stale entry is a wasted read, not a wrong answer.
                for record_type in payload_record_types(&value) {
                    commands.push(Command::HashDelete {
                        key: type_index_key(record_hash_key, &record_type),
                        field: format!("{shard:06}:{field}"),
                    });
                }
                for record in decode_matrixark_payload(&value) {
                    for bucket in record_scope_buckets(&record) {
                        commands.push(Command::HashDelete {
                            key: scope_index_key(record_hash_key, &bucket),
                            field: format!("{shard:06}:{field}"),
                        });
                    }
                }
                commands.push(Command::HashDelete {
                    key: key.clone(),
                    field,
                });
                stats.fields_deleted += 1;
            } else {
                let encoded = encode_forget_survivors(&value, survivors);
                commands.push(Command::HashSet {
                    key: key.clone(),
                    field,
                    value: encoded.into_bytes(),
                });
                stats.fields_rewritten += 1;
            }
        }
    }
    if !commands.is_empty() {
        // One durable batch: WAL-committed together, and it clears the process-global scan +
        // hgetall snapshot caches so a subsequent retrieve/get_all never re-serves a forgotten
        // record from cache.
        execute_empty_batch_runtime(engine, commands, true)?;
    }
    Ok(stats)
}

/// Every id a record can be addressed by: its own identity, and any reference it carries.
///
/// A delete removes the addressed record AND the embeddings / index postings that point at it --
/// those carry no identity of their own, only a `ref_hash` / `ref_hashes` aimed at one. Matching
/// both is what stops a delete leaving orphaned postings behind that still surface its text.
/// Write a u64 as text into a caller-owned buffer. No allocation.
///
/// Exists because the ids in these records are mostly JSON numbers (`event_id_hash`,
/// `entity_hash`, ...) and `Number::to_string()` allocates a String for each one -- which is a lot
/// of allocation to answer a question that only needs a comparison.
fn u64_into<'a>(buf: &'a mut [u8; 20], mut value: u64) -> &'a str {
    if value == 0 {
        buf[0] = b'0';
        return std::str::from_utf8(&buf[..1]).unwrap_or("0");
    }
    let mut at = buf.len();
    while value > 0 {
        at -= 1;
        buf[at] = b'0' + (value % 10) as u8;
        value /= 10;
    }
    std::str::from_utf8(&buf[at..]).unwrap_or("")
}

/// Does this record carry any id the caller is looking for?
///
/// Same identity fields as [`record_addressable_ids`], but answers the membership question without
/// building anything. That function allocates a `Vec<String>` per record plus a `String` per id it
/// finds, and both call sites immediately threw all of it away after an `.any(...)`.
///
/// It is on the hot loop of `update`: a purge at 360 memories parses 7,829 records to remove 663,
/// and asked this question of every one of them.
///
/// `contains` is a closure rather than a set so the two callers can keep the set types they
/// already have -- one holds `&str`, the other `String`.
fn record_carries_wanted_id(record: &Value, contains: impl Fn(&str) -> bool) -> bool {
    let mut hit = |value: Option<&Value>| -> bool {
        match value {
            Some(Value::String(text)) if !text.is_empty() => contains(text.as_str()),
            Some(Value::Number(number)) => {
                if let Some(unsigned) = number.as_u64() {
                    let mut buf = [0_u8; 20];
                    contains(u64_into(&mut buf, unsigned))
                } else {
                    // Signed or floating: rare for an id, so the allocation here is not worth
                    // avoiding, and matching `to_string` keeps the answer identical.
                    contains(number.to_string().as_str())
                }
            }
            _ => false,
        }
    };
    for field in [
        "event_id_hash",
        "entity_hash",
        "summary_hash",
        "segment_hash",
        "ref_hash",
    ] {
        if hit(record.get(field)) {
            return true;
        }
    }
    if let Some(Value::Array(refs)) = record.get("ref_hashes") {
        for item in refs {
            if hit(Some(item)) {
                return true;
            }
        }
    }
    false
}

/// Does `record` merely POINT AT one of the wanted ids, without carrying it?
///
/// The located set is built from both kinds of relationship: a record is filed under the ids it
/// carries AND under the ids it names as sources or targets. Deletion needs both filed -- a
/// derivative is only findable through its source event, because it is not filed under its own
/// identity hash -- but deletion only REMOVES the carriers. So a located field can be perfectly
/// legitimate and still hold nothing to remove.
///
/// That makes two very different situations look identical in `fields_without_match`, and they
/// have different fixes: a field still holding records that point at these ids is a location the
/// index is right to keep, while a field where nothing so much as mentions them is an entry that
/// has gone stale and could be dropped. This tells them apart. Mirrors the writer's pointed-id
/// fields exactly; a field the writer files under and this misses would misreport a live location
/// as stale.
fn record_points_at_wanted_id(record: &Value, contains: impl Fn(&str) -> bool) -> bool {
    let mut hit = |value: Option<&Value>| -> bool {
        match value {
            Some(Value::String(text)) if !text.is_empty() => contains(text.as_str()),
            Some(Value::Number(number)) => {
                if let Some(unsigned) = number.as_u64() {
                    let mut buf = [0_u8; 20];
                    contains(u64_into(&mut buf, unsigned))
                } else {
                    contains(number.to_string().as_str())
                }
            }
            _ => false,
        }
    };
    for field in ["source_event_hash", "target_memory_id", "superseded_by"] {
        if hit(record.get(field)) {
            return true;
        }
    }
    for field in ["source_event_ids", "source_refs"] {
        if let Some(Value::Array(values)) = record.get(field) {
            for item in values {
                if hit(Some(item)) {
                    return true;
                }
            }
        }
    }
    false
}

/// Every field the WRITER files a record under, so "is this location still described by this id"
/// can be answered the same way the entry was created.
///
/// This deliberately does NOT reuse the purge's matcher. The two ask different questions and the
/// writer's set is wider: it files under `chunk_hash`, `section_hash`, `skill_hash`,
/// `resource_hash` and `batch_id_hash`, none of which deletion looks at. The located set has two
/// consumers -- deletion, and the id-scoped read that serves retrieval -- so an entry that
/// deletion finds uninteresting can still be the only route by which a read finds its record.
/// Deciding staleness with the narrower set would drop those entries and quietly cost retrieval
/// rows while deletion stayed correct.
///
/// So the rule here is: keep the entry if ANY field the writer files under still matches. Being a
/// superset is safe (an entry is kept), being a subset is not (an entry is dropped), which is why
/// `entity_hash` and `segment_hash` are included even though the writer does not file under them.
/// `locator_filing_fields_are_covered` pins the list against the writer's.
fn record_filed_under_id(record: &Value, contains: impl Fn(&str) -> bool) -> bool {
    let mut hit = |value: Option<&Value>| -> bool {
        match value {
            Some(Value::String(text)) if !text.is_empty() => contains(text.as_str()),
            Some(Value::Number(number)) => {
                if let Some(unsigned) = number.as_u64() {
                    let mut buf = [0_u8; 20];
                    contains(u64_into(&mut buf, unsigned))
                } else {
                    contains(number.to_string().as_str())
                }
            }
            _ => false,
        }
    };
    for field in LOCATOR_FILING_SCALAR_FIELDS {
        if hit(record.get(*field)) {
            return true;
        }
    }
    for field in LOCATOR_FILING_LIST_FIELDS {
        if let Some(Value::Array(values)) = record.get(*field) {
            for item in values {
                if hit(Some(item)) {
                    return true;
                }
            }
        }
    }
    false
}

/// The writer's scalar filing fields, plus the two identity hashes deletion matches on.
const LOCATOR_FILING_SCALAR_FIELDS: &[&str] = &[
    // context_index_ref_hashes
    "ref_hash",
    "event_id_hash",
    "chunk_hash",
    "section_hash",
    "skill_hash",
    "resource_hash",
    "summary_hash",
    "batch_id_hash",
    // record_pointed_ref_ids
    "source_event_hash",
    "target_memory_id",
    "superseded_by",
    // not filed under by the writer; kept because dropping an entry is the unsafe direction
    "entity_hash",
    "segment_hash",
];

const LOCATOR_FILING_LIST_FIELDS: &[&str] = &["ref_hashes", "source_event_ids", "source_refs"];

/// Drop the located entries that this purge proved no longer describe where they point.
///
/// Only called for `(id, location)` pairs whose field was decoded in full during this call, so
/// "nothing here is filed under this id" is read off the field's actual contents rather than
/// inferred. Chunk boundaries are left exactly as the writer laid them out -- each chunk is
/// filtered in place and written back, and `location_chunks` is untouched -- so no second opinion
/// about how the list should be split can drift from the writer's.
fn prune_locator_locations(
    engine: &RecordStore,
    record_hash_key: &str,
    stale: &BTreeMap<String, BTreeSet<(String, String)>>,
    commands: &mut Vec<Command>,
) -> Result<usize, String> {
    let Some(prefix) = record_hash_key.strip_suffix(":records") else {
        return Ok(0);
    };
    let locator_key = format!("{prefix}:context_ref_locator");
    let mut dropped = 0_usize;

    let drop_here = |location: &Value, gone: &BTreeSet<(String, String)>| -> bool {
        // Anything this call cannot resolve to a location in THIS record log is left alone.
        match location_shard_and_field(location, record_hash_key) {
            Some(pair) => gone.contains(&pair),
            None => false,
        }
    };

    for (id, gone) in stale {
        let mut filter_field = |field: String| -> Result<(), String> {
            let raw = read_bytes(
                engine,
                Command::HashGet {
                    key: locator_key.clone(),
                    field: field.clone(),
                },
            )?;
            if raw.is_empty() {
                return Ok(());
            }
            let Ok(mut decoded) = serde_json::from_str::<Value>(&raw) else {
                return Ok(());
            };
            let Some(list) = decoded.get_mut("locations").and_then(Value::as_array_mut) else {
                return Ok(());
            };
            let before = list.len();
            list.retain(|location| !drop_here(location, gone));
            let removed = before - list.len();
            if removed == 0 {
                return Ok(());
            }
            dropped += removed;
            commands.push(Command::HashSet {
                key: locator_key.clone(),
                field,
                value: decoded.to_string().into_bytes(),
            });
            Ok(())
        };

        // The head field carries the chunk count, so read it before filtering it.
        let head = read_bytes(
            engine,
            Command::HashGet {
                key: locator_key.clone(),
                field: id.clone(),
            },
        )?;
        if head.is_empty() {
            continue;
        }
        let chunks = serde_json::from_str::<Value>(&head)
            .ok()
            .and_then(|value| value.get("location_chunks").and_then(Value::as_u64))
            .unwrap_or(0);
        filter_field(id.clone())?;
        for index in 1..=chunks {
            filter_field(format!("{id}#{index}"))?;
        }
    }
    Ok(dropped)
}

fn record_addressable_ids(record: &Value) -> Vec<String> {
    let mut ids = Vec::new();
    let mut push = |value: Option<&Value>| {
        if let Some(value) = value {
            match value {
                Value::String(text) if !text.is_empty() => ids.push(text.clone()),
                Value::Number(number) => ids.push(number.to_string()),
                _ => {}
            }
        }
    };
    for field in [
        "event_id_hash",
        "entity_hash",
        "summary_hash",
        "segment_hash",
        "ref_hash",
    ] {
        push(record.get(field));
    }
    if let Some(Value::Array(refs)) = record.get("ref_hashes") {
        for item in refs {
            match item {
                Value::String(text) if !text.is_empty() => ids.push(text.clone()),
                Value::Number(number) => ids.push(number.to_string()),
                _ => {}
            }
        }
    }
    ids
}

/// Remove every record addressable by one of `ids`.
///
/// Mirrors `forget_scope_records` -- same shard walk, same survivor rewrite, same single durable
/// batch that also clears the scan/hgetall caches so a later retrieve cannot re-serve a removed
/// record from cache. Only the predicate differs: identity ids instead of a scope.
/// The `(shard, field)` locations the ref locator holds for `ids`, or `None` when the locator
/// cannot be trusted to be complete for this store.
///
/// Completeness is the whole question: visiting only located fields is correct exactly when the
/// locator saw every record this store ever wrote, which is what the `provenance_from_start`
/// marker attests (it is stamped by the batch that writes the store's first record). Without it
/// the caller must walk, because a missed field would leave a deleted record physically present.
fn located_fields_for_ids(
    engine: &RecordStore,
    record_hash_key: &str,
    ids: &[String],
) -> Result<Option<BTreeMap<String, Vec<String>>>, String> {
    let Some(prefix) = record_hash_key.strip_suffix(":records") else {
        return Ok(None);
    };
    let locator_key = format!("{prefix}:context_ref_locator");
    let covered = hgetall_map(engine, format!("{locator_key}_meta"))?
        .get("provenance_from_start")
        .map(|value| value.trim() == "1")
        .unwrap_or(false);
    if !covered {
        return Ok(None);
    }
    let mut by_shard: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for id in ids {
        for location in locator_location_values(engine, &locator_key, id)? {
            let Some((shard, field)) = location_shard_and_field(&location, record_hash_key) else {
                continue;
            };
            let fields = by_shard.entry(shard).or_default();
            if !fields.iter().any(|existing| existing == &field) {
                fields.push(field);
            }
        }
    }
    Ok(Some(by_shard))
}

fn delete_records_by_ids(
    engine: &RecordStore,
    record_hash_key: &str,
    count_key: &str,
    shard_size: u64,
    ids: &[String],
) -> Result<ForgetScopeStats, String> {
    let wanted: HashSet<&str> = ids.iter().map(String::as_str).collect();
    let mut stats = ForgetScopeStats::default();
    if wanted.is_empty() {
        // An empty id set must remove NOTHING. Falling through to a match-everything walk here
        // would turn a no-op delete into a store wipe.
        return Ok(stats);
    }
    let shard_size = shard_size.max(1);
    let count = read_record_count(engine, count_key)?
        .trim()
        .parse::<u64>()
        .unwrap_or(0);
    if count == 0 {
        return Ok(stats);
    }
    let max_shard = (count - 1) / shard_size;
    let mut commands = Vec::new();
    // Located entries this call can prove no longer describe where they point: the field was
    // decoded here in full and nothing in it is filed under that id.
    let mut stale: BTreeMap<String, BTreeSet<(String, String)>> = BTreeMap::new();
    // Which fields could hold these ids? The locator answers directly on a store it covers
    // completely; otherwise every shard has to be read.
    let located = located_fields_for_ids(engine, record_hash_key, ids)?;
    let visit: Vec<(String, Vec<String>)> = match &located {
        Some(by_shard) => by_shard
            .iter()
            .map(|(shard, fields)| (shard.clone(), fields.clone()))
            .collect(),
        None => (0..=max_shard)
            .map(|shard| (format!("{shard:06}"), Vec::new()))
            .collect(),
    };
    for (shard, only_fields) in visit {
        stats.shards_scanned += 1;
        let key = format!("{}:{}", record_hash_key, shard);
        // With located fields, read exactly those. Without them (no locator coverage) the whole
        // shard is genuinely needed, because the walk is the fallback that finds the records the
        // locator could not name.
        let entries: Vec<(String, String)> = if only_fields.is_empty() {
            hgetall_map(engine, key.clone())?.into_iter().collect()
        } else {
            let found = fetch_shard_fields(engine, key.clone(), &only_fields)?;
            only_fields
                .into_iter()
                .filter_map(|field| found.get(&field).map(|value| (field, value.clone())))
                .collect()
        };
        for (field, value) in entries {
            let records = decode_matrixark_payload(&value);
            if records.is_empty() {
                // Undecodable / non-record field (e.g. a counter): never touch it.
                continue;
            }
            stats.records_scanned += records.len();
            stats.fields_visited += 1;
            let mut survivors = Vec::with_capacity(records.len());
            let mut removed_here = 0_usize;
            for record in records {
                if record_carries_wanted_id(&record, |id| wanted.contains(id)) {
                    removed_here += 1;
                } else {
                    survivors.push(record);
                }
            }
            // This field has been decoded in full, so for each id it is now known -- not guessed --
            // whether anything here is still filed under it. Judged on the SURVIVORS, since the
            // records being removed are about to stop being here.
            for id in ids {
                let still_filed = survivors
                    .iter()
                    .any(|record| record_filed_under_id(record, |candidate| candidate == id));
                if !still_filed {
                    stale
                        .entry(id.clone())
                        .or_default()
                        .insert((shard.clone(), field.clone()));
                }
            }
            if removed_here == 0 {
                stats.fields_without_match += 1;
                // Nothing was removed, so `survivors` is still every record this field holds.
                if survivors
                    .iter()
                    .any(|record| record_points_at_wanted_id(record, |id| wanted.contains(id)))
                {
                    stats.fields_pointed_only += 1;
                }
                continue;
            }
            stats.records_removed += removed_here;
            if survivors.is_empty() {
                // The field is going away entirely: its type-index entries go in the same batch.
                // A partial rewrite leaves its entries alone -- an index-served fetch re-filters
                // everything it loads, so a stale entry is a wasted read, not a wrong answer.
                for record_type in payload_record_types(&value) {
                    commands.push(Command::HashDelete {
                        key: type_index_key(record_hash_key, &record_type),
                        field: format!("{shard}:{field}"),
                    });
                }
                for record in decode_matrixark_payload(&value) {
                    for bucket in record_scope_buckets(&record) {
                        commands.push(Command::HashDelete {
                            key: scope_index_key(record_hash_key, &bucket),
                            field: format!("{shard}:{field}"),
                        });
                    }
                }
                commands.push(Command::HashDelete {
                    key: key.clone(),
                    field,
                });
                stats.fields_deleted += 1;
            } else {
                let encoded = encode_forget_survivors(&value, survivors);
                commands.push(Command::HashSet {
                    key: key.clone(),
                    field,
                    value: encoded.into_bytes(),
                });
                stats.fields_rewritten += 1;
            }
        }
    }
    // Same durable batch as the record removals: the entries and the records they describe go
    // together, so a crash cannot leave the index pointing at content that is already gone.
    if !stale.is_empty() {
        stats.locator_locations_dropped =
            prune_locator_locations(engine, record_hash_key, &stale, &mut commands)?;
    }
    if !commands.is_empty() {
        execute_empty_batch_runtime(engine, commands, true)?;
    }
    Ok(stats)
}

fn candidate_text(record: &Value) -> String {
    for field in [
        "text",
        "content",
        "summary_text",
        "state",
        "observation",
        "entity_value",
        "description",
        "value",
    ] {
        if let Some(text) = record.get(field).and_then(Value::as_str) {
            if !text.is_empty() {
                return text.to_string();
            }
        }
    }
    if let Some(text) =
        json_field(record, &["internal_extraction", "observation"]).and_then(Value::as_str)
    {
        if !text.is_empty() {
            return text.to_string();
        }
    }
    String::new()
}

fn token_estimate(text: &str) -> u64 {
    ((text.len() as u64 + 3) / 4).max(1)
}

fn sparse_query_score(query_terms: &HashSet<String>, text: &str) -> f64 {
    if query_terms.is_empty() || text.is_empty() {
        return 0.0;
    }
    let lower = text.to_ascii_lowercase();
    let hits = query_terms
        .iter()
        .filter(|term| lower.contains(term.as_str()))
        .count() as f64;
    (hits / query_terms.len() as f64).clamp(0.0, 1.0)
}

fn context_class_name(record: &Value) -> String {
    let record_type = record
        .get("record_type")
        .and_then(Value::as_str)
        .unwrap_or("");
    if record_type == "context_event" {
        let classification = record
            .get("classification")
            .and_then(Value::as_str)
            .unwrap_or("");
        let event_type = record
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or("");
        if classification == "resource_fact" || event_type.starts_with("resource_") {
            return "resource_fact".to_string();
        }
        return "event".to_string();
    }
    match record_type {
        "context_entity" => "entity".to_string(),
        "context_segment" => "segment".to_string(),
        "context_summary" => "summary".to_string(),
        "context_compression_event" => "compression".to_string(),
        other => other.to_string(),
    }
}

fn is_serving_selected_ref_class(context_class: &str) -> bool {
    matches!(context_class, "entity" | "event" | "summary")
}

fn increment_class_count(counts: &mut HashMap<String, u64>, class_name: &str) {
    *counts.entry(class_name.to_string()).or_default() += 1;
}

fn increment_class_tokens(tokens_by_class: &mut HashMap<String, u64>, class_name: &str, tokens: u64) {
    *tokens_by_class.entry(class_name.to_string()).or_default() += tokens;
}

fn broad_memory_layer(record: &Value, ref_type: &str) -> String {
    if let Some(layer) = record
        .get("memory_layer")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return layer.to_string();
    }
    let sharing_scope = record
        .get("sharing_scope")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    if matches!(sharing_scope, "tenant_shared" | "global_shared")
        || matches!(
            ref_type,
            "resource" | "resource_chunk" | "resource_fact" | "resource_entity_fact" | "skill" | "skill_section"
        )
    {
        return "shared_context".to_string();
    }
    let memory_scope = record
        .get("memory_scope")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    if matches!(memory_scope, "user_profile" | "profile" | "cross_session_profile") {
        return "profile".to_string();
    }
    if matches!(memory_scope, "session" | "session_memory") {
        return "session".to_string();
    }
    let session_continuity = record
        .get("session_continuity")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    if session_continuity == "same_session" {
        return "session".to_string();
    }
    if session_continuity == "cross_session" {
        if ref_type == "entity" {
            return "profile".to_string();
        }
        return "cross_session".to_string();
    }
    String::new()
}

fn pack_ref_from_record(
    record: &Value,
    text: &str,
    context_class: &str,
    score: f64,
    reason: &str,
    session_continuity: &str,
    continuity_boost_value: f64,
    cross_session_rerank_boost_value: f64,
) -> Value {
    let continuity_reason = match session_continuity {
        "same_session" => "same-session continuity",
        "cross_session" => "cross-session memory bridge",
        _ => "session-neutral context",
    };
    json!({
        "ref_type": context_class,
        "ref_hash": record_ref_hash(record).unwrap_or_else(|| record.get("record_id").and_then(Value::as_str).unwrap_or("").to_string()),
        "node_hash": record_node_hash(record),
        "node_path": record.get("node_path").cloned().unwrap_or_else(|| json!([])),
        "text": text,
        "token_estimate": token_estimate(text),
        "score": (score * 1000000.0).round() / 1000000.0,
        "session_continuity": session_continuity,
        "continuity_boost": (continuity_boost_value * 1000000.0).round() / 1000000.0,
        "cross_session_rerank_boost": (cross_session_rerank_boost_value * 1000000.0).round() / 1000000.0,
        "continuity_reason": continuity_reason,
        "selection_reason": reason,
        "memory_layer": broad_memory_layer(record, context_class),
        "memory_scope": record.get("memory_scope").and_then(Value::as_str).unwrap_or(""),
        "extraction_phase": record.get("extraction_phase").and_then(Value::as_str).unwrap_or(""),
        "final_session_boundary": record.get("final_session_boundary").and_then(Value::as_bool).unwrap_or(false),
        "entity_type": record.get("entity_type").and_then(Value::as_str).unwrap_or(""),
        "entity_name": record.get("entity_name").and_then(Value::as_str).unwrap_or(""),
        "source_roles": record.get("source_roles").cloned().unwrap_or_else(|| json!([])),
        "source_role_counts": record.get("source_role_counts").cloned().unwrap_or_else(|| json!({})),
        "source_hook_types": record.get("source_hook_types").cloned().unwrap_or_else(|| json!([])),
        "source_hook_type_counts": record.get("source_hook_type_counts").cloned().unwrap_or_else(|| json!({})),
        "source_codex_events": record.get("source_codex_events").cloned().unwrap_or_else(|| json!([])),
        "source_codex_event_counts": record.get("source_codex_event_counts").cloned().unwrap_or_else(|| json!({})),
        "source_session_ids": record.get("source_session_ids").cloned().unwrap_or_else(|| json!([])),
        "source_entity_hashes": record.get("source_entity_hashes").cloned().unwrap_or_else(|| json!([])),
        "source_entity_types": record.get("source_entity_types").cloned().unwrap_or_else(|| json!([])),
        "source_memory_scopes": record.get("source_memory_scopes").cloned().unwrap_or_else(|| json!([])),
        "source_session_continuities": record.get("source_session_continuities").cloned().unwrap_or_else(|| json!([])),
        "source_extraction_phases": record.get("source_extraction_phases").cloned().unwrap_or_else(|| json!([])),
        "source_profile_promotion_policies": record.get("source_profile_promotion_policies").cloned().unwrap_or_else(|| json!([])),
        "source_profile_promotion_blockers": record.get("source_profile_promotion_blockers").cloned().unwrap_or_else(|| json!([])),
        "source_ref": record.get("source_ref").cloned().unwrap_or(Value::Null),
    })
}

include!("matrixark_rust_proxy_impl/native_serving.rs");

include!("matrixark_rust_proxy_impl/retrieve_pack.rs");
fn execute_record_log_request(
    engine: &RecordStore,
    request: RecordLogRequest,
    root: PathBuf,
) -> Result<RecordLogOutput, String> {
    let output = match request.op.as_str() {
        "health" | "preflight" => RecordLogOutput {
            value: "ready".to_string(),
            entries: BTreeMap::new(),
            records: Vec::new(),
            count: Some(0),
            root,
            status: "ready".to_string(),
            mode: "single_shot".to_string(),
            append_path: String::new(),
            raw_storage_backend: String::new(),
            prometheus: String::new(),
            cached_clients: None,
            extra: BTreeMap::new(),
        },
        // Attachment blob tier, engine command side: the python surface reaches the
        // embedded engine's content-addressed blob store through these ops, mirroring the
        // datanode's HTTP /blob tier for deployments that run no HTTP server. Payloads ride
        // base64 in `value`; structured results ride the flattened extra map.
        "matrixark_resource_blob_put" => {
            let tenant_hash: u64 = request
                .key
                .trim()
                .parse()
                .map_err(|_| format!("blob put needs a decimal tenant hash in key, got {:?}", request.key))?;
            let response = engine.execute(ExecuteRequest {
                shard_id: DEFAULT_SHARD_ID,
                command: Command::ContextResourceBlobPut {
                    tenant_hash,
                    payload_base64: request.value.clone(),
                },
            });
            if !response.status.ok {
                return Err(format!("{}: {}", response.status.code, response.status.message));
            }
            let mut output = empty_output(root);
            if let CommandResponse::ContextResourceBlobCommitted { uri, size_bytes, content_hash } = response.response {
                output.status = "committed".to_string();
                output.extra.insert("matrixark_blob_uri".to_string(), json!(uri));
                output.extra.insert("matrixark_blob_size_bytes".to_string(), json!(size_bytes));
                output.extra.insert(
                    "matrixark_blob_content_hash".to_string(),
                    json!(format!("{content_hash:016x}")),
                );
            }
            output
        }
        "matrixark_resource_blob_fetch" => {
            let response = engine.execute(ExecuteRequest {
                shard_id: DEFAULT_SHARD_ID,
                command: Command::ContextResourceBlobFetch {
                    uri: request.key.clone(),
                    offset: request.blob_offset.unwrap_or(0),
                    length: request.blob_length.unwrap_or(0),
                },
            });
            if !response.status.ok {
                return Err(format!("{}: {}", response.status.code, response.status.message));
            }
            let mut output = empty_output(root);
            if let CommandResponse::ContextResourceBlobChunk { payload_base64, total_size, eof } = response.response {
                output.status = "served".to_string();
                output.value = payload_base64;
                output.extra.insert("matrixark_blob_total_size".to_string(), json!(total_size));
                output.extra.insert("matrixark_blob_eof".to_string(), json!(eof));
            }
            output
        }
        "matrixark_resource_blob_sweep" => {
            let tenant_hash: u64 = request
                .key
                .trim()
                .parse()
                .map_err(|_| format!("blob sweep needs a decimal tenant hash in key, got {:?}", request.key))?;
            let referenced: Vec<u64> = request
                .blob_referenced_hashes
                .clone()
                .unwrap_or_default()
                .iter()
                .filter_map(|hex| u64::from_str_radix(hex.trim(), 16).ok())
                .collect();
            let response = engine.execute(ExecuteRequest {
                shard_id: DEFAULT_SHARD_ID,
                command: Command::ContextResourceBlobSweep {
                    tenant_hash,
                    referenced_content_hashes: referenced,
                    min_age_ms: request.blob_min_age_ms.unwrap_or(3_600_000),
                },
            });
            if !response.status.ok {
                return Err(format!("{}: {}", response.status.code, response.status.message));
            }
            let mut output = empty_output(root);
            if let CommandResponse::ContextResourceBlobSwept { scanned, deleted } = response.response {
                output.status = "swept".to_string();
                output.extra.insert("matrixark_blob_scanned".to_string(), json!(scanned));
                output.extra.insert("matrixark_blob_deleted".to_string(), json!(deleted));
            }
            output
        }
        "matrixark_publish_visibility" => {
            let visibility_key_count = request.visibility_keys.len();
            let index_bytes = engine
                .publish_shard_index_snapshot_for_keys(
                    DEFAULT_SHARD_ID,
                    request.visibility_keys.clone(),
                )
                .map_err(|status| format!("{}: {}", status.code, status.message))?;
            clear_matrixark_scan_cache();
            let mut output = empty_output(root);
            output.status = "published".to_string();
            output.count = Some(index_bytes);
            output
                .extra
                .insert("matrixark_visibility_published".to_string(), json!(true));
            output.extra.insert(
                "matrixark_visibility_index_bytes".to_string(),
                json!(index_bytes),
            );
            // WHETHER THIS CALL WROTE A SERVED-INDEX CHECKPOINT, stated rather than inferred from
            // the byte count above. The checkpoint now runs per published key rather than per
            // call (see `VISIBILITY_PUBLISH_INDEX_FLUSH_INTERVAL_DEFAULT`), so a deferred call
            // reports zero index bytes -- and so does a call that found nothing log-resident.
            // Reading "published nothing" out of a zero would be wrong in both cases, because the
            // pages are republished before the call returns either way; this names what the zero
            // is about.
            output.extra.insert(
                "matrixark_visibility_index_checkpointed".to_string(),
                json!(index_bytes > 0),
            );
            output.extra.insert(
                "matrixark_visibility_key_count".to_string(),
                json!(visibility_key_count),
            );
            output.extra.insert(
                "matrixark_visibility_full_shard".to_string(),
                json!(visibility_key_count == 0),
            );
            output.extra.insert(
                "matrixark_visibility_scope".to_string(),
                json!("shard_index_snapshot"),
            );
            output
        }
        "put_string" => {
            execute_empty(
                &engine,
                Command::StringSet {
                    key: request.key,
                    value: request.value.into_bytes(),
                },
            )?;
            empty_output(root)
        }
        "get_string" => value_output(
            read_bytes(&engine, Command::StringGet { key: request.key })?,
            root,
        ),
        "delete" | "del" => {
            execute_empty(&engine, Command::CommonDelete { key: request.key })?;
