// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::{Deserialize, Serialize};

pub type ShardId = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Status {
    pub ok: bool,
    pub code: String,
    pub message: String,
}

impl Status {
    pub fn ok() -> Self {
        Self {
            ok: true,
            code: "ok".to_string(),
            message: String::new(),
        }
    }

    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            ok: false,
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeaturePoint {
    pub timestamp_ms: u64,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SequenceFeatureRow {
    pub timestamp_ms: u64,
    pub gid: u64,
    pub action_type: u32,
    pub duration: u32,
    pub author_id: u64,
}

impl SequenceFeatureRow {
    pub fn encode_feature_proto_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.gid);
        encode_varint_field(&mut out, 2, self.action_type as u64);
        encode_varint_field(&mut out, 3, self.duration as u64);
        encode_varint_field(&mut out, 4, self.author_id);
        out
    }

    pub fn decode_feature_proto_value(timestamp_ms: u64, value: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut gid = None;
        let mut action_type = None;
        let mut duration = None;
        let mut author_id = None;
        while cursor < value.len() {
            let tag = decode_varint(value, &mut cursor)?;
            let field = tag >> 3;
            let wire_type = tag & 0x7;
            match (field, wire_type) {
                (1, 0) => gid = Some(decode_varint(value, &mut cursor)?),
                (2, 0) => action_type = u32::try_from(decode_varint(value, &mut cursor)?).ok(),
                (3, 0) => duration = u32::try_from(decode_varint(value, &mut cursor)?).ok(),
                (4, 0) => author_id = Some(decode_varint(value, &mut cursor)?),
                (_, 0) => {
                    let _ = decode_varint(value, &mut cursor)?;
                }
                (_, 1) => cursor = cursor.checked_add(8)?,
                (_, 2) => {
                    let len = usize::try_from(decode_varint(value, &mut cursor)?).ok()?;
                    cursor = cursor.checked_add(len)?;
                }
                (_, 5) => cursor = cursor.checked_add(4)?,
                _ => return None,
            }
            if cursor > value.len() {
                return None;
            }
        }
        Some(Self {
            timestamp_ms,
            gid: gid?,
            action_type: action_type?,
            duration: duration?,
            author_id: author_id?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FeatureFilterOp {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterOrEqual,
    LessThan,
    LessOrEqual,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StringSetCondition {
    Always,
    IfExists,
    IfNotExists,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FeatureWritePolicy {
    Upsert,
    #[serde(alias = "FIRST", alias = "first", alias = "NX", alias = "nx")]
    InsertIfAbsent,
    #[serde(alias = "UPDATE", alias = "update", alias = "XX", alias = "xx")]
    ReplaceExisting,
    #[serde(alias = "BLOCK", alias = "block")]
    Block,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeatureFilter {
    pub field: String,
    pub op: FeatureFilterOp,
    pub value: u64,
}

impl FeatureFilter {
    pub fn parse_feature_filter(value: &str) -> Result<Self, String> {
        let mut parts = value.split_whitespace();
        let field = parts
            .next()
            .ok_or_else(|| "filter must be '<field> <op> <value>'".to_string())?;
        let op = parts
            .next()
            .ok_or_else(|| "filter must be '<field> <op> <value>'".to_string())?;
        let raw_value = parts
            .next()
            .ok_or_else(|| "filter must be '<field> <op> <value>'".to_string())?;
        if parts.next().is_some() {
            return Err("filter must be '<field> <op> <value>'".to_string());
        }
        if !matches!(field, "gid" | "action_type" | "duration" | "author_id") {
            return Err(format!("unknown feature field '{field}'"));
        }
        let op = match op {
            "=" | "==" => FeatureFilterOp::Equal,
            "!=" => FeatureFilterOp::NotEqual,
            ">" => FeatureFilterOp::GreaterThan,
            ">=" => FeatureFilterOp::GreaterOrEqual,
            "<" => FeatureFilterOp::LessThan,
            "<=" => FeatureFilterOp::LessOrEqual,
            _ => return Err(format!("unsupported feature filter op '{op}'")),
        };
        let value = raw_value
            .parse::<u64>()
            .map_err(|_| format!("feature filter value '{raw_value}' is not uint64"))?;
        Ok(Self {
            field: field.to_string(),
            op,
            value,
        })
    }
}

pub fn parse_feature_filters<'a>(
    filters: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<FeatureFilter>, String> {
    let mut parsed = Vec::new();
    for filter in filters
        .into_iter()
        .filter(|filter| !filter.trim().is_empty())
    {
        let filter = FeatureFilter::parse_feature_filter(filter)?;
        if let Some(index) = parsed
            .iter()
            .position(|existing: &FeatureFilter| existing.field == filter.field)
        {
            parsed[index] = filter;
        } else {
            parsed.push(filter);
        }
    }
    Ok(parsed)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SequenceQuerySpec {
    pub key: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub count: usize,
    #[serde(default)]
    pub filters: Vec<FeatureFilter>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ControlStateFamily {
    // Wire/disk form is pinned to the historical historical tags (h/cpc/fol) so this
    // rename to descriptive Rust names carries zero on-disk/wire migration.
    #[serde(rename = "h")]
    Counter,
    #[serde(rename = "cpc")]
    Distinct,
    #[serde(rename = "fol")]
    Selection,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ControlStateSelectionType {
    First,
    Last,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextNode {
    pub node_hash: u64,
    #[serde(default)]
    pub parent_hash: u64,
    #[serde(default)]
    pub kind: u32,
    #[serde(default)]
    pub canonical_name: String,
    #[serde(default)]
    pub l0: String,
    // Deprecated hot-schema field: retained for legacy Rust inputs only.
    #[serde(default, skip_serializing)]
    pub status: u32,
    #[serde(default)]
    pub last_event_time_ms: u64,
    // Deprecated hot-schema field: use ContextSummary records instead.
    #[serde(default, skip_serializing)]
    pub l1_ref: String,
    // Deprecated hot-schema field: use resource/provenance sidecars instead.
    #[serde(default, skip_serializing)]
    pub raw_metadata_ref: String,
    // Inline embedding vector for this node's L0 text, folded in the same way as the
    // vectors already carried by ContextEvent, ContextEntity and ContextSummary.
    //
    // A node's L0 embedding has exactly one owner -- this node -- so storing it apart
    // costs a key, a BlockAddress and a block to hold a value nothing else can reach.
    // Worse, the separate record is addressed by a hash of (tenant, owner, level), which
    // is one-way: given a node there is no way back to its vector except by recomputing
    // that hash, and given the record no way back to the node at all.
    //
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub vector: Vec<f32>,
    // Model that produced `vector`, and when. Both were carried by the separate record;
    // without them a reader cannot tell a vector from the current model apart from one
    // left by a model that has since been replaced.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub embedding_model_hash: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub embedding_updated_at_ms: u64,
    // A COPY of the vector on this node's level-2 (L1) ContextSummary. That summary record is
    // still the only owner; this is a cache, and every reader of it must be able to fall back
    // to the summary when it is absent.
    //
    // It is here because scoring needs both vectors for every candidate, and without it the
    // second one costs a second per-candidate engine call: format an object key, walk the
    // node's summary series, read a page and decode a whole ContextSummary -- text included --
    // to keep one field. The node fetch that scoring already makes for `vector` can hand back
    // both instead.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub summary_vector: Vec<f32>,
    // `valid_from_ms` of the summary `summary_vector` was copied from, and the whole of what
    // makes the copy usable: scoring asks for the newest summary at or before an `as_of_ms`,
    // and only this timestamp says whether the copy is that summary. A query reaching further
    // back than this must consult the summaries, because the copy is the newest one and cannot
    // speak for an earlier point in time.
    //
    // Zero means there is no copy, so every record written before this field existed falls back
    // to the summary lookup exactly as it does today.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub summary_vector_valid_from_ms: u64,
    // Which encoder produced `summary_vector`, copied from the same summary record. Its own
    // field and not `embedding_model_hash` above: the two vectors on this record can have been
    // written by different encoders, and a re-embed replaces one without touching the other.
    //
    // Without it the copy would be the one route into the ranking that skips the encoder check
    // -- and two encoders truncated to the same width produce no length error and no complaint,
    // only a cosine taken across two vector spaces.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub summary_vector_model_hash: u64,
}

/// Summary level carrying the L1 text, i.e. `ContextSummary::level` for an L1 record.
///
/// `ContextQuerySummaries` numbers levels 1 = L0 and 2 = L1, so the L1 summary is level TWO and
/// the off-by-one is easy to write and impossible to see: level 1 exists and returns records, so
/// reading it produces summaries that are simply the wrong ones.
pub const CONTEXT_SUMMARY_LEVEL_L1: u32 = 2;

impl ContextNode {
    /// This node's cached L1 summary vector, when it can answer a query as of `as_of_ms`.
    ///
    /// `None` means the caller must go to the summary records: either nothing has been copied
    /// here yet, or the copy is NEWER than the point in time being asked about. The second case
    /// is not a miss to paper over -- returning the copy anyway would answer a historical query
    /// with a summary written after it.
    pub fn summary_vector_as_of(&self, as_of_ms: u64) -> Option<&[f32]> {
        if self.summary_vector_valid_from_ms == 0
            || self.summary_vector_valid_from_ms > as_of_ms
            || self.summary_vector.is_empty()
        {
            return None;
        }
        Some(&self.summary_vector)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextEvent {
    pub event_id_hash: u64,
    pub event_time_ms: u64,
    #[serde(default)]
    pub ingestion_time_ms: u64,
    // Deprecated hot-schema field: use type/event_type instead.
    #[serde(default, skip_serializing)]
    pub kind: u32,
    #[serde(default, rename = "type", alias = "event_type")]
    pub event_type: u32,
    // Deprecated hot-schema field: reserves this field.
    #[serde(default, skip_serializing)]
    pub actor_hash: u64,
    // Deprecated hot-schema field: use secondary index status_hash instead.
    #[serde(default, skip_serializing)]
    pub status: u32,
    // Deprecated hot-schema field: reserves this field.
    #[serde(default, skip_serializing)]
    pub valid_until_ms: u64,
    #[serde(default)]
    pub confidence: f32,
    #[serde(default)]
    pub importance: f32,
    #[serde(default)]
    pub text: String,
    // Deprecated hot-schema field: use source/resource secondary indexes instead.
    #[serde(default, skip_serializing)]
    pub source_ref: String,
    // Deprecated hot-schema field: use ContextChildRef/link sidecars instead.
    #[serde(default, skip_serializing)]
    pub related_node_hashes: Vec<u64>,
    // Deprecated hot-schema field: use ContextCompressionEvent/debug sidecars instead.
    #[serde(default, skip_serializing)]
    pub compact_attrs: Vec<u8>,
    // The record's embedding vector, carried inline -- its only home.
    //
    // Every embedding is 1:1 with its owner -- measured on one ingest: event_text 6 to 6 events,
    // entity_state + profile_entity_state 6 to 6 entities, session_l0 + batch_l0 2 to 2
    // summaries -- so a separate record per vector costs a key, a BlockAddress and a page to
    // store something with exactly one owner. Retrieval fetches the vector to score a candidate
    // and then the text to pack it; inline, that is ONE read, and the text is ~50 bytes against
    // ~1536 for the vector, about 3% of a fetch already paid for.
    //
    // Default-empty and skipped when empty: a record written before the fold decodes with no
    // vector and simply counts as un-embedded until the backfill re-embeds it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub vector: Vec<f32>,
}

impl ContextEvent {
    pub fn primary_time_ms(&self) -> u64 {
        if self.ingestion_time_ms != 0 {
            self.ingestion_time_ms
        } else {
            self.event_time_ms
        }
    }

    pub fn event_type_code(&self) -> u32 {
        #[allow(deprecated)]
        if self.event_type != 0 {
            self.event_type
        } else {
            self.kind
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextIndexRef {
    pub primary_node_hash: u64,
    pub primary_event_time_ms: u64,
    #[serde(default)]
    pub event_id_hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextIndexLookup {
    pub index_name: String,
    pub index_value_hash: u64,
    #[serde(default)]
    pub scope_hash: u64,
    pub start_time_ms: u64,
    pub end_time_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum InternalContextIndex {
    EventKind,
    Entity,
    Status,
    Source,
    EventTimeBucket,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ContextExtractedEventIndexes {
    #[serde(default)]
    pub scope_hash: u64,
    #[serde(default)]
    pub entity_hashes: Vec<u64>,
    #[serde(default)]
    pub status_hash: u64,
    #[serde(default)]
    pub source_hash: u64,
    #[serde(default)]
    pub event_time_bucket_ms: u64,
    #[serde(default)]
    pub disabled_indexes: Vec<InternalContextIndex>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextAuditRef {
    pub node_hash: u64,
    pub event_time_ms: u64,
    #[serde(default)]
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextPackAudit {
    pub query_id: String,
    pub session_hash: u64,
    pub request_time_ms: u64,
    #[serde(default)]
    pub query_hash: u64,
    #[serde(default)]
    pub max_prompt_tokens: u32,
    #[serde(default)]
    pub selected_tokens: u32,
    #[serde(default)]
    pub selected_refs: Vec<ContextAuditRef>,
    #[serde(default)]
    pub blocked_refs: Vec<ContextAuditRef>,
}

/// A node the summary or embedding worker still owes work on.
///
/// Dirty tracking is a coalescing map keyed by dirty object key, not a log of records: repeated
/// edits to one node update a single entry rather than appending. This is that entry as callers
/// see it, so the coalescing is visible -- how many marks arrived and the span of event times
/// they covered. The record it replaced could carry only one timestamp, so a query had to flatten
/// first and last into a single `event_time_ms` and drop the count entirely.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct ContextDirtyNode {
    pub node_hash: u64,
    pub first_event_time_ms: u64,
    pub last_event_time_ms: u64,
    #[serde(default)]
    pub reason: u32,
    #[serde(default)]
    pub propagate_depth: u32,
    #[serde(default)]
    pub mark_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextEntity {
    pub entity_hash: u64,
    pub node_hash: u64,
    #[serde(default, rename = "type", alias = "entity_type")]
    pub entity_type: u32,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub value: String,
    pub updated_at_ms: u64,
    #[serde(default)]
    pub valid_from_ms: u64,
    #[serde(default)]
    pub confidence: f32,
    #[serde(default)]
    pub source_event_hashes: Vec<u64>,
    // The record's embedding vector, carried inline -- its only home.
    //
    // Every embedding is 1:1 with its owner -- measured on one ingest: event_text 6 to 6 events,
    // entity_state + profile_entity_state 6 to 6 entities, session_l0 + batch_l0 2 to 2
    // summaries -- so a separate record per vector costs a key, a BlockAddress and a page to
    // store something with exactly one owner. Retrieval fetches the vector to score a candidate
    // and then the text to pack it; inline, that is ONE read, and the text is ~50 bytes against
    // ~1536 for the vector, about 3% of a fetch already paid for.
    //
    // Default-empty and skipped when empty: a record written before the fold decodes with no
    // vector and simply counts as un-embedded until the backfill re-embeds it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub vector: Vec<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextChildRef {
    pub parent_hash: u64,
    pub child_hash: u64,
    pub updated_at_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextSummaryVector {
    pub node_hash: u64,
    pub vector: Vec<f32>,
    /// Which encoder produced `vector`, carried out alongside it. The retrieve path scores from
    /// this response alone and never opens the summary record again, so without the hash here
    /// the model check would have nothing to check.
    #[serde(default)]
    pub embedding_model_hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextSummary {
    pub node_hash: u64,
    pub level: u32,
    #[serde(default)]
    pub text: String,
    pub valid_from_ms: u64,
    // The record's embedding vector, carried inline -- its only home.
    //
    // Every embedding is 1:1 with its owner -- measured on one ingest: event_text 6 to 6 events,
    // entity_state + profile_entity_state 6 to 6 entities, session_l0 + batch_l0 2 to 2
    // summaries -- so a separate record per vector costs a key, a BlockAddress and a page to
    // store something with exactly one owner. Retrieval fetches the vector to score a candidate
    // and then the text to pack it; inline, that is ONE read, and the text is ~50 bytes against
    // ~1536 for the vector, about 3% of a fetch already paid for.
    //
    // Default-empty and skipped when empty: a record written before the fold decodes with no
    // vector and simply counts as un-embedded until the backfill re-embeds it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub vector: Vec<f32>,
    /// Which encoder produced `vector`, mirroring the field of the same name on `ContextNode`.
    ///
    /// A node whose own vector came from a replaced encoder is already declined, but that only
    /// removed one of the two ways this node gets scored: the same retrieve pass also scores
    /// this summary's vector, and it had no hash to check. That is worse than being un-embedded
    /// rather than equal to it -- both passes fill ONE score map, so a summary score marks the
    /// node scored and withdraws the lexical fallback the declined node was routed to.
    ///
    /// Zero means unknown -- a summary written before this field existed, or one carrying no
    /// vector at all -- and unknown never conflicts, so existing stores keep scoring as they did.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub embedding_model_hash: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextCompressionEvent {
    pub compression_id_hash: u64,
    pub node_hash: u64,
    pub source_start_ms: u64,
    pub source_end_ms: u64,
    pub compressed_time_ms: u64,
    #[serde(default)]
    pub summary: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ContextTraversedNode {
    pub node_hash: u64,
    pub depth: u32,
    pub score: f32,
}

pub type ContextNodeModel = ContextNode;
pub type ContextEventModel = ContextEvent;
pub type ContextSlab = ContextEvent;
pub type ContextIndexModel = ContextIndexRef;
pub type ContextAuditModel = ContextPackAudit;
pub type ContextChildModel = ContextChildRef;
pub type ContextSummaryModel = ContextSummary;
pub type ContextCompressionModel = ContextCompressionEvent;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextModelDescriptor {
    pub model_id: u8,
    pub name: String,
    pub key_family: String,
    /// The storage primitive this model's blocks are laid out as, spelled as
    /// `engine::storage_bucket_internals::model_kind_registry!` spells it -- that macro is the
    /// authority for every kind name this engine uses, so a value here is checkable against it
    /// rather than being prose. `string` is a single block per key; `feature` is a timestamped
    /// keyed series. Descriptive only: this field is written into the context-workflow state
    /// report and nothing reads, matches or dispatches on it.
    pub block_primitive: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aliases: Vec<String>,
}

pub const CONTEXT_NODE_MODEL_ID: u8 = 9;
pub const CONTEXT_EVENT_MODEL_ID: u8 = 10;
pub const CONTEXT_INDEX_MODEL_ID: u8 = 11;
pub const CONTEXT_AUDIT_MODEL_ID: u8 = 12;
pub const CONTEXT_CHILD_MODEL_ID: u8 = 14;
pub const CONTEXT_EMBEDDING_MODEL_ID: u8 = 15;
pub const CONTEXT_SUMMARY_MODEL_ID: u8 = 16;
pub const CONTEXT_COMPRESSION_MODEL_ID: u8 = 17;
pub const CONTEXT_ENTITY_MODEL_ID: u8 = 18;

fn context_model_descriptor_entry(
    model_id: u8,
    name: &str,
    key_family: &str,
    block_primitive: &str,
    aliases: &[&str],
) -> ContextModelDescriptor {
    ContextModelDescriptor {
        model_id,
        name: name.to_string(),
        key_family: key_family.to_string(),
        block_primitive: block_primitive.to_string(),
        aliases: aliases.iter().map(|alias| alias.to_string()).collect(),
    }
}

pub fn context_model_descriptors() -> Vec<ContextModelDescriptor> {
    vec![
        context_model_descriptor_entry(
            CONTEXT_NODE_MODEL_ID,
            "ContextNodeModel",
            "ctx:node",
            "string",
            &["ContextNode", "ctxnode"],
        ),
        context_model_descriptor_entry(
            CONTEXT_EVENT_MODEL_ID,
            "ContextEventModel",
            "ctx:event",
            "feature",
            &["ContextEvent", "ContextSegment", "ctxevent", "ctxsegment"],
        ),
        context_model_descriptor_entry(
            CONTEXT_INDEX_MODEL_ID,
            "ContextIndexModel",
            "ctxidx",
            "feature",
            &["ContextIndex", "ContextIndexRef", "ctx:index"],
        ),
        context_model_descriptor_entry(
            CONTEXT_AUDIT_MODEL_ID,
            "ContextAuditModel",
            "ctx:audit",
            "feature",
            &["ContextAudit", "ContextPackAudit"],
        ),
        context_model_descriptor_entry(
            CONTEXT_CHILD_MODEL_ID,
            "ContextChildModel",
            "ctx:child",
            "feature",
            &["ContextChild", "ContextChildRef"],
        ),
        context_model_descriptor_entry(
            CONTEXT_SUMMARY_MODEL_ID,
            "ContextSummaryModel",
            "ctx:summary",
            "feature",
            &["ContextSummary"],
        ),
        context_model_descriptor_entry(
            CONTEXT_COMPRESSION_MODEL_ID,
            "ContextCompressionModel",
            "ctx:compress",
            "feature",
            &[
                "ContextCompression",
                "ContextCompressionEvent",
                "ctx:compression",
            ],
        ),
        context_model_descriptor_entry(
            CONTEXT_ENTITY_MODEL_ID,
            "ContextEntityModel",
            "ctx:entity",
            "string",
            &["ContextEntity"],
        ),
    ]
}

pub fn context_model_descriptor(selector: &str) -> Option<ContextModelDescriptor> {
    let selector = selector.trim();
    let selector_lower = selector.to_ascii_lowercase();
    let selector_model_id = selector.parse::<u8>().ok();
    context_model_descriptors().into_iter().find(|descriptor| {
        Some(descriptor.model_id) == selector_model_id
            || descriptor.name.eq_ignore_ascii_case(selector)
            || descriptor.key_family.eq_ignore_ascii_case(selector)
            || descriptor
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(selector))
            || descriptor
                .aliases
                .iter()
                .any(|alias| alias.to_ascii_lowercase() == selector_lower)
    })
}

pub trait ContextWire: Sized + Serialize + for<'de> Deserialize<'de> {
    fn encode_context_proto_value(&self) -> Vec<u8>;
    fn decode_context_proto_value(bytes: &[u8]) -> Option<Self>;

    fn encode_context_value(&self) -> Vec<u8> {
        self.encode_context_proto_value()
    }

    fn decode_context_value(bytes: &[u8]) -> Option<Self> {
        Self::decode_context_proto_value(bytes).or_else(|| serde_json::from_slice(bytes).ok())
    }
}

impl ContextWire for ContextNode {
    fn encode_context_proto_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.node_hash);
        encode_varint_field(&mut out, 2, self.parent_hash);
        encode_varint_field(&mut out, 3, u64::from(self.kind));
        encode_bytes_field(&mut out, 4, self.canonical_name.as_bytes());
        encode_bytes_field(&mut out, 5, self.l0.as_bytes());
        encode_varint_field(&mut out, 6, self.last_event_time_ms);
        // raw_metadata_ref is a deprecated hot-schema field (like status and l1_ref,
        // both #[serde(default, skip_serializing)]);
        // source provenance moved to resource/provenance sidecars. It is the last
        // deprecated field still being written to the canonical wire payload,
        // so encoding it round-trips a value the trimmed schema must drop. Stop
        // emitting field 10; decode still accepts it for legacy on-disk pages.
        encode_vector_field(&mut out, &self.vector);
        if self.embedding_model_hash != 0 {
            encode_varint_field(&mut out, CONTEXT_EMBEDDING_MODEL_FIELD, self.embedding_model_hash);
        }
        if self.embedding_updated_at_ms != 0 {
            encode_varint_field(
                &mut out,
                CONTEXT_EMBEDDING_UPDATED_FIELD,
                self.embedding_updated_at_ms,
            );
        }
        if self.summary_vector_valid_from_ms != 0 && !self.summary_vector.is_empty() {
            // The timestamp and the vector are written together or not at all: a vector with no
            // timestamp cannot be used by anything (it does not say which query it answers), and
            // a timestamp with no vector claims a copy that is not there.
            if self.summary_vector == self.vector {
                encode_varint_field(&mut out, CONTEXT_SUMMARY_VECTOR_SAME_FIELD, 1);
            } else {
                encode_vector_field_as(&mut out, &NODE_SUMMARY_VECTOR_FIELDS, &self.summary_vector);
            }
            encode_varint_field(
                &mut out,
                CONTEXT_SUMMARY_VECTOR_VALID_FROM_FIELD,
                self.summary_vector_valid_from_ms,
            );
            if self.summary_vector_model_hash != 0 {
                encode_varint_field(
                    &mut out,
                    CONTEXT_SUMMARY_VECTOR_MODEL_FIELD,
                    self.summary_vector_model_hash,
                );
            }
        }
        out
    }

    fn decode_context_proto_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            node_hash: 0,
            parent_hash: 0,
            kind: 0,
            canonical_name: String::new(),
            l0: String::new(),
            status: 0,
            last_event_time_ms: 0,
            l1_ref: String::new(),
            raw_metadata_ref: String::new(),
            vector: Vec::new(),
            embedding_model_hash: 0,
            embedding_updated_at_ms: 0,
            summary_vector: Vec::new(),
            summary_vector_valid_from_ms: 0,
            summary_vector_model_hash: 0,
        };
        let mut summary_vector_is_own_vector = false;
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.node_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.parent_hash = decode_varint(bytes, &mut cursor)?,
                (3, 0) => value.kind = u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?,
                (4, 2) => value.canonical_name = decode_string(bytes, &mut cursor)?,
                (5, 2) => value.l0 = decode_string(bytes, &mut cursor)?,
                (6, 0) => value.last_event_time_ms = decode_varint(bytes, &mut cursor)?,
                // Legacy Rust-only node fields from pre-trim builds.
                (7, 0) => value.last_event_time_ms = decode_varint(bytes, &mut cursor)?,
                (9, 2) => value.l1_ref = decode_string(bytes, &mut cursor)?,
                (10, 2) => value.raw_metadata_ref = decode_string(bytes, &mut cursor)?,
                (CONTEXT_VECTOR_FIELD, 2) => {
                    value.vector = unpack_f32_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (CONTEXT_VECTOR_INT8_FIELD, 2) => {
                    value.vector = unpack_i8_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (CONTEXT_VECTOR_SCALED_FIELD, 2) => {
                    value.vector = unpack_scaled_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (CONTEXT_EMBEDDING_MODEL_FIELD, 0) => {
                    value.embedding_model_hash = decode_varint(bytes, &mut cursor)?
                }
                (CONTEXT_EMBEDDING_UPDATED_FIELD, 0) => {
                    value.embedding_updated_at_ms = decode_varint(bytes, &mut cursor)?
                }
                (CONTEXT_SUMMARY_VECTOR_FIELD, 2) => {
                    value.summary_vector = unpack_f32_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (CONTEXT_SUMMARY_VECTOR_INT8_FIELD, 2) => {
                    value.summary_vector = unpack_i8_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (CONTEXT_SUMMARY_VECTOR_SCALED_FIELD, 2) => {
                    value.summary_vector = unpack_scaled_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (CONTEXT_SUMMARY_VECTOR_VALID_FROM_FIELD, 0) => {
                    value.summary_vector_valid_from_ms = decode_varint(bytes, &mut cursor)?
                }
                (CONTEXT_SUMMARY_VECTOR_SAME_FIELD, 0) => {
                    summary_vector_is_own_vector = decode_varint(bytes, &mut cursor)? != 0
                }
                (CONTEXT_SUMMARY_VECTOR_MODEL_FIELD, 0) => {
                    value.summary_vector_model_hash = decode_varint(bytes, &mut cursor)?
                }
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        if summary_vector_is_own_vector {
            // Resolved here rather than in the arm above: the marker names the record's own
            // vector, and protobuf gives no ordering guarantee, so inside the loop it would
            // copy whatever had been decoded so far -- nothing at all, whenever the writer put
            // the marker first.
            value.summary_vector = value.vector.clone();
        }
        Some(value)
    }
}

impl ContextWire for ContextEvent {
    fn encode_context_proto_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.event_id_hash);
        encode_varint_field(&mut out, 2, self.event_time_ms);
        encode_varint_field(&mut out, 3, u64::from(self.event_type_code()));
        encode_fixed32_field(&mut out, 6, self.confidence.to_bits());
        encode_fixed32_field(&mut out, 7, self.importance.to_bits());
        encode_bytes_field(&mut out, 8, self.text.as_bytes());
        encode_varint_field(&mut out, 9, self.primary_time_ms());
        encode_vector_field(&mut out, &self.vector);
        out
    }

    fn decode_context_proto_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            event_id_hash: 0,
            event_time_ms: 0,
            ingestion_time_ms: 0,
            kind: 0,
            event_type: 0,
            actor_hash: 0,
            status: 0,
            valid_until_ms: 0,
            confidence: 0.0,
            importance: 0.0,
            text: String::new(),
            source_ref: String::new(),
            related_node_hashes: Vec::new(),
            compact_attrs: Vec::new(),
            vector: Vec::new(),
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.event_id_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.event_time_ms = decode_varint(bytes, &mut cursor)?,
                (3, 0) => {
                    let event_type = u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?;
                    value.event_type = event_type;
                }
                // Legacy Rust-only event fields from pre-trim builds.
                (4, 0) => {
                    value.event_type = u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?
                }
                (5, 0) => value.actor_hash = decode_varint(bytes, &mut cursor)?,
                (6, 0) => value.status = u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?,
                (6, 5) => value.confidence = f32::from_bits(decode_fixed32(bytes, &mut cursor)?),
                (7, 0) => value.valid_until_ms = decode_varint(bytes, &mut cursor)?,
                (7, 5) => value.importance = f32::from_bits(decode_fixed32(bytes, &mut cursor)?),
                (8, 2) => value.text = decode_string(bytes, &mut cursor)?,
                (8, 5) => value.confidence = f32::from_bits(decode_fixed32(bytes, &mut cursor)?),
                (9, 0) => value.ingestion_time_ms = decode_varint(bytes, &mut cursor)?,
                (9, 5) => value.importance = f32::from_bits(decode_fixed32(bytes, &mut cursor)?),
                (10, 2) => value.text = decode_string(bytes, &mut cursor)?,
                (CONTEXT_VECTOR_FIELD, 2) => {
                    value.vector = unpack_f32_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (CONTEXT_VECTOR_INT8_FIELD, 2) => {
                    value.vector = unpack_i8_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (CONTEXT_VECTOR_SCALED_FIELD, 2) => {
                    value.vector = unpack_scaled_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (11, 2) => value.source_ref = decode_string(bytes, &mut cursor)?,
                (12, 0) => value
                    .related_node_hashes
                    .push(decode_varint(bytes, &mut cursor)?),
                (12, 2) => {
                    let packed = decode_bytes(bytes, &mut cursor)?;
                    let mut packed_cursor = 0;
                    while packed_cursor < packed.len() {
                        value
                            .related_node_hashes
                            .push(decode_varint(&packed, &mut packed_cursor)?);
                    }
                }
                (13, 2) => value.compact_attrs = decode_bytes(bytes, &mut cursor)?,
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        if value.ingestion_time_ms == 0 {
            value.ingestion_time_ms = value.event_time_ms;
        }
        if value.event_type == 0 {
            value.event_type = value.kind;
        }
        Some(value)
    }
}
