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

impl ContextWire for ContextIndexRef {
    fn encode_context_proto_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.primary_node_hash);
        encode_varint_field(&mut out, 2, self.primary_event_time_ms);
        encode_varint_field(&mut out, 3, self.event_id_hash);
        out
    }

    fn decode_context_proto_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            primary_node_hash: 0,
            primary_event_time_ms: 0,
            event_id_hash: 0,
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.primary_node_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.primary_event_time_ms = decode_varint(bytes, &mut cursor)?,
                (3, 0) => value.event_id_hash = decode_varint(bytes, &mut cursor)?,
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

impl ContextWire for ContextAuditRef {
    fn encode_context_proto_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.node_hash);
        encode_varint_field(&mut out, 2, self.event_time_ms);
        encode_bytes_field(&mut out, 3, self.reason.as_bytes());
        out
    }

    fn decode_context_proto_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            node_hash: 0,
            event_time_ms: 0,
            reason: String::new(),
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.node_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.event_time_ms = decode_varint(bytes, &mut cursor)?,
                (3, 2) => value.reason = decode_string(bytes, &mut cursor)?,
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

impl ContextWire for ContextPackAudit {
    fn encode_context_proto_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        // Field numbers MUST match the wire proto (context interface.proto ContextPackAudit):
        // query_id=1, session_hash=2, request_time_ms=3, max_prompt_tokens=4,
        // selected_tokens=5, selected_refs=6. Rust-only fields go AFTER the reserved max tag so
        // they never shift a reserved field (previously query_hash sat at 4, shifting three
        // fields and dropping selected_refs on cross-impl decode).
        encode_bytes_field(&mut out, 1, self.query_id.as_bytes());
        encode_varint_field(&mut out, 2, self.session_hash);
        encode_varint_field(&mut out, 3, self.request_time_ms);
        encode_varint_field(&mut out, 4, u64::from(self.max_prompt_tokens));
        encode_varint_field(&mut out, 5, u64::from(self.selected_tokens));
        for selected in &self.selected_refs {
            encode_bytes_field(&mut out, 6, &selected.encode_context_proto_value());
        }
        // Rust-only extensions (no wire counterpart), on non-colliding tags.
        encode_varint_field(&mut out, 9, self.query_hash);
        for blocked in &self.blocked_refs {
            encode_bytes_field(&mut out, 10, &blocked.encode_context_proto_value());
        }
        out
    }

    fn decode_context_proto_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            query_id: String::new(),
            session_hash: 0,
            request_time_ms: 0,
            query_hash: 0,
            max_prompt_tokens: 0,
            selected_tokens: 0,
            selected_refs: Vec::new(),
            blocked_refs: Vec::new(),
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 2) => value.query_id = decode_string(bytes, &mut cursor)?,
                (2, 0) => value.session_hash = decode_varint(bytes, &mut cursor)?,
                (3, 0) => value.request_time_ms = decode_varint(bytes, &mut cursor)?,
                (4, 0) => {
                    value.max_prompt_tokens =
                        u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?
                }
                (5, 0) => {
                    value.selected_tokens =
                        u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?
                }
                (6, 2) => value
                    .selected_refs
                    .push(ContextAuditRef::decode_context_value(&decode_bytes(
                        bytes,
                        &mut cursor,
                    )?)?),
                (9, 0) => value.query_hash = decode_varint(bytes, &mut cursor)?,
                (10, 2) => value
                    .blocked_refs
                    .push(ContextAuditRef::decode_context_value(&decode_bytes(
                        bytes,
                        &mut cursor,
                    )?)?),
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}


impl ContextWire for ContextEntity {
    fn encode_context_proto_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.entity_hash);
        encode_varint_field(&mut out, 2, self.node_hash);
        encode_varint_field(&mut out, 3, u64::from(self.entity_type));
        encode_bytes_field(&mut out, 4, self.name.as_bytes());
        encode_bytes_field(&mut out, 5, self.value.as_bytes());
        encode_varint_field(&mut out, 6, self.updated_at_ms);
        encode_varint_field(&mut out, 7, self.valid_from_ms);
        encode_fixed32_field(&mut out, 8, self.confidence.to_bits());
        for source_event_hash in &self.source_event_hashes {
            encode_varint_field(&mut out, 9, *source_event_hash);
        }
        encode_vector_field(&mut out, &self.vector);
        out
    }

    fn decode_context_proto_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            entity_hash: 0,
            node_hash: 0,
            entity_type: 0,
            name: String::new(),
            value: String::new(),
            updated_at_ms: 0,
            valid_from_ms: 0,
            confidence: 0.0,
            source_event_hashes: Vec::new(),
            vector: Vec::new(),
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.entity_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.node_hash = decode_varint(bytes, &mut cursor)?,
                (3, 0) => {
                    value.entity_type = u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?
                }
                (4, 2) => value.name = decode_string(bytes, &mut cursor)?,
                (5, 2) => value.value = decode_string(bytes, &mut cursor)?,
                (6, 0) => value.updated_at_ms = decode_varint(bytes, &mut cursor)?,
                (7, 0) => value.valid_from_ms = decode_varint(bytes, &mut cursor)?,
                (8, 5) => value.confidence = f32::from_bits(decode_fixed32(bytes, &mut cursor)?),
                (9, 0) => value
                    .source_event_hashes
                    .push(decode_varint(bytes, &mut cursor)?),
                (9, 2) => {
                    let packed = decode_bytes(bytes, &mut cursor)?;
                    let mut packed_cursor = 0;
                    while packed_cursor < packed.len() {
                        value
                            .source_event_hashes
                            .push(decode_varint(&packed, &mut packed_cursor)?);
                    }
                }
                (CONTEXT_VECTOR_FIELD, 2) => {
                    value.vector = unpack_f32_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (CONTEXT_VECTOR_INT8_FIELD, 2) => {
                    value.vector = unpack_i8_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (CONTEXT_VECTOR_SCALED_FIELD, 2) => {
                    value.vector = unpack_scaled_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

impl ContextWire for ContextChildRef {
    fn encode_context_proto_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.parent_hash);
        encode_varint_field(&mut out, 2, self.child_hash);
        encode_varint_field(&mut out, 5, self.updated_at_ms);
        out
    }

    fn decode_context_proto_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            parent_hash: 0,
            child_hash: 0,
            updated_at_ms: 0,
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.parent_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.child_hash = decode_varint(bytes, &mut cursor)?,
                (5, 0) => value.updated_at_ms = decode_varint(bytes, &mut cursor)?,
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

impl ContextWire for ContextSummary {
    fn encode_context_proto_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 2, self.node_hash);
        encode_varint_field(&mut out, 3, u64::from(self.level));
        encode_bytes_field(&mut out, 4, self.text.as_bytes());
        encode_varint_field(&mut out, 5, self.valid_from_ms);
        encode_vector_field(&mut out, &self.vector);
        if self.embedding_model_hash != 0 {
            encode_varint_field(
                &mut out,
                CONTEXT_EMBEDDING_MODEL_FIELD,
                self.embedding_model_hash,
            );
        }
        out
    }

    fn decode_context_proto_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            node_hash: 0,
            level: 0,
            text: String::new(),
            valid_from_ms: 0,
            vector: Vec::new(),
            embedding_model_hash: 0,
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (2, 0) => value.node_hash = decode_varint(bytes, &mut cursor)?,
                (3, 0) => value.level = u32::try_from(decode_varint(bytes, &mut cursor)?).ok()?,
                (4, 2) => value.text = decode_string(bytes, &mut cursor)?,
                (5, 0) => value.valid_from_ms = decode_varint(bytes, &mut cursor)?,
                (CONTEXT_VECTOR_FIELD, 2) => {
                    value.vector = unpack_f32_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (CONTEXT_VECTOR_INT8_FIELD, 2) => {
                    value.vector = unpack_i8_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (CONTEXT_EMBEDDING_MODEL_FIELD, 0) => {
                    value.embedding_model_hash = decode_varint(bytes, &mut cursor)?
                }
                (CONTEXT_VECTOR_SCALED_FIELD, 2) => {
                    value.vector = unpack_scaled_vector(decode_bytes_ref(bytes, &mut cursor)?)
                }
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

impl ContextWire for ContextCompressionEvent {
    fn encode_context_proto_value(&self) -> Vec<u8> {
        let mut out = Vec::new();
        encode_varint_field(&mut out, 1, self.compression_id_hash);
        encode_varint_field(&mut out, 2, self.node_hash);
        encode_varint_field(&mut out, 3, self.source_start_ms);
        encode_varint_field(&mut out, 4, self.source_end_ms);
        encode_varint_field(&mut out, 5, self.compressed_time_ms);
        encode_bytes_field(&mut out, 6, self.summary.as_bytes());
        out
    }

    fn decode_context_proto_value(bytes: &[u8]) -> Option<Self> {
        let mut cursor = 0;
        let mut value = Self {
            compression_id_hash: 0,
            node_hash: 0,
            source_start_ms: 0,
            source_end_ms: 0,
            compressed_time_ms: 0,
            summary: String::new(),
        };
        while cursor < bytes.len() {
            let tag = decode_varint(bytes, &mut cursor)?;
            match (tag >> 3, tag & 0x7) {
                (1, 0) => value.compression_id_hash = decode_varint(bytes, &mut cursor)?,
                (2, 0) => value.node_hash = decode_varint(bytes, &mut cursor)?,
                (3, 0) => value.source_start_ms = decode_varint(bytes, &mut cursor)?,
                (4, 0) => value.source_end_ms = decode_varint(bytes, &mut cursor)?,
                (5, 0) => value.compressed_time_ms = decode_varint(bytes, &mut cursor)?,
                (6, 2) => value.summary = decode_string(bytes, &mut cursor)?,
                (_, wire_type) => skip_proto_field(bytes, &mut cursor, wire_type)?,
            }
        }
        Some(value)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Command {
    /// Appended by a leader when it takes office, and applied as nothing.
    ///
    /// A leader may not conclude that entries from an earlier term are committed just because a
    /// majority holds them; it has to commit something of its own term first. Without that, a
    /// node that restarts and takes office keeps whatever commit point it had, and entries above
    /// it sit in the log unapplied until the next write happens to arrive.
    LeaderEstablish,
    CommonDelete {
        key: String,
    },
    CommonExpire {
        key: String,
        ttl_ms: u64,
    },
    CommonTtl {
        key: String,
    },
    /// Remove a key's expiry without touching its value: Redis PERSIST. Answers 1 when a
    /// timeout was actually removed, 0 when the key is missing or already had none.
    CommonPersist {
        key: String,
    },
    CommonExists {
        key: String,
    },
    StringSet {
        key: String,
        #[serde(with = "crate::bytes_serde")]
        value: Vec<u8>,
    },
    StringSetEx {
        key: String,
        #[serde(with = "crate::bytes_serde")]
        value: Vec<u8>,
        ttl_ms: u64,
    },
    StringSetConditional {
        key: String,
        #[serde(with = "crate::bytes_serde")]
        value: Vec<u8>,
        #[serde(default)]
        ttl_ms: Option<u64>,
        condition: StringSetCondition,
        return_old: bool,
        /// `SET key value KEEPTTL`: replace the value and leave the deadline where it is.
        ///
        /// THE THIRD OUTCOME. A value-replacing write can do one of three things to the
        /// deadline it overwrites -- arm a new one (`ttl_ms: Some`), discard the old one
        /// (`ttl_ms: None`), or leave it alone -- and `Option<u64>` can only spell two. That
        /// is the same shape that made `GETEX key PERSIST` a no-op until #1665: two of the
        /// three outcomes shared a `None` and the code read that `None` as the wrong one.
        ///
        /// It is a SEPARATE FIELD rather than a three-way enum replacing `ttl_ms` on purpose.
        /// `Command` is serde-serialized into the WAL, and a replayed record written before
        /// this field existed must keep meaning exactly what it meant when it was written.
        /// `#[serde(default)]` gives those records `false` -- the clearing behaviour they were
        /// recorded with. Retyping `ttl_ms` instead would default an old `{"ttl_ms": 5000}`
        /// record to "no deadline" on replay and silently drop a deadline that was durable.
        ///
        /// `ttl_ms: Some(_)` together with `keep_ttl: true` is contradictory and unreachable:
        /// `parse_set_options` refuses `EX`/`PX` beside `KEEPTTL` with a syntax error, as
        /// Redis does. The shard treats an arming TTL as the winner if one is ever built by
        /// hand, and says so where it does.
        #[serde(default)]
        keep_ttl: bool,
    },
    StringGet {
        key: String,
    },
    StringDelete {
        key: String,
    },
    HashSet {
        key: String,
        field: String,
        #[serde(with = "crate::bytes_serde")]
        value: Vec<u8>,
    },
    HashGet {
        key: String,
        field: String,
    },
    HashMultiGet {
        key: String,
        fields: Vec<String>,
    },
    HashMultiSet {
        key: String,
        #[serde(with = "crate::bytes_serde::pairs")]
        entries: Vec<(String, Vec<u8>)>,
    },
    HashIncrBy {
        key: String,
        field: String,
        increment: i64,
    },
    HashGetAll {
        key: String,
    },
    HashLen {
        key: String,
    },
    HashDelete {
        key: String,
        field: String,
    },
    SetAdd {
        key: String,
        #[serde(with = "crate::bytes_serde")]
        member: Vec<u8>,
    },
    /// Upsert one member with a score. Answers 1 for a new member, 0 for a re-score.
    ZSetAdd {
        key: String,
        #[serde(with = "crate::bytes_serde")]
        member: Vec<u8>,
        score: f64,
    },
    /// The member's score as its shortest string form, or nil when absent.
    ZSetScore {
        key: String,
        #[serde(with = "crate::bytes_serde")]
        member: Vec<u8>,
    },
    ZSetRemove {
        key: String,
        #[serde(with = "crate::bytes_serde")]
        member: Vec<u8>,
    },
    ZSetCard {
        key: String,
    },
    /// Index range in (score, member) order, Redis semantics (negatives from the tail).
    /// Answers interleaved member/score-string pairs.
    ZSetRange {
        key: String,
        start: i64,
        stop: i64,
        rev: bool,
    },
    /// Score-window range; exclusive flags implement the leading-paren syntax. Answers
    /// interleaved member/score-string pairs.
    ZSetRangeByScore {
        key: String,
        min: f64,
        max: f64,
        min_exclusive: bool,
        max_exclusive: bool,
        rev: bool,
    },
    /// Atomic seen-within-window check-and-mark: answers 1 when the member was already seen
    /// inside the window (a duplicate), else marks it and answers 0. Expired entries are
    /// swept from the front in bounded steps on every call.
    SeenCheck {
        key: String,
        #[serde(with = "crate::bytes_serde")]
        member: Vec<u8>,
        window_ms: u64,
    },
    /// How many members the set currently holds (expired-but-unswept included).
    SeenCard {
        key: String,
    },
    /// Atomic take-with-refill on a token bucket: refill by elapsed time (capped at
    /// capacity), then take `tokens` if they fit. Answers three strings -- allowed ("1"/"0"),
    /// tokens remaining, and retry-after ms (0 when allowed).
    BucketTake {
        key: String,
        tokens: f64,
        capacity: f64,
        refill_per_sec: f64,
    },
    /// The same arithmetic without taking: what a take of `tokens` WOULD answer.
    BucketPeek {
        key: String,
        tokens: f64,
        capacity: f64,
        refill_per_sec: f64,
    },
    /// Add to a member's score (0 when absent), atomically under the shard lock.
    /// Answers the new score as its shortest string form.
    ZSetIncrBy {
        key: String,
        #[serde(with = "crate::bytes_serde")]
        member: Vec<u8>,
        increment: f64,
    },
    /// Pop up to `count` members off the low (min) or high end, in order.
    /// Answers interleaved member/score-string pairs.
    ZSetPop {
        key: String,
        min: bool,
        count: u64,
    },
    /// The member's 0-based position in (score, member) order, tail-based when rev.
    /// Answers the rank as a decimal string, or nil for a missing member.
    ZSetRank {
        key: String,
        #[serde(with = "crate::bytes_serde")]
        member: Vec<u8>,
        rev: bool,
    },
    /// Push one element onto a list end (left = head). Answers the new length.
    ListPush {
        key: String,
        #[serde(with = "crate::bytes_serde")]
        member: Vec<u8>,
        left: bool,
    },
    /// Pop one element off a list end. Answers the element, or nil for an empty/missing list.
    ListPop {
        key: String,
        left: bool,
    },
    /// Inclusive range with Redis index semantics (negatives count from the tail).
    ListRange {
        key: String,
        start: i64,
        stop: i64,
    },
    ListLen {
        key: String,
    },
    SetMembers {
        key: String,
    },
    SetRemove {
        key: String,
        #[serde(with = "crate::bytes_serde")]
        member: Vec<u8>,
    },
    FeatureAppend {
        key: String,
        points: Vec<FeaturePoint>,
    },
    FeatureAppendWithPolicy {
        key: String,
        points: Vec<FeaturePoint>,
        policy: FeatureWritePolicy,
    },
    FeatureQuery {
        key: String,
        start_ms: u64,
        end_ms: u64,
        #[serde(default)]
        count: Option<usize>,
    },
    FeatureQueryFiltered {
        key: String,
        start_ms: u64,
        end_ms: u64,
        #[serde(default)]
        count: Option<usize>,
        #[serde(default)]
        filters: Vec<FeatureFilter>,
    },
    FeatureReplace {
        key: String,
        start_ms: u64,
        end_ms: u64,
        points: Vec<FeaturePoint>,
    },
    FeatureDelete {
        key: String,
    },
    FeatureAggQuery {
        key: String,
        start_ms: u64,
        end_ms: u64,
        aggregator: String,
        #[serde(default)]
        count: Option<usize>,
    },
    SequenceAdd {
        key: String,
        rows: Vec<SequenceFeatureRow>,
    },
    SequenceQuery {
        key: String,
        start_ms: u64,
        end_ms: u64,
        count: usize,
        #[serde(default)]
        filters: Vec<FeatureFilter>,
    },
    SequenceBatchQuery {
        queries: Vec<SequenceQuerySpec>,
    },
    ControlStateIncrement {
        key: String,
        timestamp_ms: u64,
        amount: i64,
    },
    ControlStateIncrementWithOptions {
        key: String,
        timestamp_ms: u64,
        amount: i64,
        #[serde(default)]
        precision_ms: Option<u64>,
        #[serde(default)]
        ttl_ms: Option<u64>,
    },
    ControlStateChangeAdd {
        key: String,
        timestamp_ms: u64,
        #[serde(with = "crate::bytes_serde")]
        value: Vec<u8>,
        #[serde(default)]
        precision_ms: Option<u64>,
        #[serde(default)]
        ttl_ms: Option<u64>,
    },
    ControlStateCount {
        key: String,
        start_ms: u64,
        end_ms: u64,
    },
    ControlStateQuery {
        key: String,
        start_ms: u64,
        end_ms: u64,
        aggregator: String,
    },
    ControlStateDetail {
        key: String,
        start_ms: u64,
        end_ms: u64,
        #[serde(default)]
        count: Option<usize>,
    },
    ControlStateSet {
        family: ControlStateFamily,
        key: String,
        timestamp_ms: u64,
        amount: i64,
    },
    ControlStateSetAndGet {
        family: ControlStateFamily,
        key: String,
        timestamp_ms: u64,
        amount: i64,
        start_ms: u64,
        end_ms: u64,
        aggregator: String,
    },
    /// Full-conformance analog of the control_state `HSETANDGET` operator: an
    /// atomic increment-then-read that additionally supports precision bucketing,
    /// per-key TTL, and UUID idempotency (dedup within a bounded window so
    /// at-least-once queue replays do not double-count). `aggregator` accepts the
    /// same verbs as `ControlStateFamilyQuery`, including `change` (distinct count).
    ControlStateSetAndGetWithOptions {
        family: ControlStateFamily,
        key: String,
        timestamp_ms: u64,
        amount: i64,
        start_ms: u64,
        end_ms: u64,
        aggregator: String,
        #[serde(default)]
        precision_ms: Option<u64>,
        #[serde(default)]
        ttl_ms: Option<u64>,
        #[serde(default)]
        uuid: Option<String>,
    },
    ControlStateFamilyQuery {
        family: ControlStateFamily,
        key: String,
        start_ms: u64,
        end_ms: u64,
        aggregator: String,
    },
    #[serde(alias = "control_state_fol_set")]
    ControlStateSelectionSet {
        key: String,
        #[serde(with = "crate::bytes_serde")]
        value: Vec<u8>,
        occur_time_ms: u64,
        ttl_ms: u64,
        #[serde(alias = "fol_type")]
        selection_type: ControlStateSelectionType,
    },
    #[serde(alias = "control_state_fol_query")]
    ControlStateSelectionQuery {
        key: String,
    },
    ControlStateManager {
        key: String,
        /// Optional manager op-code for `MANAGER` conformance: QUERY(2), FIELD_LIST(5),
        /// FIELD_COUNT(6), ALL_DATA_VALUE(7). `None` / unknown returns the family summary.
        #[serde(default)]
        op_type: Option<String>,
        /// Exact field keys (timestamp_ms) for QUERY; the second tuple element is unused
        /// (kept for wire symmetry with the KvPair field_list).
        #[serde(default)]
        field_list: Vec<(String, String)>,
        /// Inclusive range start for FIELD_LIST (timestamp_ms as string).
        #[serde(default)]
        start_offset: String,
        /// Inclusive range end for FIELD_LIST (timestamp_ms as string).
        #[serde(default)]
        end_offset: String,
        /// Select the Distinct (Cpc) family series instead of the default Counter (H) family.
        #[serde(default, alias = "is_cpc")]
        is_distinct: bool,
    },
    ControlStateDebug {
        key: String,
        start_ms: u64,
        end_ms: u64,
    },
    /// Attach an embedding to the node it describes.
    ///
    /// Addressed by the node itself rather than by a hash of (tenant, owner, level). That hash
    /// is one-way: a writer holding a node can compute it, but nothing holding the result can
    /// get back to the node, so the vector and the record it describes could only ever be
    /// re-associated by recomputing the hash from the owner. Naming the owner here is what lets
    /// the vector live on the record it belongs to.
    /// Vectors for these nodes, read from the nodes themselves.
    ///
    /// The counterpart of ContextSetNodeEmbedding: asking by owner is possible because the
    /// vector lives on the owner. The retired separate rows could never answer this -- they
    /// were keyed by a hash of (tenant, owner, level), so the caller had to already know each
    /// owner to rebuild the key, and the reply could not say which owner it came from.
    ContextQueryNodeEmbeddings {
        tenant_hash: u64,
        node_hashes: Vec<u64>,
    },
    /// Attachment blob store: start a multi-part upload into the engine-owned blob directory.
    /// The full original payload of an oversized resource lives here so one TemporalStore holds
    /// everything -- chunks stay searchable in records while the attachment itself is fetchable
    /// again by its `temporalstore://resources/{tenant}/{content-hash}` URI.
    ContextResourceBlobBegin {
        tenant_hash: u64,
    },
    /// Append one part to a staged upload. Parts are sequential against one node.
    ContextResourceBlobAppend {
        tenant_hash: u64,
        upload_token: String,
        payload_base64: String,
    },
    /// Publish a staged upload: content-hash, fsync, rename into place. The resource record
    /// that carries the returned URI is the commit point; a blob with no record is garbage the
    /// sweep collects.
    ContextResourceBlobCommit {
        tenant_hash: u64,
        upload_token: String,
    },
    /// Single-shot begin+append+commit for payloads that fit one request.
    ContextResourceBlobPut {
        tenant_hash: u64,
        payload_base64: String,
    },
    /// Range-read a published blob. `length == 0` means to the end.
    ContextResourceBlobFetch {
        uri: String,
        offset: u64,
        length: u64,
    },
    /// Delete unreferenced blobs older than  for one tenant, plus stale staging
    /// files. The caller supplies the referenced set from the resource records it holds.
    ContextResourceBlobSweep {
        tenant_hash: u64,
        referenced_content_hashes: Vec<u64>,
        min_age_ms: u64,
    },
    ContextSetNodeEmbedding {
        tenant_hash: u64,
        node_hash: u64,
        model_hash: u64,
        vector: Vec<f32>,
        updated_at_ms: u64,
    },
    ContextUpsertNode {
        tenant_hash: u64,
        /// Boxed: ~208 bytes of a 216-byte variant, and this variant caps the enum.
        /// Serialises identically to the unboxed field.
        node: Box<ContextNode>,
    },
    ContextGetNode {
        tenant_hash: u64,
        node_hash: u64,
    },
    ContextGetNodes {
        tenant_hash: u64,
        node_hashes: Vec<u64>,
    },
    ContextWriteEvent {
        tenant_hash: u64,
        node_hash: u64,
        /// Boxed: 184 bytes of a 208-byte variant, the other one capping the enum.
        event: Box<ContextEvent>,
        #[serde(default)]
        first_write_only: bool,
        #[serde(default)]
        cold_storage: bool,
    },
    ContextWriteExtractedEvent {
        tenant_hash: u64,
        node_hash: u64,
        /// Boxed because this field decides how wide EVERY command is.
        ///
        /// `Command` is as wide as its widest variant, and this variant was the widest at 288
        /// bytes -- 184 of them this field. Every command everywhere paid for it, including a
        /// `StringSet` carrying eight bytes. Behind a box the variant is about 112 and the enum
        /// drops to 216, which is the next widest (`ContextUpsertNode`).
        ///
        /// `Box<T>` serialises exactly as `T`, so nothing on disk or on the wire changes.
        event: Box<ContextEvent>,
        #[serde(default)]
        indexes: ContextExtractedEventIndexes,
        #[serde(default)]
        first_write_only: bool,
        #[serde(default)]
        cold_storage: bool,
    },
    ContextQueryEvents {
        tenant_hash: u64,
        node_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        max_scan: Option<usize>,
        #[serde(default)]
        current_valid_only: bool,
        #[serde(default)]
        as_of_ms: u64,
        #[serde(default)]
        kinds: Vec<u32>,
        #[serde(default)]
        statuses: Vec<u32>,
        #[serde(default)]
        min_confidence: f32,
        #[serde(default)]
        min_importance: f32,
    },
    ContextWriteIndexRef {
        tenant_hash: u64,
        index_name: String,
        index_value_hash: u64,
        #[serde(default)]
        scope_hash: u64,
        event_time_ms: u64,
        index_ref: ContextIndexRef,
    },
    ContextQueryIndex {
        tenant_hash: u64,
        index_name: String,
        index_value_hash: u64,
        #[serde(default)]
        scope_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        #[serde(default)]
        limit: Option<usize>,
    },
    ContextQueryIndexIntersection {
        tenant_hash: u64,
        predicates: Vec<ContextIndexLookup>,
        #[serde(default)]
        limit: Option<usize>,
    },
    ContextWritePackAudit {
        tenant_hash: u64,
        audit: ContextPackAudit,
    },
    ContextQueryPackAudit {
        tenant_hash: u64,
        session_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        #[serde(default)]
        limit: Option<usize>,
    },
    ContextMarkSummaryDirty {
        tenant_hash: u64,
        node_hash: u64,
        event_time_ms: u64,
        #[serde(default)]
        reason: u32,
        #[serde(default)]
        propagate_depth: u32,
    },
    ContextQuerySummaryDirty {
        tenant_hash: u64,
        node_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        #[serde(default)]
        limit: Option<usize>,
    },
    // Embedding-dirty marker trio. Fully independent of the summary-dirty trio
    // above (key `ctx:embdirty:{tenant}:{node}`): a node can be embedding-dirty
    // without being summary-dirty and vice-versa. Marks nodes whose semantic
    // embedding is deferred (raw-first bulk ingest) or failed (live-path provider
    // error) so the async embed drainer can attach vectors later. `clear` turns
    // the mark command into a per-node clear (used by the drainer once a node is
    // successfully embedded); the marker struct is reused only as a lightweight
    // {node_hash, event_time_ms} carrier.
    ContextMarkEmbeddingDirty {
        tenant_hash: u64,
        node_hash: u64,
        event_time_ms: u64,
        #[serde(default)]
        reason: u32,
        #[serde(default)]
        propagate_depth: u32,
        #[serde(default)]
        clear: bool,
    },
    ContextQueryEmbeddingDirty {
        tenant_hash: u64,
        // node_hash == 0 means "all pending embedding-dirty nodes for this shard"
        // (the drainer's O(pending) scan). A non-zero node_hash queries one node.
        node_hash: u64,
        start_time_ms: u64,
        end_time_ms: u64,
        #[serde(default)]
        limit: Option<usize>,
    },
    ContextUpsertEntity {
        tenant_hash: u64,
        entity: ContextEntity,
