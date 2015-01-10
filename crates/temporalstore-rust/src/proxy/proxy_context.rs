// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! High-level `/context/*` routing for the service proxy (the "direct network
//! path"), split fast-ack / batched:
//!
//! * `POST /context/ingest`  -- FAST raw store. Each message is written as a
//!   lightweight `raw_event` hash record via the datanode's `/ingest/batch`
//!   (a plain command store, ~3ms), with NO inline extraction. This is the hot
//!   path `/v1/ingest` uses so ingest QPS is bounded by a raw write, not by
//!   embedding + summary generation (~218ms).
//! * `POST /context/extract` -- BATCHED extraction. Builds a
//!   `ContextIngestExtractRequest` and forwards to the datanode's
//!   `/context/ingest_extract` (full node/event/summary/embedding fanout). Used
//!   on commit / finalize. When the request carries no messages/sources it reads
//!   the buffered `raw_event` records back (HGETALL over `/execute`) and extracts
//!   those, then clears the buffer.
//! * `POST /context/retrieve` -- forwards a `ContextRetrieveRequest`.
//!
//! In every case the proxy computes `tenant_hash` + `shard_id` reusing the SAME
//! routing `/execute` uses (`crate::client::shard_id_for_key`) and looks up the
//! owning datanode through the metaserver topology. The `/v1` gateway sends raw
//! identifiers only -- the proxy owns all hashing.
use std::borrow::Cow;
use std::fmt::Write as _;
use super::*;

use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::client::shard_id_for_key;
use crate::http::{post_json_with_options, request_bytes_with_options};
use crate::types::{Command, CommandResponse, ExecuteRequest, ExecuteResponse};

/// Tenant scope carried in a high-level `/context/*` request. The proxy owns
/// hashing; callers (the `/v1` gateway) send raw identifiers only.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProxyContextScope {
    #[serde(default)]
    pub tenant_id: String,
    #[serde(default)]
    pub account_id: String,
    // Reserved for future user-scoped routing; accepted on the wire today so the
    // gateway can forward it unchanged, but tenant hashing keys on account+tenant.
    #[allow(dead_code)]
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub session_id: String,
}

/// Deserialises `Option<String>`-shaped JSON while still pointing at the body.
///
/// `#[serde(borrow)]` borrows a bare `Cow<'a, str>`, but NOT one inside an `Option`: serde's own
/// `Deserialize` for `Option<T>` delegates to `T`, and that path always allocates. A titled
/// message therefore copied its title out of the body even though every other field pointed at
/// it. `a_message_borrows_the_body_unless_it_needs_unescaping` asserts both arms of this too.
///
/// `null` and an absent field both stay `None`, which is what `Option` accepted before.
fn optional_borrowed_str<'de: 'a, 'a, D>(deserializer: D) -> Result<Option<Cow<'a, str>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Inner<'a>(std::marker::PhantomData<&'a ()>);

    impl<'de: 'a, 'a> serde::de::Visitor<'de> for Inner<'a> {
        type Value = Cow<'a, str>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a string")
        }

        /// The whole point: the body outlives the request, so this hands back a pointer into it.
        fn visit_borrowed_str<E>(self, value: &'de str) -> Result<Self::Value, E> {
            Ok(Cow::Borrowed(value))
        }

        /// Reached when the string needed unescaping, so there is nothing in the body to point at.
        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E> {
            Ok(Cow::Owned(value.to_string()))
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
            Ok(Cow::Owned(value))
        }
    }

    struct Outer<'a>(std::marker::PhantomData<&'a ()>);

    impl<'de: 'a, 'a> serde::de::Visitor<'de> for Outer<'a> {
        type Value = Option<Cow<'a, str>>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a string or null")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer
                .deserialize_str(Inner(std::marker::PhantomData))
                .map(Some)
        }
    }

    deserializer.deserialize_option(Outer(std::marker::PhantomData))
}

/// One inbound message, borrowed from the request body.
///
/// `Cow` rather than `String`: serde hands back a borrow of the body whenever the JSON string
/// needs no unescaping, which is the ordinary case, and falls back to an owned copy when it
/// does. Every message used to cost two allocations here -- role and content -- before the
/// proxy looked at it.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProxyContextMessage<'a> {
    #[serde(default, borrow)]
    pub role: Cow<'a, str>,
    #[serde(default, borrow)]
    pub content: Cow<'a, str>,
    #[serde(default)]
    pub timestamp_ms: Option<u64>,
    #[serde(default, borrow, deserialize_with = "optional_borrowed_str")]
    pub title: Option<Cow<'a, str>>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProxyContextIngestRequest<'a> {
    #[serde(default)]
    pub scope: ProxyContextScope,
    #[serde(default, borrow)]
    pub messages: Vec<ProxyContextMessage<'a>>,
    /// Pre-shaped low-level sources (`ContextExtractRequest` json). `shard_id`
    /// and `tenant_hash` are re-stamped by the proxy.
    #[serde(default)]
    pub sources: Vec<Value>,
    /// Alias for `sources` accepted from `/v1` callers who post `records`.
    #[serde(default)]
    pub records: Vec<Value>,
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub start_time_ms: u64,
    #[serde(default)]
    pub end_time_ms: u64,
    #[serde(default)]
    pub max_events: Option<usize>,
    #[serde(default)]
    pub provider: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct ProxyContextRetrieveRequest {
    #[serde(default)]
    pub scope: ProxyContextScope,
    #[serde(default)]
    pub query: String,
    #[serde(default)]
    pub start_time_ms: u64,
    #[serde(default)]
    pub end_time_ms: u64,
    #[serde(default)]
    pub max_events: Option<usize>,
    #[serde(default)]
    pub node_hashes: Vec<u64>,
    #[serde(default)]
    pub provider: Option<Value>,
}

/// Scope hash used for context tenancy. Matches the Python control-plane
/// `identity_hashes` (`tools/matrixark_mcp_core_identity.py`): the low 8 bytes
/// of `sha256("{account_id}:{tenant_id}")`, masked to a positive i63.
pub(super) fn context_tenant_hash(scope: &ProxyContextScope) -> u64 {
    let account_id = if scope.account_id.is_empty() {
        "acct_local"
    } else {
        scope.account_id.as_str()
    };
    let tenant_id = if scope.tenant_id.is_empty() {
        "tenant_local_agent"
    } else {
        scope.tenant_id.as_str()
    };
    stable_scope_hash(&format!("{account_id}:{tenant_id}"))
}

fn stable_scope_hash(value: &str) -> u64 {
    let digest = Sha256::digest(value.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(bytes) & 0x7FFF_FFFF_FFFF_FFFF
}

fn wide_window_end(now: u64) -> u64 {
    now.saturating_add(86_400_000)
}

fn scope_session(scope: &ProxyContextScope) -> &str {
    // Borrowed, not owned. Every caller uses this as a `&str` -- `rawlog_key` takes one, the rest
    // interpolate it or pass it on -- so the `String` existed only to be pointed at: a clone of
    // the caller's session id, or a fresh allocation of the constant "default", once per context
    // request.
    if scope.session_id.is_empty() {
        "default"
    } else {
        &scope.session_id
    }
}

/// The retrieve request as it goes on the wire, borrowed from the caller's own request.
///
/// ALPHABETICAL, and that is load-bearing. `json!` renders through a `serde_json::Map`, which is a
/// `BTreeMap`, so its output is key-sorted; a serde struct renders in DECLARATION order. Declaring
/// these out of order changes the bytes this proxy sends. The optional two are skipped when absent,
/// which is what `json!` did by only inserting them when present.
#[derive(serde::Serialize)]
struct RetrievePayload<'a> {
    end_time_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_events: Option<usize>,
    node_hashes: &'a [u64],
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<&'a Value>,
    query: &'a str,
    shard_id: ShardId,
    start_time_ms: u64,
    tenant_hash: u64,
}

/// Buffer key holding the per-scope `raw_event` records awaiting extraction.
fn rawlog_key(tenant_hash: u64, session: &str) -> String {
    // Sized up front rather than left to `format!`, which grows into its buffer. A u64 is at
    // most 20 digits, and the two literals are 15 and 1.
    let mut key = String::with_capacity(16 + 20 + 1 + session.len());
    key.push_str("context:rawlog:");
    push_fixed_width(&mut key, tenant_hash, 0);
    key.push(':');
    key.push_str(session);
    key
}

/// Append `value` zero-padded to `width`, the way `{value:0width$}` would.
///
/// Fixed width is what makes lexicographic order equal arrival order for these keys, so the
/// padding is part of the contract rather than cosmetic.
fn push_fixed_width(out: &mut String, value: u64, width: usize) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let mut rest = value;
    loop {
        i -= 1;
        buf[i] = b'0' + (rest % 10) as u8;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    let digits = &buf[i..];
    for _ in digits.len()..width {
        out.push('0');
    }
    // ASCII digits by construction.
    out.push_str(std::str::from_utf8(digits).unwrap_or("0"));
}

fn source_from_fields(
    shard_id: ShardId,
    tenant_hash: u64,
    source_id: String,
    title: String,
    body: String,
    timestamp_ms: u64,
) -> Value {
    json!({
        "shard_id": shard_id,
        "tenant_hash": tenant_hash,
        "source_kind": "chat",
        "source_id": source_id,
        "title": title,
        "body": body,
        "timestamp_ms": timestamp_ms,
    })
}

/// One chat source, borrowed from the request that already owns every part.
///
/// Fields are declared ALPHABETICALLY on purpose. `json!` renders through a `serde_json::Map`,
/// which is a `BTreeMap` and therefore key-sorted; a serde struct renders in declaration order.
/// Declaring these out of order would move the bytes on the wire without changing the value, so
/// `a_chat_source_is_the_same_bytes_either_way` compares the two renderings byte for byte.
#[derive(serde::Serialize)]
pub(super) struct ChatSource<'a> {
    body: &'a str,
    shard_id: ShardId,
    source_id: &'a str,
    source_kind: &'static str,
    tenant_hash: u64,
    timestamp_ms: u64,
    title: &'a str,
}

/// The prefix every chat source id in one request shares.
///
/// `format!` sizes its buffer from the literal parts and grows as it writes, so building each
/// id with it costs about two allocations. The tenant hash and session are the same for every
/// message, so the prefix is built once and each id reserves room for its index on top: one
/// allocation per id. `the_chat_source_ids_are_what_format_produced` holds the result to the
/// exact bytes `format!` produced, because these ids are stored and read back.
fn chat_id_prefix(tenant_hash: u64, session: &str) -> String {
    // "chat:" plus a u64 at its widest, plus the two separators.
    let mut prefix = String::with_capacity(27 + session.len());
    let _ = write!(prefix, "chat:{tenant_hash}:{session}:");
    prefix
}

/// One chat source id: the shared prefix followed by the message index.
fn chat_id_at(prefix: &str, idx: usize) -> String {
    let mut id = String::with_capacity(prefix.len() + 8);
    id.push_str(prefix);
    let _ = write!(id, "{idx}");
    id
}

/// Builds a borrowed chat source. Mirrors `source_from_fields`, which builds the owned form.
pub(super) fn chat_source<'a>(
    shard_id: ShardId,
    tenant_hash: u64,
    source_id: &'a str,
    title: &'a str,
    body: &'a str,
    timestamp_ms: u64,
) -> ChatSource<'a> {
    ChatSource {
        body,
        shard_id,
        source_id,
        source_kind: "chat",
        tenant_hash,
        timestamp_ms,
        title,
    }
}

/// A source is either one we build from a message, or one the caller supplied as-is.
///
/// `untagged` so the `Raw` arm renders the caller's value exactly as it arrived; a tag would
/// corrupt every pre-shaped source.
#[derive(serde::Serialize)]
#[serde(untagged)]
pub(super) enum SourceItem<'a> {
    Chat(ChatSource<'a>),
    Raw(&'a Value),
}

/// The extract request body, borrowed. Alphabetical for the same reason as `ChatSource`.
#[derive(serde::Serialize)]
pub(super) struct ExtractPayload<'a> {
    end_time_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_events: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<&'a Value>,
    query: &'a str,
    shard_id: ShardId,
    sources: &'a [SourceItem<'a>],
    start_time_ms: u64,
    tenant_hash: u64,
}

/// One buffered raw event, as it is stored.
///
/// Field names and order match what `json!` produced here before, and what every reader
/// downstream expects. Borrowed so serialising copies nothing that the request already owns.
#[derive(serde::Serialize)]
struct RawEventRecord<'a> {
    // ALPHABETICAL, and that is load-bearing. `json!` renders through a `serde_json::Map`,
    // which is a BTreeMap, so its output is key-sorted; serde renders a struct in DECLARATION
    // order. Declaring these out of order silently changes the stored bytes of every buffered
    // event. `a_raw_event_serialises_exactly_as_it_did_when_it_went_through_a_value` fails if
    // this order is disturbed.
    body: &'a str,
    record_type: &'a str,
    role: &'a str,
    timestamp_ms: u64,
    title: &'a str,
}

impl ProxyService {
    /// Route a tenant to its owning shard using the same key hashing as
    /// `/execute` (`shard_id_for_key`). The tenant hash string is the routing
    /// key so every request for a tenant lands on one shard.
    pub(super) fn context_shard_id(&self, tenant_hash: u64) -> ShardId {
        let first = self.with_options(|options| options.context_first_shard_id);
        shard_id_for_key(
            &tenant_hash.to_string(),
            first,
            self.effective_context_shard_count(),
            first,
        )
    }

    fn context_http_options(&self) -> HttpRequestOptions {
        self.with_options(|options| HttpRequestOptions {
            connect_timeout_ms: options.connect_timeout_ms.max(1_000),
            io_timeout_ms: options.context_io_timeout_ms,
            max_retries: options.max_retries,
        })
    }

    /// Resolve the datanode that owns `shard_id` through the client's route cache, or
    /// an HTTP error response ready to return.
    fn context_shard_addr(&self, shard_id: ShardId) -> Result<String, (u16, Vec<u8>)> {
        // Drop routes the metaserver has moved, then resolve through the proxy's shared
        // client: the same route cache, topology invalidation and continuous-failure check
        // that every command entry point uses.
        //
        // This used to be a direct `/shards/{id}` GET, which is the one routing path in the
        // proxy that was written by hand, and so the one that had none of the above. The
        // invalidation is not optional here: a proxy fronting the context gateway serves
        // only these three routes and never calls `execute`, so nothing else on it would
        // ever notice a shard had moved.
        self.invalidate_cached_routes_if_meta_changed();
        match self.client().shard_primary_addr(shard_id, false) {
            Ok(server_addr) => Ok(server_addr),
            Err(err) => {
                self.inner
                    .stats
                    .write()
                    .expect("proxy stats lock poisoned")
                    .metaserver_errors += 1;
                Err(crate::http::json_response(
                    502,
                    &Status::error(
                        "metaserver_error",
                        format!("shard {shard_id} route lookup failed: {err}"),
                    ),
                ))
            }
        }
    }

    fn context_forward_to(&self, server_addr: &str, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
        match request_bytes_with_options(
            server_addr,
            "POST",
            path,
            body,
            "application/json",
            self.context_http_options(),
        ) {
            Ok(raw) => (200, raw),
            Err(err) => {
                self.inner
                    .stats
                    .write()
                    .expect("proxy stats lock poisoned")
                    .backend_errors += 1;
                // Report the failure against the address so a datanode that keeps failing
                // stops being served from the route cache. Forwarding by hand means this
                // accounting has to be done by hand too -- without it the continuous-failure
                // check only ever saw command traffic, and never the gateway's.
                self.client().note_backend_failure(server_addr);
                crate::http::json_response(
                    502,
                    &Status::error(
                        "backend_error",
                        format!("context forward to {server_addr}{path} failed: {err}"),
                    ),
                )
            }
        }
    }

    /// FAST path: buffer the messages as lightweight `raw_event` records in ONE
    /// routed `/execute` `HashMultiSet` (a single raw write, NO extraction, no
    /// embeddings/summaries). Reuses the proxy's cached-route execute path, so it
    /// is one datanode write regardless of message count.
    pub(super) fn context_ingest(&self, request: ProxyContextIngestRequest<'_>) -> (u16, Vec<u8>) {
        self.inner
            .context_ingest_requests
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _admitted = match self.admit_context(&request.scope, true) {
            Ok(guard) => guard,
            Err(response) => return response,
        };
        let tenant_hash = context_tenant_hash(&request.scope);
        let shard_id = self.context_shard_id(tenant_hash);
        let session = scope_session(&request.scope);
        let key = rawlog_key(tenant_hash, session);
        let now = now_ms();
        // Kept inside 8 digits so the field stays fixed-width and lexicographic order
        // remains arrival order. Wrapping needs a hundred million ingests AND a same-
        // millisecond collision with the exact sequence a wrap apart.
        let call = self
            .inner
            .context_ingest_sequence
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % 100_000_000;

        // One entry per message, and the count is known before the loop starts.
        let mut entries: Vec<(String, Vec<u8>)> = Vec::with_capacity(request.messages.len());
        for (idx, message) in request.messages.iter().enumerate() {
            let timestamp_ms = message.timestamp_ms.unwrap_or(now);
            // Borrowed in all three cases. The record this feeds takes `title: &str`, so the owned
            // title only ever existed to be pointed at: a clone of the caller's title, a clone of
            // the role, or a fresh allocation of the constant "message" -- one per message, every
            // message. The other site keeps its `String`, because it hands ownership on.
            let title: std::borrow::Cow<'_, str> = match message.title.as_deref() {
                Some(value) if !value.is_empty() => std::borrow::Cow::Borrowed(value),
                _ if !message.role.is_empty() => std::borrow::Cow::Borrowed(message.role.as_ref()),
                _ => std::borrow::Cow::Borrowed("message"),
            };
            // Orders raw events by (timestamp, call, index within the call), all
            // fixed-width so lexicographic order is arrival order. The call component is
            // what stops two ingests in the same millisecond writing the same field and
            // one silently overwriting the other; `{idx}` alone restarts at zero per call.
            // 20 + 1 + 8 + 1 + 6 -- every part is fixed width, so the length is known and the
            // string is built in one allocation instead of grown into.
            let mut field = String::with_capacity(36);
            push_fixed_width(&mut field, timestamp_ms, 20);
            field.push(':');
            push_fixed_width(&mut field, call, 8);
            field.push(':');
            push_fixed_width(&mut field, idx as u64, 6);
            // Serialised straight from a borrowed view rather than through `json!`, which
            // builds a whole `Value` -- a map, a `String` for each of the five keys, and a
            // `Value::String` copy of role, title and body -- only to render it and drop it.
            // The field names and their order are the same, because these records are read
            // back by everything downstream.
            // Written into a buffer sized for this record. `to_string` starts from serde_json's
            // own 128 and grows, which a raw event clears often enough to cost a second
            // allocation on a third of them; the field names and punctuation below are 88 bytes.
            let mut value =
                Vec::with_capacity(96 + message.content.len() + message.role.len() + title.len());
            if serde_json::to_writer(
                &mut value,
                &RawEventRecord {
                    body: message.content.as_ref(),
                    record_type: "raw_event",
                    role: message.role.as_ref(),
                    timestamp_ms,
                    title: &title,
                },
            )
            .is_err()
            {
                // What `to_string(..).unwrap_or_default()` did: an unserialisable record becomes
                // an empty value rather than a partial one.
                value.clear();
            }
            entries.push((field, value));
        }
        if entries.is_empty() {
            // Nothing to buffer (e.g. pre-shaped records only) -- still a fast ack.
