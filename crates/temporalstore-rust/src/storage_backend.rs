// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Distributed-mode storage-backend selection.
//!
//! A distributed TemporalStore node persists shard data either through **shared
//! storage** (every node reads/writes one authoritative object store, so no
//! log replication is needed) or through **raft replication** (each node keeps
//! a local copy kept in sync by the raft log). Which one a node uses is
//! resolved here, from configuration plus compile-time capability detection.
//!
//! ## `auto` selection (the default)
//!
//! With `TS_STORAGE_BACKEND` unset or `auto`, selection is **intelligent and
//! self-healing**. The goal is to prefer the *networked* shared MatrixObject
//! store whenever it is actually reachable — so shard data follows shards on
//! rebalance and survives datanode loss — but to degrade gracefully rather than
//! wedge a node when it is not:
//!
//! 1. **matrixobject (shared/networked)** — when the `matrixobject` feature is
//!    compiled AND a MatrixObject endpoint is configured (`TS_MATRIXOBJECT_ENDPOINT`)
//!    AND a cheap startup **reachability probe** to that endpoint succeeds.
//! 2. **matrixobject (node-local)** — when the feature is compiled but no
//!    endpoint is configured (the historical node-local behavior, unchanged).
//! 3. **shared path** — otherwise, any configured shared filesystem/object-store
//!    root (`TS_SHARED_STORE_DIR`). This is the graceful downgrade target when a
//!    configured MatrixObject endpoint is *unreachable*.
//! 4. **raft replication** — otherwise. With no raft peers this is effectively a
//!    single local node, matching the historical default
//!    ([`ReplicationMode::default`] is `Raft`).
//!
//! An explicit override (`TS_STORAGE_BACKEND=matrixobject|shared|raft`) forces a
//! tier deterministically and **skips the probe** — the operator has asked for a
//! specific backend. Every resolution yields a [`BackendDecision`] carrying a
//! human-readable `reason` so the caller can emit one clear log line stating
//! which backend was chosen and *why* (including any downgrade).
//!
//! With nothing configured the resolver returns [`StorageBackend::RaftReplication`],
//! so enabling any shared path is opt-in and backward compatible.

use std::net::{TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use temporalstore_snapshot::object_store::{FileObjectStore, ObjectStore, ObjectStoreError};

use crate::e2e::ReplicationMode;

/// Env var: explicit backend override (`auto` | `matrixobject` | `shared` | `raft`).
pub const TS_STORAGE_BACKEND: &str = "TS_STORAGE_BACKEND";
/// Env var: matrixobject bucket name used when the matrixobject backend is chosen.
pub const TS_MATRIXOBJECT_BUCKET: &str = "TS_MATRIXOBJECT_BUCKET";
/// Env var: endpoint (`host:port` or `http://host:port`) of the *networked*
/// MatrixObject object-store service. When set (and the feature is compiled) the
/// `auto` resolver probes it and, only if reachable, selects the shared
/// MatrixObject backend so shard data follows shards across nodes.
pub const TS_MATRIXOBJECT_ENDPOINT: &str = "TS_MATRIXOBJECT_ENDPOINT";
/// Env var: reachability-probe timeout in milliseconds for the MatrixObject
/// endpoint. Kept short so an unreachable store degrades quickly at startup.
pub const TS_MATRIXOBJECT_PROBE_TIMEOUT_MS: &str = "TS_MATRIXOBJECT_PROBE_TIMEOUT_MS";
/// Env var: root directory/URI of a configured shared object store (non-matrixobject).
pub const TS_SHARED_STORE_DIR: &str = "TS_SHARED_STORE_DIR";
/// Env var: cluster id namespacing shared-store keys (all nodes must agree).
pub const TS_SHARED_STORE_CLUSTER_ID: &str = "TS_SHARED_STORE_CLUSTER_ID";

/// Env var: directory holding this node's durable MatrixObject snapshot.
///
/// NAMED HERE BECAUSE IT WAS NOT NAMED ANYWHERE. The durability wiring in `src/bin/temporalstore_data_server.rs`
/// read this key as a bare string literal, which is the one shape of config that cannot be
/// checked: a typo in it does not fail, it silently selects the default. Every other key this
/// module resolves is a `const` for that reason.
pub const TS_MATRIXOBJECT_STORE_DIR: &str = "TS_MATRIXOBJECT_STORE_DIR";

/// Default bucket used for the matrixobject backend when unset.
pub const DEFAULT_MATRIXOBJECT_BUCKET: &str = "temporalstore";

/// Default directory for the durable MatrixObject snapshot when unset.
///
/// Unchanged from the literal the durability wiring used, so a node relying on the default keeps
/// the same directory.
pub const DEFAULT_MATRIXOBJECT_STORE_DIR: &str = "target/temporalstore-matrixobject";
/// Default shared-store cluster id when unset.
pub const DEFAULT_SHARED_STORE_CLUSTER_ID: &str = "temporalstore";
/// Default port assumed for a MatrixObject endpoint given without one (matches
/// the object-store service default, `tools/matrixark_object_store_server.py`).
pub const DEFAULT_OBJECT_STORE_PORT: u16 = 17200;
/// Default reachability-probe timeout (ms) when `TS_MATRIXOBJECT_PROBE_TIMEOUT_MS`
/// is unset or unparseable.
pub const DEFAULT_PROBE_TIMEOUT_MS: u64 = 750;

/// `true` when the crate was compiled with the `matrixobject` feature, i.e. the
/// matrixobject shared-storage backend is available to select at runtime.
pub const fn matrixobject_feature_compiled() -> bool {
    cfg!(feature = "matrixobject")
}

/// The storage/replication strategy a distributed node should use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageBackend {
    /// Shared object storage via matrixobject (the preferred shared backend).
    MatrixObject {
        /// Bucket that holds this cluster's shard objects.
        bucket: String,
        /// Cluster id namespacing shared-store keys.
        cluster_id: String,
        /// Directory holding this node's durable MatrixObject snapshot.
        ///
        /// CARRIED ON THE BACKEND SO THERE IS ONE ANSWER. The store that serves this backend
        /// and the store the durability wiring builds were resolved independently -- and only
        /// one of them resolved anything: the durability path read
        /// `TS_MATRIXOBJECT_STORE_DIR`, and `build_shared_object_store` took
        /// `StoreOptions::default()`, which keeps the whole object store in memory. Two
        /// stores, one bucket name, different contents, and nothing to make them agree.
        store_dir: PathBuf,
    },
    /// Shared object storage rooted at a configured filesystem/mounted path
    /// (a `FileObjectStore`-compatible root shared by every node).
    SharedPath {
        /// Shared object-store root directory (or URI-like root).
        root: PathBuf,
        /// Cluster id namespacing shared-store keys.
        cluster_id: String,
    },
    /// No shared storage available: keep a local copy per node, replicated by raft.
    RaftReplication,
}

impl StorageBackend {
    /// Whether the cache should keep a DISK tier beneath memory.
    ///
    /// A cache tier earns its cost from the latency it spans. With shared storage the durable
    /// copy is on another machine (or a shared mount), and a local disk tier closes a real
    /// millisecond-scale distance. With raft the durable copy is this node's own disk, so the
    /// tier is a third copy of bytes the OS page cache already serves from the same device --
    /// measured at 106 MB of cache against 21.6 MB of pages, 70% of everything on disk, to
    /// shorten a distance that is already zero.
    ///
    /// So: shared storage caches to disk; one-box and raft run memory + their own disk.
    /// `TS_CACHE_DISK_TIER` overrides either way for an operator who knows better.
    pub fn wants_disk_cache_tier(&self) -> bool {
        let backend_default = match self {
            StorageBackend::MatrixObject { .. } | StorageBackend::SharedPath { .. } => true,
            StorageBackend::RaftReplication => false,
        };
        crate::env_flag::env_bool("TS_CACHE_DISK_TIER", backend_default)
    }

    /// The cluster-level replication mode implied by this backend.
    pub fn replication_mode(&self) -> ReplicationMode {
        match self {
            StorageBackend::MatrixObject { .. } | StorageBackend::SharedPath { .. } => {
                ReplicationMode::SharedStore
            }
            StorageBackend::RaftReplication => ReplicationMode::Raft,
        }
    }

    /// `true` when this backend is one of the shared-storage variants.
    pub fn is_shared_storage(&self) -> bool {
        matches!(
            self,
            StorageBackend::MatrixObject { .. } | StorageBackend::SharedPath { .. }
        )
    }

    /// Short, log-friendly description of the resolved backend.
    pub fn describe(&self) -> String {
        match self {
            StorageBackend::MatrixObject {
                bucket,
                cluster_id,
                store_dir,
            } => {
                format!(
                    "shared-storage:matrixobject(bucket={bucket}, cluster={cluster_id}, dir={})",
                    store_dir.display()
                )
            }
            StorageBackend::SharedPath { root, cluster_id } => {
                format!(
                    "shared-storage:path(root={}, cluster={cluster_id})",
                    root.display()
                )
            }
            StorageBackend::RaftReplication => "raft-replication".to_string(),
        }
    }

    /// Construct the shared object store this backend refers to.
    ///
    /// Returns `Ok(None)` for [`StorageBackend::RaftReplication`] — there is no
    /// shared store; the node replicates via raft. The matrixobject arm is only
    /// reachable when the `matrixobject` feature is compiled (resolution keys on
    /// [`matrixobject_feature_compiled`]); the uncompiled arm is a defensive error.
    pub fn build_shared_object_store(
        &self,
    ) -> Result<Option<Arc<dyn ObjectStore>>, ObjectStoreError> {
        match self {
            StorageBackend::SharedPath { root, .. } => {
                Ok(Some(Arc::new(FileObjectStore::new(root.clone()))))
            }
            #[cfg(feature = "matrixobject")]
            StorageBackend::MatrixObject {
                bucket, store_dir, ..
            } => {
                // DURABLE, NOT IN MEMORY. This called `with_default_options`, and the doc on
                // `with_persistent_dir` beside it says what that means: "`StoreOptions::default()`
                // keeps the entire object store in memory, so the bytes vanish when the process
                // exits." So the store handed out as the shared-storage backend lost everything on
                // restart, while the durability wiring built a SECOND, durable store under the same
                // bucket name from a directory this resolver did not know about.
                //
                // IT WAS ALSO THE ONLY THING THE STARTUP CHECK CHECKED. The one production caller
                // builds this "early so a broken shared-storage config fails fast at startup rather
                // than on the first write", and then drops the store. Built in memory, that check
                // could not fail: an unwritable or corrupt snapshot directory passed it and the
                // process died later in `wire_matrixobject_durability`, which exits 1 on the same
                // condition. Pointed at the real directory, the check is about the store the node
                // will actually use.
                //
                // `MatrixObjectStore::new` reads the snapshot back on construction and
                // `flush_persistent_snapshot` rewrites it, so this survives a restart.
                //
                // IT IS STILL NODE-LOCAL, and that is worth stating where the type is built rather
                // than leaving it to be inferred from the word "shared": the object-store crate has
                // no network surface, so data follows a shard to another node only if `store_dir`
                // is itself a shared mount. `is_shared_storage` and the disk-cache-tier default
                // both assume a durable copy that is not this node's own disk, and that assumption
                // holds only under such a mount.
                let store = crate::matrixobject_store::MatrixObjectObjectStore::with_persistent_dir(
                    bucket.clone(),
                    store_dir,
                )?;
                Ok(Some(Arc::new(store)))
            }
            #[cfg(not(feature = "matrixobject"))]
            StorageBackend::MatrixObject { .. } => Err(ObjectStoreError::InvalidKey(
                "matrixobject backend selected but the `matrixobject` feature is not compiled"
                    .to_string(),
            )),
            StorageBackend::RaftReplication => Ok(None),
        }
    }
}

/// A resolved backend together with a human-readable reason for the choice.
///
/// The `reason` is intended to be logged verbatim by the caller as the single
/// "resolved storage backend" line, so it states *why* — e.g. a probe success,
/// a graceful downgrade after an unreachable endpoint, or a forced override.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendDecision {
    /// The concrete backend a node should use.
    pub backend: StorageBackend,
    /// Human-readable explanation of why this backend was selected.
    pub reason: String,
}

impl BackendDecision {
    fn new(backend: StorageBackend, reason: impl Into<String>) -> Self {
        Self {
            backend,
            reason: reason.into(),
        }
    }

    /// The cluster-level replication mode implied by the chosen backend.
    pub fn replication_mode(&self) -> ReplicationMode {
        self.backend.replication_mode()
    }
}

/// Signature of a reachability probe: given an endpoint string, return `true`
/// when the networked MatrixObject store appears reachable. Injectable so tests
/// exercise the decision table without a live service.
pub type ReachabilityProbe<'a> = dyn Fn(&str) -> bool + 'a;

/// Parse a `host[:port]` / `scheme://host[:port][/path]` endpoint into the
/// `host:port` pair to probe, defaulting the port to [`DEFAULT_OBJECT_STORE_PORT`].
///
/// Returns `None` for an endpoint with no usable host.
fn parse_endpoint_host_port(endpoint: &str) -> Option<(String, u16)> {
    let mut rest = endpoint.trim();
    if rest.is_empty() {
        return None;
    }
    // Strip an optional scheme (http://, https://, matrixobject://, ...).
    if let Some(idx) = rest.find("://") {
        rest = &rest[idx + 3..];
    }
    // Drop any path/query/fragment and userinfo.
    rest = rest.split(['/', '?', '#']).next().unwrap_or(rest);
    if let Some((_, host)) = rest.rsplit_once('@') {
        rest = host;
    }
    let rest = rest.trim();
    if rest.is_empty() {
        return None;
    }
    // host:port (ignore bracketed IPv6 subtleties — the common case is host:port).
    match rest.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() && !port.is_empty() => {
            let port = port.parse::<u16>().ok()?;
            Some((host.to_string(), port))
        }
        _ => Some((rest.to_string(), DEFAULT_OBJECT_STORE_PORT)),
    }
}

/// Default reachability probe: a short-timeout TCP connect to the endpoint's
/// `host:port` (the same cheap liveness check `proxy.rs`/`raft_node.rs` use).
///
/// A successful TCP connect means the object-store service is listening; the
/// service additionally exposes `GET /v1/healthz` for a semantic check, left as
/// a future strengthening of this probe (no HTTP client is linked here).
pub fn default_endpoint_probe(endpoint: &str) -> bool {
    let Some((host, port)) = parse_endpoint_host_port(endpoint) else {
        return false;
    };
    let timeout = Duration::from_millis(probe_timeout_ms());
    let Ok(addrs) = (host.as_str(), port).to_socket_addrs() else {
        return false;
    };
    for addr in addrs {
        if TcpStream::connect_timeout(&addr, timeout).is_ok() {
            return true;
        }
    }
    false
}

fn probe_timeout_ms() -> u64 {
    std::env::var(TS_MATRIXOBJECT_PROBE_TIMEOUT_MS)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .unwrap_or(DEFAULT_PROBE_TIMEOUT_MS)
}

/// An explicit `TS_STORAGE_BACKEND` selection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum BackendOverride {
    /// Resolve by detection + configuration precedence.
    #[default]
    Auto,
    /// Force the matrixobject shared backend (errors out via fallback if uncompiled).
    MatrixObject,
    /// Force a configured shared-path backend.
    SharedPath,
    /// Force raft replication regardless of shared-storage config.
    Raft,
}

impl BackendOverride {
    fn parse(raw: Option<&str>) -> Self {
        match raw.map(|value| value.trim().to_ascii_lowercase()).as_deref() {
            Some("matrixobject") | Some("matrix_object") | Some("object") => Self::MatrixObject,
            Some("shared") | Some("shared_path") | Some("shared_store") | Some("path") => {
                Self::SharedPath
            }
            Some("raft") | Some("raft_replication") | Some("replication") => Self::Raft,
            // "auto", "", and unknown values all fall back to auto-detection.
            _ => Self::Auto,
        }
    }
}

/// Inputs to storage-backend selection, gathered from the environment (or an
/// injected getter, for tests) plus compile-time capability detection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageBackendConfig {
    override_mode: BackendOverride,
    /// Whether the matrixobject backend is available (feature compiled + enabled).
    matrixobject_available: bool,
    matrixobject_bucket: String,
    /// Configured endpoint of the *networked* MatrixObject store, if any. When
    /// present the `auto` resolver probes it before selecting matrixobject.
    matrixobject_endpoint: Option<String>,
    /// Directory for this node's durable MatrixObject snapshot.
    matrixobject_store_dir: PathBuf,
    shared_store_dir: Option<PathBuf>,
    cluster_id: String,
}

impl StorageBackendConfig {
    /// Resolve config from process environment variables.
    pub fn from_env() -> Self {
        Self::from_getter(|name| std::env::var(name).ok())
    }

    /// Resolve config from an arbitrary getter (used in tests to avoid touching
    /// real process env). matrixobject availability is taken from the compiled
    /// feature; a value of `TS_STORAGE_BACKEND=raft` still forces raft.
    pub fn from_getter(get: impl Fn(&str) -> Option<String>) -> Self {
        Self::from_parts(matrixobject_feature_compiled(), &get)
    }

    /// Core builder with matrixobject availability injected — the single place
    /// tests can exercise both the "feature present" and "feature absent" paths.
    pub fn from_parts(matrixobject_available: bool, get: &impl Fn(&str) -> Option<String>) -> Self {
        let bucket = non_empty(get(TS_MATRIXOBJECT_BUCKET))
            .unwrap_or_else(|| DEFAULT_MATRIXOBJECT_BUCKET.to_string());
        let cluster_id = non_empty(get(TS_SHARED_STORE_CLUSTER_ID))
            .unwrap_or_else(|| DEFAULT_SHARED_STORE_CLUSTER_ID.to_string());
        let shared_store_dir = non_empty(get(TS_SHARED_STORE_DIR)).map(PathBuf::from);
        let matrixobject_endpoint = non_empty(get(TS_MATRIXOBJECT_ENDPOINT));
        let matrixobject_store_dir = non_empty(get(TS_MATRIXOBJECT_STORE_DIR))
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_MATRIXOBJECT_STORE_DIR));
        Self {
            override_mode: BackendOverride::parse(get(TS_STORAGE_BACKEND).as_deref()),
            matrixobject_available,
            matrixobject_bucket: bucket,
            matrixobject_endpoint,
            matrixobject_store_dir,
            shared_store_dir,
            cluster_id,
        }
    }

    /// Resolve the concrete backend a node should use, applying the precedence
    /// documented on this module, using the default TCP reachability probe for
    /// any configured MatrixObject endpoint.
    pub fn resolve(&self) -> StorageBackend {
        self.resolve_decision().backend
    }

    /// Resolve to a [`BackendDecision`] (backend + reason) using the default
    /// reachability probe. This is what production callers use; the reason is
    /// intended to be logged as the single "resolved storage backend" line.
    pub fn resolve_decision(&self) -> BackendDecision {
        self.resolve_decision_with_probe(&default_endpoint_probe)
    }

    /// Resolve to a [`BackendDecision`] using an injected reachability probe.
    /// Tests drive the full decision table through here with a stub probe.
    pub fn resolve_decision_with_probe(&self, probe: &ReachabilityProbe<'_>) -> BackendDecision {
        match self.override_mode {
            BackendOverride::Raft => BackendDecision::new(
                StorageBackend::RaftReplication,
                "TS_STORAGE_BACKEND=raft: forced raft replication",
            ),
            // Explicit matrixobject override forces the backend without probing —
            // the operator asked for it deterministically.
            BackendOverride::MatrixObject if self.matrixobject_available => BackendDecision::new(
                self.matrixobject(),
                self.forced_matrixobject_reason(),
            ),
            BackendOverride::SharedPath => match &self.shared_store_dir {
                Some(root) => BackendDecision::new(
                    self.shared_path(root),
                    format!(
                        "TS_STORAGE_BACKEND=shared: forced shared-path at {}",
                        root.display()
                    ),
                ),
                // Explicitly asked for shared-path but none configured: fall
                // through to auto so we still pick the best available backend
                // rather than silently mis-configuring.
                None => self.resolve_auto_decision(probe),
            },
            // MatrixObject override without the feature compiled, or Auto.
            BackendOverride::MatrixObject | BackendOverride::Auto => {
                self.resolve_auto_decision(probe)
            }
        }
    }

    fn matrixobject(&self) -> StorageBackend {
        StorageBackend::MatrixObject {
            bucket: self.matrixobject_bucket.clone(),
            cluster_id: self.cluster_id.clone(),
            store_dir: self.matrixobject_store_dir.clone(),
        }
    }

    fn shared_path(&self, root: &PathBuf) -> StorageBackend {
        StorageBackend::SharedPath {
            root: root.clone(),
            cluster_id: self.cluster_id.clone(),
        }
    }

    fn forced_matrixobject_reason(&self) -> String {
        match &self.matrixobject_endpoint {
            Some(endpoint) => format!(
                "TS_STORAGE_BACKEND=matrixobject: forced (endpoint {endpoint}, probe skipped)"
            ),
            None => "TS_STORAGE_BACKEND=matrixobject: forced (node-local, no endpoint)".to_string(),
        }
    }

    /// The `auto` decision: prefer a reachable networked matrixobject store,
    /// then node-local matrixobject, then a configured shared path, then raft.
    fn resolve_auto_decision(&self, probe: &ReachabilityProbe<'_>) -> BackendDecision {
        if self.matrixobject_available {
            match &self.matrixobject_endpoint {
                Some(endpoint) => {
                    if probe(endpoint) {
                        return BackendDecision::new(
                            self.matrixobject(),
                            format!(
                                "auto: matrixobject endpoint {endpoint} reachable — \
                                 shared object storage selected"
                            ),
                        );
                    }
                    // Configured but unreachable: degrade gracefully rather than
                    // wedge the node on a store it cannot reach.
                    let mut fallback = self.fallback_below_matrixobject();
                    fallback.reason = format!(
                        "auto: matrixobject endpoint {endpoint} UNREACHABLE — downgraded to {}",
                        fallback.backend.describe()
                    );
                    return fallback;
                }
                None => {
                    return BackendDecision::new(
                        self.matrixobject(),
                        "auto: matrixobject feature available, node-local \
                         (no TS_MATRIXOBJECT_ENDPOINT configured)",
                    );
                }
            }
        }
        let mut fallback = self.fallback_below_matrixobject();
        fallback.reason = format!(
            "auto: matrixobject unavailable — selected {}",
            fallback.backend.describe()
        );
        fallback
    }

    /// The tier below matrixobject: a configured shared path, else raft.
    fn fallback_below_matrixobject(&self) -> BackendDecision {
        match &self.shared_store_dir {
            Some(root) => BackendDecision::new(
                self.shared_path(root),
                format!("shared-path at {}", root.display()),
            ),
            None => BackendDecision::new(
                StorageBackend::RaftReplication,
                "raft replication (no shared storage configured)",
            ),
        }
    }

    /// Convenience: the cluster-level [`ReplicationMode`] implied by the
    /// resolved backend.
    pub fn replication_mode(&self) -> ReplicationMode {
        self.resolve().replication_mode()
    }
}

impl Default for StorageBackendConfig {
    fn default() -> Self {
        Self::from_getter(|_| None)
    }
}

fn non_empty(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn getter(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    fn resolve(available: bool, pairs: &[(&str, &str)]) -> StorageBackend {
        StorageBackendConfig::from_parts(available, &getter(pairs)).resolve()
    }
