// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! production_readiness_report aggregator + service readiness helpers, split from readiness.rs.
use super::*;

pub fn production_readiness_report() -> ProductionReadinessReport {
    let raft = distributed_raft_readiness();
    let ingestion = ingestion_readiness_report();
    let client_routing = client_routing_readiness_report();
    let proxy_serving = proxy_serving_readiness_report();
    let data_node_service = data_node_service_readiness_report();
    let metaserver_control_plane = metaserver_control_plane_readiness_report();
    let storage_cache_dependency_matrix = storage_cache_dependency_matrix_report();
    let storage_ssd_cache_pressure = storage_ssd_cache_pressure_readiness_report();
    let storage_migration_corpus = storage_migration_corpus_readiness_report();
    let storage_production_posture = storage_production_posture_report();
    let feature_modules = feature_module_production_readiness_report();
    let context_workflow = context_workflow_production_readiness_report();
    let areas = vec![
        ReadinessArea {
            area: "raft_replication".to_string(),
            ready: raft.production_ready,
            covered: vec![
                "separate raft_node binary with /raft/propose, /raft/read, /raft/status"
                    .to_string(),
                "HTTP AppendEntries, RequestVote, InstallSnapshot, and chunked snapshot endpoints"
                    .to_string(),
                "majority write checks, follower catch-up, safe reads, promotion, scale up/down"
                    .to_string(),
                "local WAL recovery and local separate-node replication test".to_string(),
                "raft_node, raft-enabled server, and metaserver process startup select ProductionRaftEngineKind::TemporalRaft by default"
                    .to_string(),
                "Raft TemporalRaft rollout readiness covers adapter presence, data-node/metaserver startup selection, durable local log state, and real multi-process data-node/metaserver log-store rollout evidence"
                    .to_string(),
                "TemporalRaft process rollout reports must include MatrixRaft-derived operational semantics evidence for read-index, lease-read, lagging follower rejection, stale follower writes, snapshot install/restart, membership add/promote/remove, follower rejoin after compaction, secondary-read eligibility, apply convergence, and WAL persistence"
                    .to_string(),
                "OpenRaft process rollout reports must include MatrixRaft-derived operational semantics evidence for read-index, lease-read, lagging follower rejection, stale follower writes, snapshot install/restart, membership add/promote/remove, follower rejoin after compaction, secondary-read eligibility, apply convergence, and WAL persistence"
                    .to_string(),
                "RaftStorageApplyFence is persisted in WAL records and rejects missing, corrupt, stale, or ahead-of-storage recovery state"
                    .to_string(),
                "Raft atomic apply readiness covers storage apply fence persistence, WAL fence recovery validation, production runtime data-node atomic durability reports, storage mutation atomic commit, snapshot-install atomic commit, and snapshot lifecycle reporting"
                    .to_string(),
                "RaftSnapshotInstallReport exposes freeze, flush, manifest verify, checksum verify, install, tail replay, and rollback status for snapshot installs"
                    .to_string(),
                "ProductionMetaRaftRuntime can drive a data-Raft membership workflow for learner add, catch-up verification, promotion, leader transfer, and voter removal"
                    .to_string(),
                "Raft transport security readiness covers auth-token validation, mTLS cert/key/CA config validation, service-process mTLS runtime selection, authenticated HTTP transport, and plaintext-only local chaos guardrails"
                    .to_string(),
                "Raft external chaos readiness covers local OS-process restart/failover, stale-read partition heal, lagging follower catch-up, networked membership/snapshot, storage replay, external packet-loss, disk-pressure, and process-chaos gates"
                    .to_string(),
            ],
            missing: raft.missing.clone(),
        },
        ReadinessArea {
            area: "client".to_string(),
            ready: client_routing.production_ready,
            covered: vec![
                "typed table client, pipeline, retries/timeouts, route refresh".to_string(),
                "primary routing for writes and optional secondary routing for reads".to_string(),
                "background topology sync and crc64 slot formula".to_string(),
                "client preflight report exposes route/table cache, backend-failure backlog, stats, and options"
                    .to_string(),
                "client retry classifier separates budget-free safe topology retries from unsafe write retries that require explicit write retry budget"
                    .to_string(),
                "shared conformance corpus runs through the typed table client API and direct engine path for common, feature, sequence, control_state, context, and restart reads"
                    .to_string(),
                "versioned Rust-native SDK contract committed in proto/temporalstore/v1 with validation in the local parity gate"
                    .to_string(),
                "tonic/prost client and server binding types are generated at build time from the committed v1 schema"
                    .to_string(),
                "generated tonic Execute and BatchExecute service adapters convert protobuf commands and delegate to the existing engine execution path"
                    .to_string(),
                "generated tonic OpenTable, SyncTopology, and ClientPreflight adapters delegate to the existing TemporalStoreClient table, topology, and preflight paths"
                    .to_string(),
                "client preflight exposes a standard partition-set compatibility view derived from cached table ranges and shard routes"
                    .to_string(),
                "client routing readiness covers typed table client, topology sync, retry budgets, topology preflight, Neptune routing hooks, deployment placement hooks, and the tracked Rust-native HTTP/JSON, RESP, and tonic migration contract while keeping legacy wire migration explicitly out of scope"
                    .to_string(),
            ],
            missing: client_routing.missing,
        },
        ReadinessArea {
            area: "proxy".to_string(),
            ready: proxy_serving.production_ready,
            covered: vec![
                "HTTP proxy execute/batch routes delegate through TemporalStoreClient for route cache, retries, backend-error refresh, and stats sync"
                    .to_string(),
                "background heartbeat loop and heartbeat auto-register".to_string(),
                "backend-error route refresh and failure-streak avoidance".to_string(),
                "namespace/table open path and table-routed proxy execute/batch routes"
                    .to_string(),
                "service-name JSON aliases for ExecuteCmd, BatchExecuteCmd, OpenTable, and table execute/batch execute"
                    .to_string(),
                "service-name admin aliases expose proxy info, heartbeat/config, and embedded client preflight"
                    .to_string(),
                "command-shaped proxy HTTP/JSON aliases cover Get, Set, FeatureAdd, ControlStateHset, HMGet, HMSet, HGetAll, and HLen through the normal routed client path"
                    .to_string(),
                "Rust-native service discovery replacement for consul via proxy auto-register, heartbeat TTL, admin inspection, and Prometheus stale/registered metrics"
                    .to_string(),
                "proxy serving readiness covers HTTP execute routes, heartbeat/config application, topology-version invalidation, admission policy, route quarantine, Rust-native service discovery, RESP migration, tonic streaming/callback shape, and the tracked HTTP/JSON, RESP, and tonic migration contract while keeping legacy wire proxy transport out of scope"
                    .to_string(),
            ],
            missing: proxy_serving.missing,
        },
        ReadinessArea {
            area: "metaserver".to_string(),
            ready: metaserver_control_plane.production_ready,
            covered: vec![
                "server/proxy inventory, heartbeat, namespace/table topology, meta stats/info"
                    .to_string(),
                "optional in-process Raft-backed mutation path".to_string(),
                "load-aware replica placement skeleton with location and host diversity"
                    .to_string(),
                "single-node metabase snapshot export/import and atomic local snapshot save/load"
                    .to_string(),
                "HTTP scheduler admin surface can submit, run-next, snapshot, and restore deterministic metaserver tasks"
                    .to_string(),
                "optional local scheduler snapshot persistence through TS_META_SCHEDULER_SNAPSHOT"
                    .to_string(),
                "metaserver preflight is exposed for both single-node and Raft-backed metadata runtimes, including MasterService aliases"
                    .to_string(),
                "metaserver control-plane readiness covers inventory heartbeat, namespace/table topology, partition-set/member/version topology, local Raft mutation path, load-aware placement, local snapshots, scheduler admin, scheduler snapshots, scheduler retry/task Raft persistence, networked metaserver Raft rollout, and metaserver-owned data-Raft membership execution"
                    .to_string(),
                "networked metaserver scheduler execution readiness covers multi-process metaserver Raft, real data-node process scheduling for missing primary, under-replication, stale/dead server, load/reload/unload, membership changes, cooldowns, safe mode, scheduler task replay, durable data-Raft membership coupling, and stale scheduler token/generation rejection"
                    .to_string(),
            ],
            missing: metaserver_control_plane.missing,
        },
        ReadinessArea {
            area: "data_node_distributed_raft".to_string(),
            ready: raft.production_ready,
            covered: vec![
                "separate raft_node binary can accept /raft/propose and peer raft messages"
                    .to_string(),
                "local model covers majority commit, catch-up, failover, safe scale up/down"
                    .to_string(),
                "data Raft supports replicator-style bounded follower catch-up with lag/progress reports"
                    .to_string(),
                "production Raft timer loop uses bounded follower catch-up per heartbeat"
                    .to_string(),
                "WAL-backed local Raft state persists commits, leadership, and membership"
                    .to_string(),
                "local Raft WAL supports segmented append, sync, segment retention, ordered recovery, and corrupt-tail truncation"
                    .to_string(),
                "WAL-backed data Raft persists installed snapshot payload/floor so restart can recover trimmed pre-snapshot state"
                    .to_string(),
                "data-node Raft exposes planned add/remove/replace membership changes with joint-consensus, catch-up, quorum checks, and reports"
                    .to_string(),
                "metaserver Raft exposes matching planned add/remove/replace membership changes with catch-up, quorum checks, and reports"
                    .to_string(),
                "metaserver table topology can be converted into data-node Raft voter membership plans with no-op detection and server-state validation"
                    .to_string(),
                "metaserver topology membership plans can be applied to data-node Raft with joint-consensus catch-up and an applied/no-op report"
                    .to_string(),
                "raft_node and raft-enabled server expose /raft/membership/apply for networked safe membership changes, and the OS-process harness verifies scale down/up through that route"
                    .to_string(),
                "standard deterministic metaserver task scheduler model covers priority ordering, retry-later backoff, abort, and UpdateMembership task enqueue"
                    .to_string(),
                "scheduler task queue can be snapshotted/restored and freezing shard replicas can be repaired into UpdateMembership tasks"
                    .to_string(),
                "chunked snapshot message assembly and stale snapshot rejection are tested"
                    .to_string(),
                "chunked timestamped KV commands round-trip through the data-Raft command codec and snapshot install rebuilds packed page layout"
                    .to_string(),
                "distributed append failures fall back to post-commit snapshot install for lagging followers"
                    .to_string(),
                "external snapshot transfer policy, leader upload, metaserver snapshot-ref recording, URI download, install, and Raft catch-up are tested"
                    .to_string(),
                "metaserver Raft has a production runtime wrapper with validation, status, failover/catch-up timer, and stale-server detection"
                    .to_string(),
                "local OS-process raft_node harness covers secondary restart catch-up and surviving-node failover after leader crash"
                    .to_string(),
                "local OS-process raft_node harness covers network partition stale-read rejection, heal, and follower catch-up"
                    .to_string(),
                "local OS-process raft_node harness covers lagging-follower observation, majority-side writes, heal, and catch-up reads"
                    .to_string(),
                "local OS-process raft_node harness covers rolling restart of every voter with WAL recovery and post-restart replication"
                    .to_string(),
                "TemporalRaft-backed data-node and metaserver adapter is available behind the temporal-raft-engine feature with durable log state, state-machine apply, snapshot metadata, read-index checks, membership changes, leader transfer, and restart recovery tests"
                    .to_string(),
                "raft_node, raft-enabled server, and metaserver process startup wire the production runtime options to ProductionRaftEngineKind::TemporalRaft"
                    .to_string(),
                "Raft TemporalRaft rollout readiness covers adapter presence, data-node/metaserver startup selection, durable local log state, and real multi-process data-node/metaserver log-store rollout evidence"
                    .to_string(),
                "TemporalRaft process rollout reports must include MatrixRaft-derived operational semantics evidence for read-index, lease-read, lagging follower rejection, stale follower writes, snapshot install/restart, membership add/promote/remove, follower rejoin after compaction, secondary-read eligibility, apply convergence, and WAL persistence"
                    .to_string(),
                "OpenRaft process rollout reports must include MatrixRaft-derived operational semantics evidence for read-index, lease-read, lagging follower rejection, stale follower writes, snapshot install/restart, membership add/promote/remove, follower rejoin after compaction, secondary-read eligibility, apply convergence, and WAL persistence"
                    .to_string(),
                "RaftStorageApplyFence persists shard, term, committed/applied index, snapshot id, storage epoch, and checksum with WAL recovery validation"
                    .to_string(),
                "Raft atomic apply readiness covers storage apply fence persistence, WAL fence recovery validation, production runtime data-node atomic durability reports, storage mutation atomic commit, snapshot-install atomic commit, and snapshot lifecycle reporting"
                    .to_string(),
                "Raft snapshot lifecycle reports install, tail replay, and rollback decisions for data-node snapshot recovery paths"
                    .to_string(),
                "metaserver-owned data-Raft membership workflow reports learner add, catch-up verification, promotion, leader transfer, and voter removal"
                    .to_string(),
                "Raft metaserver membership readiness covers topology membership plans, data-Raft apply reports, learner catch-up/promotion, leader transfer, voter removal, networked scheduler /raft/membership/apply transport, persisted scheduler task state, and real data-node group execution under follower lag, failover, scale up/down, and secondary replication"
                    .to_string(),
                "MatrixRaft-style leader write authority, ReadIndex guards, learner catch-up/promotion checks, and fail-closed stale leader-transfer checks are modeled locally"
                    .to_string(),
                "Raft transport security readiness covers auth-token validation, mTLS cert/key/CA config validation, service-process mTLS runtime selection, authenticated HTTP transport, and plaintext-only local chaos guardrails"
                    .to_string(),
                "Raft external chaos readiness covers local OS-process restart/failover, stale-read partition heal, lagging follower catch-up, networked membership/snapshot, storage replay, external packet-loss, disk-pressure, and process-chaos gates"
                    .to_string(),
            ],
            missing: if raft.production_ready {
                Vec::new()
            } else {
                raft.missing.clone()
            },
        },
        ReadinessArea {
            area: "dataserver".to_string(),
            ready: data_node_service.production_ready,
            covered: vec![
                "TemporalEngine command execution, checked execute/batch, runtime queue"
                    .to_string(),
                "async jobs, cancellation status surface, dirty-object reporting".to_string(),
                "page-address persistence and Prometheus metrics endpoint".to_string(),
                "load rejects duplicate loaded shards and unload removes shard metadata with not-found semantics"
                    .to_string(),
                "config get/set follows partition-map not-found semantics".to_string(),
                "data-node membership update rejects stale global/unit versions and reports whether local replica remains active"
                    .to_string(),
                "data-node runtime uses shard-affine worker lanes so one partition has FIFO single-lane execution while different shards run in parallel"
                    .to_string(),
                "data-node scheduler prioritizes foreground execute work over background dump/compact/GC and applies a separate background queue admission limit"
                    .to_string(),
                "dirty shards can be discovered and scheduled as background dump tasks".to_string(),
                "stoppable periodic dirty-dump scheduler submits dirty shard dumps through the background queue without duplicating already queued shard dumps"
                    .to_string(),
                "stoppable expiry sweep scheduler removes expired records from loaded shards and reports sweep/removal counters"
                    .to_string(),
                "background dump/compact/GC honor in-flight cancellation checkpoints before destructive phases"
                    .to_string(),
                "local shard_stat_info and object-manager stats report logical objects, page refs, dirty objects, dirty routing slots, and storage bytes"
                    .to_string(),
                "local shard/table/tenant read/write QPS admission is enforced from Config quota fields"
                    .to_string(),
                "ServerService admin aliases expose runtime stats, preflight, dirty-object, and queued-worker state"
                    .to_string(),
                "crash recovery reports and tests cover wal, index-log, page stream, and band-manifest ordering"
                    .to_string(),
                "data-node service readiness covers execute runtime, async jobs, lifecycle admin, shard-affine workers, local admission, crash recovery reports, tonic/gRPC streaming callbacks, distributed admission, and multi-process lifecycle validation"
                    .to_string(),
            ],
            missing: data_node_service.missing,
        },
        ReadinessArea {
            area: "ingestion".to_string(),
            ready: ingestion.production_ready,
            covered: ingestion.covered,
            missing: ingestion.missing,
        },
        ReadinessArea {
            area: "fault_tolerance".to_string(),
            ready: true,
            covered: vec![
                "local majority-loss rejection for reads and writes".to_string(),
                "local primary crash promotion and recovered follower catch-up".to_string(),
                "local stale candidate, lagging read-index, and stale snapshot guards".to_string(),
                "kill switches for proxy reads/writes, replication, async storage, and scale changes"
                    .to_string(),
                "local combined recovery proof covers Raft WAL restore plus wal, index-log, page-file, and packed timestamped KV recovery"
                    .to_string(),
                "Prometheus alert rules and fault runbook cover stuck replica, split-brain control_state, slow follower, and storage pressure triage"
                    .to_string(),
                "external_chaos_gate composes OS-process Raft kill/restart, stale-read partition, lag/heal, rolling restart, networked membership/snapshot, and storage replay harnesses"
                    .to_string(),
                "Raft external chaos readiness covers local OS-process restart/failover, stale-read partition heal, lagging follower catch-up, networked membership/snapshot, storage replay, external packet-loss, disk-pressure, and process-chaos gates"
                    .to_string(),
                "rolling restart and rolling upgrade validation covers proxy, client, metaserver, and data-node using preflight, readiness, failover, replay, and rollback checks"
                    .to_string(),
            ],
            missing: Vec::new(),
        },
        ReadinessArea {
            area: "storage_cache".to_string(),
            ready: storage_migration_corpus.production_ready
                && storage_cache_dependency_matrix.production_ready
                && storage_ssd_cache_pressure.production_ready
                && storage_production_posture.production_ready,
            covered: vec![
                "local page segment files with page-address indexes".to_string(),
                "local page compaction rolls to a fresh segment, rewrites live page references, persists the compacted index, and makes old segments GC-eligible"
                    .to_string(),
                "memory plus disk read-through cache with zstd block envelope".to_string(),
                "shared-store checkpoint/wal replay model and GC tests".to_string(),
                "storage recovery integrity report summarizes indexed/discovered/live/orphan/corrupt segments, stale refs, unreadable bytes, and ownership mismatches"
                    .to_string(),
                "storage lifecycle plan ranks reclaim candidates by stale bytes, live-ref density, orphan status, and delayed-destroy pressure"
                    .to_string(),
                "slot-dump install preflight reports stale sequence, missing/corrupt segments, unreadable refs, and safe install status before marker writes"
                    .to_string(),
                "shard index persistence and slot-dump install use temp-file fsync plus rename instead of direct overwrite"
                    .to_string(),
                "memory cache supports pinned hot/page blocks with eviction skip accounting, inspection, and Prometheus metrics"
                    .to_string(),
                "storage lifecycle cache warmup returns selected slots, page-ref hits/fills/failures, block-store read count, and warmed bytes"
                    .to_string(),
                "storage production report exposes Rust JSONL wal/index-log format, replay-safe status, sequence/record/byte counts, and binary compatibility gaps"
                    .to_string(),
                "storage production report exposes Rust page-envelope version, checksum/object-id/routing-slot/compression support, band bytes, and page-header compatibility gaps"
                    .to_string(),
                "storage compatibility decision is explicit: Rust log/page formats are migration-only versus binary logs/page headers, with golden conversion/replay required before migration"
