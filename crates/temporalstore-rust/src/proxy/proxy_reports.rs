// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ProxyService info/heartbeat/preflight/policy/operational-surface reports, split from proxy.rs.
use super::*;

impl ProxyService {
    pub fn info(&self) -> ProxyInfo {
        self.sync_client_stats();
        let options = self.options();
        ProxyInfo {
            status: Status::ok(),
            meta_addr: options.meta_addr.clone(),
            route_cache_size: self.client().route_cache_size(),
            stats: *self.inner.stats.read().expect("proxy stats lock poisoned"),
            boot_time_ms: self.inner.boot_time_ms,
        }
    }

    pub fn heartbeat_report(&self) -> ProxyHeartbeatReport {
        self.sync_client_stats();
        let options = self.options();
        ProxyHeartbeatReport {
            status: Status::ok(),
            boot_time_ms: self.inner.boot_time_ms,
            meta_addr: options.meta_addr.clone(),
            config_version: proxy_config_version(&options),
            route_cache_size: self.client().route_cache_size(),
            stats: *self.inner.stats.read().expect("proxy stats lock poisoned"),
        }
    }

    pub fn preflight_report(&self) -> ProxyPreflightReport {
        self.sync_client_stats();
        let options = self.options();
        let stats = *self.inner.stats.read().expect("proxy stats lock poisoned");
        let client_stats = self.client().stats();
        let route_cache_size = self.client().route_cache_size();
        let mut topology_cache = self.client().topology_cache_report();
        let topology_check_status =
            if route_cache_size == 0 || topology_cache.max_topology_version == 0 {
                Status::ok()
            } else {
                self.fetch_meta_topology_version(topology_cache.max_topology_version)
                    .map(|report| {
                        topology_cache = self
                            .client()
                            .topology_cache_report_against(report.current_topology_version);
                        report.status
                    })
                    .unwrap_or_else(|status| status)
            };
        // Asked once per report, not per request: it is a control-plane question and this runs
        // when someone is looking.
        let meta_leader = self.fetch_meta_leader().ok();
        let meta_addr_is_leader = meta_leader
            .as_ref()
            .is_some_and(|leader| !leader.addr.is_empty() && leader.addr == options.meta_addr);
        let authoritative_topology_version = topology_cache.authoritative_topology_version;
        let topology_cache_stale = topology_cache.cache_stale;
        let service_discovery = self.service_discovery_report_with_options(&options);
        let mut degraded_reasons = Vec::new();
        if stats.metaserver_errors > 0 || client_stats.meta_sync_errors > 0 {
            degraded_reasons.push("metaserver_errors".to_string());
        }
        if stats.backend_errors > 0 || client_stats.backend_errors > 0 {
            degraded_reasons.push("backend_errors".to_string());
        }
        if stats.continuous_backend_failures > 0 || client_stats.continuous_backend_failures > 0 {
            degraded_reasons.push("continuous_backend_failures".to_string());
        }
        if stats.bad_requests > 0 {
            degraded_reasons.push("bad_requests".to_string());
        }
        if stats.admission_rejections > 0 {
            degraded_reasons.push("admission_rejections".to_string());
        }
        if stats.account_rejections > 0 {
            degraded_reasons.push("account_rejections".to_string());
        }
        if stats.inflight_rejections > 0 {
            degraded_reasons.push("inflight_rejections".to_string());
        }
        if options.serving_mode != ProxyServingMode::Serving {
            degraded_reasons.push(format!("serving_mode:{:?}", options.serving_mode));
        }
        if topology_cache_stale {
            degraded_reasons.push("topology_cache_stale".to_string());
        }
        if !topology_check_status.ok {
            degraded_reasons.push("topology_check_failed".to_string());
        }
        if service_discovery.stale
            && (service_discovery.registered
                || service_discovery.last_success_ms.is_some()
                || service_discovery.last_error_ms.is_some())
        {
            degraded_reasons.push("service_discovery_stale".to_string());
        }
        let status = if degraded_reasons.is_empty() {
            Status::ok()
        } else {
            Status::error("degraded", degraded_reasons.join(","))
        };
        let config_version = proxy_config_version(&options);
        let policy = self.policy_report();
        ProxyPreflightReport {
            status,
            meta_addr: options.meta_addr.clone(),
            proxy_addr: options.proxy_addr.clone(),
            namespace: options.namespace.clone(),
            config_version,
            route_cache_size,
            authoritative_topology_version,
            topology_cache_stale,
            topology_check_status: Some(topology_check_status),
            meta_leader,
            meta_addr_is_leader,
            stats,
            client: ProxyClientPreflightReport {
                route_cache_size,
                topology_cache,
                open_table_calls: client_stats.open_table_calls,
                execute_requests: client_stats.execute_requests,
                batch_execute_requests: client_stats.batch_execute_requests,
                route_cache_hits: client_stats.route_cache_hits,
                route_cache_misses: client_stats.route_cache_misses,
                route_refreshes: client_stats.route_refreshes,
                backend_errors: client_stats.backend_errors,
                backend_error_streak: client_stats.backend_error_streak,
                continuous_backend_failures: client_stats.continuous_backend_failures,
                meta_sync_errors: client_stats.meta_sync_errors,
            },
            policy,
            service_discovery,
            degraded_reasons,
        }
    }

    pub fn policy_report(&self) -> ProxyPolicyReport {
        self.sync_client_stats();
        let options = self.options();
        let stats = *self.inner.stats.read().expect("proxy stats lock poisoned");
        let (inflight_total, inflight_writes) = self.inflight_snapshot();
        ProxyPolicyReport {
            serving_mode: options.serving_mode,
            drop_percent: options.drop_percent.min(100),
            // `drop_percent` at 100 refuses every request, so a report that reads only
            // `serving_mode` says a proxy is serving while nothing gets through. The
            // percentage is in this report either way, but these three are the fields an
            // operator reads to answer "is this proxy taking traffic".
            serving_reads: !matches!(options.serving_mode, ProxyServingMode::NotServing)
                && options.drop_percent < 100,
            serving_writes: matches!(
                options.serving_mode,
                ProxyServingMode::Serving | ProxyServingMode::Degraded
            ) && options.drop_percent < 100,
            rejecting_all: matches!(options.serving_mode, ProxyServingMode::NotServing)
                || options.drop_percent >= 100,
            admission_rejections: stats.admission_rejections,
            account_rejections: stats.account_rejections,
            inflight_rejections: stats.inflight_rejections,
            serving_rejections: stats.serving_rejections,
            drop_rejections: stats.drop_rejections,
            enforce_ingestion_account: options.enforce_ingestion_account,
            ingestion_account: options.ingestion_account.clone(),
            max_inflight_requests: options.max_inflight_requests,
            max_inflight_write_requests: options.max_inflight_write_requests,
            inflight_requests: inflight_total,
            inflight_write_requests: inflight_writes,
            pin_primary_reads: options.pin_primary_reads,
            context_shard_count: self.effective_context_shard_count(),
            context_shard_count_source: self.context_shard_count_source().to_string(),
        }
    }

    pub fn tonic_streaming_contract(&self) -> ProxyTonicStreamingContract {
        ProxyTonicStreamingContract::default()
    }

    pub fn metrics_parity_report(&self) -> ProxyMetricsParityReport {
        let rendered = self.prometheus_metrics();
        ProxyMetricsParityReport {
            status: Status::ok(),
            compared_files: vec![
                "<repo>/crates/temporalstore-rust/src/proxy/proxy_metrics.rs".to_string(),
                "<repo>/crates/temporalstore-rust/src/proxy/proxy_prometheus.rs".to_string(),
                "<repo>/crates/temporalstore-rust/src/proxy/meta_sync.rs".to_string(),
                "<repo>/crates/temporalstore-rust/src/proxy/proxy_handle.rs".to_string(),
                "<repo>/crates/temporalstore-rust/src/proxy/proxy_configuration_defaults.rs".to_string(),
            ],
