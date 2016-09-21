// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ProxyService::prometheus_metrics, split from proxy.rs.
use super::*;

impl ProxyService {
    pub fn prometheus_metrics(&self) -> String {
        self.sync_client_stats();
        let options = self.options();
        let stats = *self.inner.stats.read().expect("proxy stats lock poisoned");
        let client = self.client().stats();
        let mut out = String::new();
        out.push_str("# HELP temporalstore_proxy_requests_total Proxy request counters by kind.\n");
        out.push_str("# TYPE temporalstore_proxy_requests_total counter\n");
        for (kind, value) in [
            ("execute", stats.execute_requests),
            ("batch_execute", stats.batch_execute_requests),
            ("bad_request", stats.bad_requests),
            ("context_ingest", stats.context_ingest_requests),
            ("context_extract", stats.context_extract_requests),
            ("context_retrieve", stats.context_retrieve_requests),
            ("admission_rejection", stats.admission_rejections),
            ("account_rejection", stats.account_rejections),
            ("inflight_rejection", stats.inflight_rejections),
            ("serving_rejection", stats.serving_rejections),
            ("drop_rejection", stats.drop_rejections),
            ("heartbeat", stats.heartbeat_total),
            ("heartbeat_slow", stats.heartbeat_slow_total),
            ("auto_register", stats.auto_register_total),
            ("auto_register_throttled", stats.auto_register_throttled),
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_requests_total",
                &[("kind", kind)],
                value,
            );
        }

        out.push_str(
            "# HELP temporalstore_proxy_route_cache_entries Current proxy route cache entries.\n",
        );
        out.push_str("# TYPE temporalstore_proxy_route_cache_entries gauge\n");
        push_proxy_metric(
            &mut out,
            "temporalstore_proxy_route_cache_entries",
            &[],
            self.client().route_cache_size() as u64,
        );

        out.push_str("# HELP temporalstore_proxy_route_cache_events_total Proxy route cache and refresh events.\n");
        out.push_str("# TYPE temporalstore_proxy_route_cache_events_total counter\n");
        for (kind, value) in [
            ("hit", stats.route_cache_hits),
            ("miss", stats.route_cache_misses),
            ("refresh", stats.route_refreshes),
            // Metaserver topology checks skipped because one was made within the interval.
            // This is the count of round-trips kept off the request path, so it belongs
            // where the rest of the route-cache maintenance is reported.
            ("topology_check_skipped", stats.topology_checks_skipped),
            ("topology_check_failed", stats.topology_check_failures),
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_route_cache_events_total",
                &[("kind", kind)],
                value,
            );
        }

        out.push_str("# HELP temporalstore_proxy_backend_events_total Proxy backend and metaserver failure counters.\n");
        out.push_str("# TYPE temporalstore_proxy_backend_events_total counter\n");
        for (kind, value) in [
            ("backend_error", stats.backend_errors),
            (
                "continuous_backend_failure",
                stats.continuous_backend_failures,
            ),
            ("write_of_unknown_outcome", stats.writes_of_unknown_outcome),
            ("metaserver_error", stats.metaserver_errors),
            ("client_meta_sync_error", client.meta_sync_errors),
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_backend_events_total",
                &[("kind", kind)],
                value,
            );
        }

        out.push_str("# HELP temporalstore_proxy_serving_mode Current proxy serving mode as a one-hot gauge.\n");
        out.push_str("# TYPE temporalstore_proxy_serving_mode gauge\n");
        for mode in [
            ProxyServingMode::Serving,
            ProxyServingMode::Readonly,
            ProxyServingMode::WriteDisabled,
            ProxyServingMode::Degraded,
            ProxyServingMode::NotServing,
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_serving_mode",
                &[("mode", proxy_serving_mode_label(mode))],
                u64::from(options.serving_mode == mode),
            );
        }

        out.push_str("# HELP temporalstore_proxy_drop_percent Current proxy deterministic drop percentage.\n");
        out.push_str("# TYPE temporalstore_proxy_drop_percent gauge\n");
        push_proxy_metric(
            &mut out,
            "temporalstore_proxy_drop_percent",
            &[],
            options.drop_percent as u64,
        );

        let (inflight_total, inflight_writes) = self.inflight_snapshot();
        out.push_str(
            "# HELP temporalstore_proxy_inflight_requests Requests currently in flight through this proxy.\n",
        );
        out.push_str("# TYPE temporalstore_proxy_inflight_requests gauge\n");
        for (kind, value) in [("total", inflight_total), ("write", inflight_writes)] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_inflight_requests",
                &[("kind", kind)],
                value,
            );
        }

        out.push_str(
            "# HELP temporalstore_proxy_inflight_limit Configured in-flight admission quota; 0 is unlimited.\n",
        );
        out.push_str("# TYPE temporalstore_proxy_inflight_limit gauge\n");
        for (kind, value) in [
            ("total", options.max_inflight_requests),
            ("write", options.max_inflight_write_requests),
        ] {
            push_proxy_metric(
                &mut out,
                "temporalstore_proxy_inflight_limit",
                &[("kind", kind)],
                value,
