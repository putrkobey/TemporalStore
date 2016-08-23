// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! ProxyService meta topology refresh + heartbeat/service-discovery, split from proxy.rs.
use super::*;

impl ProxyService {
    pub fn refresh_topology_from_meta(&self) -> ProxyTopologyRefreshResponse {
        match self.client().refresh_stale_routes_from_meta() {
            Ok(report) => ProxyTopologyRefreshResponse {
                status: report.status.clone(),
                report: Some(report),
            },
            Err(err) => {
                self.inner
                    .stats
                    .write()
                    .expect("proxy stats lock poisoned")
                    .metaserver_errors += 1;
                ProxyTopologyRefreshResponse {
                    status: Status::error("refresh_failed", err.to_string()),
                    report: None,
                }
            }
        }
    }

    /// Ask the configured metaserver which node leads its raft group.
    ///
    /// This proxy holds ONE `meta_addr`. Pointed at a follower it gets `meta_not_ready` back for
    /// control-plane work and nothing in its own diagnostics says why, because the answer lives
    /// on the metaserver rather than here. Reported so an operator can see the address they
    /// configured, the address that leads, and whether they are the same.
    pub(super) fn fetch_meta_leader(&self) -> Result<crate::meta::MetaRaftLeaderResponse, Status> {
        let options = self.options();
        crate::http::get_json_with_options_and_headers::<crate::meta::MetaRaftLeaderResponse>(
            &options.meta_addr,
            "/meta/raft/leader",
            &crate::meta::admin_auth_header(),
            options.control_http_options(),
        )
        .map_err(|err| Status::error("meta_leader_unavailable", err.to_string()))
    }

    pub(super) fn fetch_meta_topology_version(
        &self,
        old_topology_version: u64,
    ) -> Result<TopologyVersionReport, Status> {
        let options = self.options();
        post_json_with_options_and_headers::<_, TopologyVersionReport>(
            &options.meta_addr,
            "/meta/topology_version",
            &TopologyVersionRequest {
                old_topology_version,
            },
            &crate::meta::admin_auth_header(),
            options.control_http_options(),
        )
        .map_err(|err| Status::error("topology_check_failed", err.to_string()))
    }

    pub fn heartbeat_to_meta(&self) -> ProxyHeartbeatResponse {
        let options = self.options();
        self.inner
            .stats
            .write()
            .expect("proxy stats lock poisoned")
            .heartbeat_total += 1;
        let request = ProxyHeartbeatRequest {
            proxy_addr: options.proxy_addr.clone(),
            boot_time_ms: self.inner.boot_time_ms,
            namespace: options.namespace.clone(),
            config_version: proxy_config_version(&options),
            binary_version: options.binary_version.clone(),
        };
        match post_json_with_options_and_headers::<_, ProxyHeartbeatResponse>(
            &options.meta_addr,
            "/proxies/heartbeat",
            &request,
            &crate::meta::admin_auth_header(),
            options.control_http_options(),
        ) {
            Ok(response) if response.status.ok || response.status.code == "resource_frozen" => {
                // The metaserver is reachable and answering, so this is the cheapest moment to
                // learn how many shards the cluster has.
                self.refresh_cluster_shard_count();
                self.record_service_discovery_heartbeat(&response.status);
                self.apply_heartbeat_config(&response);
                response
            }
            Ok(response) if response.status.code == "not_found" => {
                // The metaserver does not know this proxy, and registering again is the right
                // answer -- but not on every single heartbeat. A metaserver that keeps saying
                // not_found would otherwise take a registration plus a second heartbeat from
                // every proxy, every interval, which is the heaviest load at the worst moment.
                if !self.auto_register_is_due() {
                    self.inner
                        .stats
                        .write()
                        .expect("proxy stats lock poisoned")
                        .auto_register_throttled += 1;
                    self.record_service_discovery_error(&response.status);
                    return response;
                }
                if self.auto_register_proxy(&options).status.ok {
                    let response = post_json_with_options_and_headers::<_, ProxyHeartbeatResponse>(
                        &options.meta_addr,
                        "/proxies/heartbeat",
                        &request,
                        &crate::meta::admin_auth_header(),
                        options.control_http_options(),
                    )
                    .unwrap_or_else(|err| ProxyHeartbeatResponse {
                        status: Status::error("metaserver_error", err.to_string()),
                        config_changed: false,
                        namespace: String::new(),
                        config_version: 0,
                        serving_mode: "not_serving".to_string(),
                        drop_percent: None,
                    });
                    if response.status.ok {
                        self.record_service_discovery_heartbeat(&response.status);
                        self.apply_heartbeat_config(&response);
                    } else {
                        self.record_service_discovery_error(&response.status);
                    }
                    response
                } else {
                    self.record_service_discovery_error(&response.status);
                    response
                }
            }
            Ok(response) => {
                // The metaserver REACHED us and said no -- the proxy was dropped, re-scoped,
                // or is otherwise not the owner of what it thinks it owns. Drop the local
                // namespace/config authority so it stops acting on a grant that has been
                // withdrawn; the next accepted heartbeat hands back the real one, and until
                // then reporting an empty namespace is what makes the metaserver mark the
                // config changed and re-send it.
                //
                // Note this is the EXPLICIT-rejection branch only. A transport failure lands
                // in `Err` below and deliberately changes nothing: an unreachable or slow
                // metaserver must not cost every proxy its configuration.
                self.clear_config_authority();
                self.record_service_discovery_error(&response.status);
                response
            }
            Err(err) => {
                let status = Status::error("metaserver_error", err.to_string());
                self.record_service_discovery_error(&status);
                ProxyHeartbeatResponse {
                    status,
                    config_changed: false,
                    namespace: String::new(),
                    config_version: 0,
                    serving_mode: "not_serving".to_string(),
                    drop_percent: None,
                }
            }
        }
    }
