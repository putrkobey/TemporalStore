// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! MetaRaftCluster methods, split from raft.rs.
use super::*;

impl MetaRaftCluster {
    /// Local metaserver Raft fixture for unit tests and validation harnesses.
    ///
    /// Production metaserver Raft must use the networked production runtime.
    pub fn new(node_ids: impl IntoIterator<Item = RaftNodeId>) -> Self {
        Self::new_with_config(node_ids, RaftConfig::default())
            .expect("default raft config must be valid")
    }

    /// Local metaserver Raft fixture with explicit config for tests/harnesses.
    pub fn new_with_config(
        node_ids: impl IntoIterator<Item = RaftNodeId>,
        config: RaftConfig,
    ) -> Result<Self, RaftError> {
        config
            .validate()
            .map_err(|err| RaftError::InvalidConfig(err.to_string()))?;
        let mut nodes = BTreeMap::new();
        let mut iter = node_ids.into_iter();
        let leader_id = iter.next().unwrap_or(1);
        nodes.insert(leader_id, new_meta_node(leader_id, RaftRole::Leader));
        for node_id in iter {
            nodes.insert(node_id, new_meta_node(node_id, RaftRole::Follower));
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(MetaRaftClusterInner {
                leader_id,
                nodes,
                config,
            })),
        })
    }

    /// Apply the conviction lock to every node's metadata.
    ///
    /// `TS_META_FORBID_SELF_CLEARING_CONVICTION` was read after `from_env` had
    /// already returned the raft backend, so it reached the single-node
    /// metaserver and nothing else. The checks that consult it run on these
    /// nodes, against a flag that was always false.
    pub fn set_conviction_lock(&self, forbid: bool) {
        let mut inner = self.inner.write().expect("meta raft lock poisoned");
        for node in inner.nodes.values_mut() {
            node.meta.set_conviction_lock(forbid);
        }
    }

    pub fn propose(&self, command: MetaCommand) -> Result<(), RaftError> {
        self.propose_inner(command).map(|_| ())
    }

    pub fn propose_mutation(&self, mutation: MetaMutation) -> Result<Status, RaftError> {
        Ok(self
            .propose_inner(MetaCommand::ApplyMutation(mutation))?
            .unwrap_or_else(|| {
                // `apply_meta_committed` answers `None` only when it applied
                // nothing, which for a change just appended and committed means
                // the entry was skipped as already applied. Reporting that as
                // success is what let a numbering defect discard every metadata
                // change after a snapshot install while every caller was told
                // the change had been made.
                //
                // Not reachable today, and that is the point: this is the
                // difference between the next defect of that shape being loud
                // and being silent.
                Status::error(
                    "mutation_not_applied",
                    "the metaserver committed this change and applied nothing",
                )
            }))
    }

    pub fn register(&self, request: RegisterShardRequest) -> RegisterShardResponse {
        RegisterShardResponse {
            status: self.mutation_status(MetaMutation::RegisterShard(request)),
        }
    }

    pub fn get(&self, shard_id: ShardId) -> GetShardResponse {
        self.read_meta().map_or_else(
            |status| GetShardResponse {
                status,
                location: None,
            },
            |meta| meta.get(shard_id),
        )
    }

    pub fn register_server(&self, request: RegisterServerRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::RegisterServer(request)),
        }
    }

    pub fn server_heartbeat(&self, request: ServerHeartbeatRequest) -> ServerHeartbeatResponse {
        self.read_meta().map_or_else(
            |status| ServerHeartbeatResponse {
                status,
                forbid_auto_register: true,
                topology_version: 0,
                server_state: String::new(),
            },
            |meta| meta.server_heartbeat(request),
        )
    }

    pub fn register_proxy(&self, request: RegisterProxyRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::RegisterProxy(request)),
        }
    }

    pub fn proxy_heartbeat(&self, request: ProxyHeartbeatRequest) -> ProxyHeartbeatResponse {
        self.read_meta().map_or_else(
            |status| ProxyHeartbeatResponse {
                status,
                config_changed: false,
                namespace: String::new(),
                config_version: 0,
                serving_mode: "not_serving".to_string(),
                drop_percent: None,
            },
            |meta| meta.proxy_heartbeat(request),
        )
    }

    pub fn add_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::AddNamespace(request)),
        }
    }

    pub fn add_table(&self, request: AddTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::AddTable(request)),
        }
    }

    pub fn delete_table(&self, request: DeleteTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DeleteTable(request)),
        }
    }

    pub fn update_table(&self, request: UpdateTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::UpdateTable(request)),
        }
    }

    pub fn freeze_table(&self, request: DeleteTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::FreezeTable(request)),
        }
    }

    pub fn unfreeze_table(&self, request: DeleteTableRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::UnfreezeTable(request)),
        }
    }

    pub fn finish_load(&self, request: LoadFinishRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::FinishLoad(request)),
        }
    }

    pub fn publish_shard_snapshot(&self, request: PublishShardSnapshotRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::PublishShardSnapshot(request)),
        }
    }

    pub fn update_server(&self, request: crate::meta::UpdateServerRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::UpdateServer(request)),
        }
    }

    pub fn notify_server_stop(&self, request: crate::meta::NotifyStopRequest) -> AckResponse {
        let serving = self
            .list_servers()
            .servers
            .into_iter()
            .find(|server| server.server_addr == request.endpoint);
        match serving {
            None => AckResponse {
                status: Status::error("server_not_found", "server not found"),
            },
            Some(server) if server.state != MetaEntityState::Normal => AckResponse {
                status: Status::error("not_modified", "server is already out of service"),
            },
            Some(_) => self.freeze_server(StateChangeRequest {
                endpoint: request.endpoint,
                freeze_cooldown_ms: 0,
                reason: crate::meta::FreezeReason::Stopping,
            }),
        }
    }

    pub fn notify_proxy_stop(&self, request: crate::meta::NotifyStopRequest) -> AckResponse {
        let serving = self
            .list_proxies()
            .proxies
            .into_iter()
            .find(|proxy| proxy.proxy_addr == request.endpoint);
        match serving {
            None => AckResponse {
                status: Status::error("proxy_not_found", "proxy not found"),
            },
            Some(proxy) if proxy.state != MetaEntityState::Normal => AckResponse {
                status: Status::error("not_modified", "proxy is already out of service"),
            },
            Some(_) => self.drop_proxy(StateChangeRequest {
                endpoint: request.endpoint,
                freeze_cooldown_ms: 0,
                reason: crate::meta::FreezeReason::Stopping,
            }),
        }
    }

    /// Mute or resume metadata change across the raft group.
    /// Whether metadata change is muted, or `None` when no quorum can answer.
    ///
    /// The mute could be set through this backend but never read back through
    /// it. Reporting `false` when the cluster cannot be read would claim
    /// changes are flowing at exactly the moment nothing can be known.
    pub fn meta_change_muted(&self) -> Option<bool> {
        self.read_meta().ok().map(|meta| meta.is_meta_change_muted())
    }

    pub fn set_meta_change_muted(&self, muted: bool) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::SetMetaChangeMuted(muted)),
        }
    }

    pub fn freeze_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::SetNamespaceState(
                request,
                MetaEntityState::Frozen,
            )),
        }
    }

    pub fn unfreeze_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::SetNamespaceState(
                request,
                MetaEntityState::Normal,
            )),
        }
    }

    pub fn drop_namespace(&self, request: AddNamespaceRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::SetNamespaceState(
                request,
                MetaEntityState::Dropped,
            )),
        }
    }

    pub fn freeze_shard(&self, request: crate::meta::ShardStateRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::SetShardState(
                request,
                MetaEntityState::Frozen,
            )),
        }
    }

    pub fn unfreeze_shard(&self, request: crate::meta::ShardStateRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::SetShardState(
                request,
                MetaEntityState::Normal,
            )),
        }
    }

    pub fn drop_shard(&self, request: crate::meta::ShardStateRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DropShard(request)),
        }
    }

    pub fn reserved_names(&self) -> crate::meta::ReservedNamesResponse {
        self.read_meta().map_or_else(
            |status| crate::meta::ReservedNamesResponse {
                status,
                reserved: crate::meta::ReservedNames::default(),
            },
            |meta| meta.reserved_names(),
        )
    }

    pub fn set_reserved_names(&self, reserved: crate::meta::ReservedNames) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::SetReservedNames(reserved)),
        }
    }

    pub fn topology_events(
        &self,
        request: crate::meta::TopologyEventsRequest,
    ) -> crate::meta::TopologyEventsResponse {
        self.read_meta().map_or_else(
            |status| crate::meta::TopologyEventsResponse {
                status,
                events: Vec::new(),
                oldest_retained_version: 0,
                missed_events: false,
            },
            |meta| meta.topology_events(request),
        )
    }

    pub fn freeze_server(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::FreezeServer(request)),
        }
    }

    pub fn unfreeze_server(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::UnfreezeServer(request)),
        }
    }

    pub fn drop_server(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DropServer(request)),
        }
    }

    pub fn put_proxy_group(&self, request: crate::meta::PutProxyGroupRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::PutProxyGroup(request)),
        }
    }

    pub fn drop_proxy_group(&self, request: crate::meta::DropProxyGroupRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DropProxyGroup(request)),
        }
    }

    pub fn list_proxy_groups(&self) -> crate::meta::ListProxyGroupsResponse {
        self.read_meta().map_or_else(
            |status| crate::meta::ListProxyGroupsResponse {
                status,
                groups: Vec::new(),
            },
            |meta| meta.list_proxy_groups(),
        )
    }

    pub fn freeze_proxy(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::FreezeProxy(request)),
        }
    }

    pub fn unfreeze_proxy(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::UnfreezeProxy(request)),
        }
    }

    pub fn drop_proxy(&self, request: StateChangeRequest) -> AckResponse {
        AckResponse {
            status: self.mutation_status(MetaMutation::DropProxy(request)),
        }
    }

    pub fn freeze_stale_servers(&self, stale_after_ms: u64) -> StaleServerReport {
        let report = self.freeze_stale_resources_with_policy(
            stale_after_ms,
            SafeModePolicy {
                server_freeze_cooldown_ms: 0,
                proxy_freeze_cooldown_ms: 0,
            },
        );
        StaleServerReport {
            status: report.status,
            frozen_servers: report.frozen_servers,
        }
    }

    pub fn freeze_stale_resources_with_policy(
        &self,
        stale_after_ms: u64,
        policy: SafeModePolicy,
    ) -> StaleResourceReport {
        let now = current_time_ms();
        let servers = self.list_servers();
        if !servers.status.ok {
            return StaleResourceReport {
                status: servers.status,
                frozen_servers: Vec::new(),
                frozen_proxies: Vec::new(),
            };
        }
        let proxies = self.list_proxies();
        if !proxies.status.ok {
            return StaleResourceReport {
                status: proxies.status,
                frozen_servers: Vec::new(),
                frozen_proxies: Vec::new(),
            };
        }

        let mut frozen_servers = Vec::new();
        for server in servers.servers {
            if server.state == MetaEntityState::Normal
                && now.saturating_sub(server.last_heartbeat_ms) > stale_after_ms
            {
                let status = self.freeze_server(StateChangeRequest {
                    endpoint: server.server_addr.clone(),
                    freeze_cooldown_ms: policy.server_freeze_cooldown_ms,
                    reason: crate::meta::FreezeReason::Unresponsive,
                });
                if !status.status.ok {
                    return StaleResourceReport {
                        status: status.status,
                        frozen_servers,
                        frozen_proxies: Vec::new(),
                    };
                }
                frozen_servers.push(server.server_addr);
            }
        }

        let mut frozen_proxies = Vec::new();
        for proxy in proxies.proxies {
            if proxy.state == MetaEntityState::Normal
                && now.saturating_sub(proxy.last_heartbeat_ms) > stale_after_ms
            {
                let status = self.freeze_proxy(StateChangeRequest {
                    endpoint: proxy.proxy_addr.clone(),
                    freeze_cooldown_ms: policy.proxy_freeze_cooldown_ms,
                    reason: crate::meta::FreezeReason::Unresponsive,
                });
                if !status.status.ok {
                    return StaleResourceReport {
                        status: status.status,
                        frozen_servers,
                        frozen_proxies,
                    };
                }
                frozen_proxies.push(proxy.proxy_addr);
            }
        }

        StaleResourceReport {
            status: Status::ok(),
            frozen_servers,
            frozen_proxies,
        }
    }

    pub fn safe_mode_report(&self) -> SafeModeReport {
        self.read_meta().map_or_else(
            |status| SafeModeReport {
                status,
                blocked_servers: Vec::new(),
                blocked_proxies: Vec::new(),
                server_count: 0,
                proxy_count: 0,
            },
            |meta| meta.safe_mode_report(),
        )
    }

    pub fn list_shards(&self, request: crate::meta::ListShardsRequest) -> ListShardsResponse {
        self.read_meta().map_or_else(
            |status| ListShardsResponse {
                status,
                shards: Vec::new(),
                next_after_shard_id: None,
            },
            |meta| meta.list_shards(request),
        )
    }

    pub fn list_servers(&self) -> ListServersResponse {
        self.read_meta().map_or_else(
            |status| ListServersResponse {
                status,
                servers: Vec::new(),
            },
            |meta| meta.list_servers(),
