// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! TemporalStoreClient meta topology sync + route refresh/invalidation + reports, split from client.rs.
use super::*;
use crate::meta::TableServingField;

impl TemporalStoreClient {
    pub fn sync_table_topology(
        &self,
        namespace: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<TableOptions, ClientError> {
        let namespace = namespace.into();
        let table_name = table_name.into();
        self.ensure_meta_sync_table_state(&namespace, &table_name);
        self.inner
            .stats
            .lock()
            .expect("client stats lock poisoned")
            .meta_sync_total += 1;
        let meta_addr = self
            .inner
            .options
            .meta_addr
            .as_ref()
            .ok_or_else(|| ClientError::Status("meta_addr is required".to_string()))?;
        let topology: TableTopologyResponse = match post_json_with_options_and_headers(
            meta_addr,
            "/tables/topology",
            &GetTableTopologyRequest {
                client_location: String::new(),
                namespace: namespace.clone(),
                table_name: table_name.clone(),
                old_topology_version: self.last_synced_topology_version(&namespace, &table_name),
            },
            &crate::meta::admin_auth_header(),
            self.inner.options.meta_sync_http_options(),
        ) {
            Ok(topology) => topology,
            Err(err) => {
                self.inner
                    .stats
                    .lock()
                    .expect("client stats lock poisoned")
                    .meta_sync_errors += 1;
                self.record_meta_sync_error(&namespace, &table_name, &err.to_string());
                return Err(err.into());
            }
        };
        if !topology.status.ok {
            self.inner
                .stats
                .lock()
                .expect("client stats lock poisoned")
                .meta_sync_errors += 1;
            self.record_meta_sync_error(&namespace, &table_name, &topology.status.message);
            return Err(ClientError::Status(topology.status.message));
        }
        let table = topology.table.ok_or_else(|| {
            self.record_meta_sync_error(&namespace, &table_name, "table topology missing");
            ClientError::Status("table topology missing".to_string())
        })?;
        // Asked for only when it is going to be used. Stamping routes needs the cluster's
        // topology version, and getting it is its own metaserver round-trip -- but an
        // unchanged reply installs no routes, so on that path the version is wanted for the
        // sync record alone, and the table's own version is already the right answer: the
        // metaserver has just confirmed it is current, which is why it answered unchanged.
        //
        // Asking anyway made every sync cost two round-trips instead of one, and since the
        // unchanged reply is the common case once a topology settles, that was the usual cost
        // rather than an occasional one.
        let route_topology_version = if topology.unchanged {
            table.topology_version
        } else {
            self.current_meta_topology_version()
                .unwrap_or(table.topology_version)
                .max(table.topology_version)
        };
        let serving_options = table.serving_options.clone();
        // Whether the table speaks for a field, or leaves it to this client's own
        // option. Asking the table settles it; the alternative -- inferring it from
        // "the value differs from the default" -- silently overrode any table that
        // chose a default value on purpose, which is exactly what `drop_percent: 0`
        // ("never shed this table") and `max_write_retries: 0` ("never retry a write
        // here") are.
        let table_decides = |field: TableServingField| serving_options.table_decides(field);
        let options = TableOptions {
            table_id: table.table_id,
            io_timeout_ms: if table_decides(TableServingField::IoTimeoutMs) {
                serving_options.io_timeout_ms
            } else {
                self.inner.options.io_timeout_ms
            },
            connect_timeout_ms: if table_decides(TableServingField::ConnectTimeoutMs) {
                serving_options.connect_timeout_ms
            } else {
                self.inner.options.connect_timeout_ms
            },
            continuous_failed_time_ms: if table_decides(TableServingField::ContinuousFailedTimeMs) {
                serving_options.continuous_failed_time_ms
            } else {
                TableOptions::default().continuous_failed_time_ms
            },
            first_shard_id: table.first_shard_id,
            shard_count: table.shard_count,
            partition_version: table.partition_version,
            pin_primary: serving_options.pin_primary,
            replica_read_policy: replica_read_policy_from_meta(
                &serving_options.replica_read_policy,
            ),
            preferred_location: if table_decides(TableServingField::PreferredLocation)
                && !serving_options.preferred_location.is_empty()
            {
                serving_options.preferred_location.clone()
            } else {
                self.inner.options.local_location.clone()
            },
            drop_percent: if table_decides(TableServingField::DropPercent) {
                serving_options.drop_percent.min(100)
            } else {
                self.inner.options.drop_percent.min(100)
            },
            max_read_retries: if table_decides(TableServingField::MaxReadRetries) {
                serving_options.max_read_retries as usize
            } else {
                self.inner.options.max_read_retries
            },
            max_write_retries: if table_decides(TableServingField::MaxWriteRetries) {
                serving_options.max_write_retries as usize
            } else {
                self.inner.options.max_write_retries
            },
            retry_backoff_ms: if table_decides(TableServingField::RetryBackoffMs) {
                serving_options.retry_backoff_ms
            } else {
                self.inner.options.retry_backoff_ms
            },
            ..TableOptions::default()
        };
        let table_key = table_combine_name(&namespace, &table_name);
        // Shards the topology names but cannot route, because it gives them no primary. This
        // happens while a primary is being elected. Their previous routes are deliberately
        // NOT discarded below: a snapshot taken mid-election should not destroy a route that
        // still works, and the old one either still serves or fails and gets refreshed.
        let unroutable: Vec<ShardId> = topology
            .shards
            .iter()
            .filter(|partition| partition.primary.is_none())
            .map(|partition| partition.shard_id)
            .collect();
        let routes = topology
            .shards
            .iter()
            .filter_map(|partition| {
                partition.primary.as_ref().map(|primary| {
                    (
                        partition.shard_id,
                        CachedRoute {
                            table_key: table_key.clone(),
                            partition_id: partition.shard_id,
                            start_bucket: partition.start_bucket,
                            end_bucket: partition.end_bucket,
                            partition_version: table.partition_version,
                            primary_addr: primary.clone(),
                            replica_addrs: partition
                                .replicas
                                .iter()
                                .filter(|replica| *replica != primary)
                                .cloned()
                                .collect(),
                            replica_endpoints: partition
                                .replica_endpoints
                                .iter()
                                .filter(|endpoint| endpoint.server_addr != *primary)
                                .cloned()
                                .collect(),
                            next_replica_index: std::sync::atomic::AtomicUsize::new(0),
                            fetched_at: Instant::now(),
                            topology_version: route_topology_version,
                            load_version: partition.load_version,
                            refresh_reason: "table_topology_sync".to_string(),
                        },
                    )
                })
            })
            .collect::<Vec<_>>();
        self.with_tables_mut(|tables| tables.insert(table_key.clone(), options.clone()));

        // "unchanged" means the metaserver did not rebuild the shard list because we already
        // have this version -- so the response carries NO shards, and running the route
        // surgery below against an empty list would delete every route this table has. The
        // serving options still came back and are applied above; the routes are already right.
        //
        // This has to be handled before the version is sent, not after: while the request was
        // hardcoded to version 0 the metaserver could never answer unchanged, which is the
        // only reason the wipe never happened.
        if topology.unchanged {
            self.record_meta_sync_success(
                &namespace,
                &table_name,
                route_topology_version,
                0,
            );
            return Ok(options);
        }

        let mut route_cache = self
            .inner
            .routes
            .write()
            .expect("client route cache lock poisoned");
        let last_shard_id = table
            .first_shard_id
            .saturating_add(table.shard_count.saturating_sub(1));
        route_cache.retain(|shard_id, route| {
            // Keep what the new topology cannot replace.
            if unroutable.contains(shard_id) {
                return true;
            }
            if route.table_key == table_key {
                return false;
            }
            *shard_id < table.first_shard_id || *shard_id > last_shard_id
        });
        for (shard_id, route) in routes {
            route_cache.insert(shard_id, route);
        }
        self.record_meta_sync_success(
            &namespace,
            &table_name,
            route_topology_version,
            unroutable.len() as u64,
        );
        Ok(options)
    }

    pub(super) fn current_meta_topology_version(&self) -> Option<u64> {
        let meta_addr = self.inner.options.meta_addr.as_ref()?;
        let topology = post_json_with_options_and_headers::<_, TopologyVersionReport>(
            meta_addr,
            "/meta/topology_version",
            &TopologyVersionRequest {
                old_topology_version: 0,
            },
            &crate::meta::admin_auth_header(),
            self.inner.options.meta_sync_http_options(),
        )
        .ok()?;
        topology
            .status
            .ok
            .then_some(topology.current_topology_version)
    }
