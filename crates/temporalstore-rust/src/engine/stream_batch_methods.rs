// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Stream read/scan + batch execute + index snapshot publish methods for TemporalEngine, split from engine.rs.
use super::*;

impl TemporalEngine {
    pub fn read_stream(&self, request: StreamReadRequest) -> StreamReadResponse {
        let data: Result<Vec<u8>, String> = match request.stream_kind {
            StreamKind::Block => self
                .block_store
                .read_logical_range(request.block_slab_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
            StreamKind::Index => {
                // Serve the complete current index through the funnel (live in-memory on
                // the delta path, base file otherwise), then slice the requested window.
                // Preserves the original missing-file -> error semantics.
                self.load_served_index_bytes(request.shard_id)
                    .map_err(|err| err.to_string())
                    .map(|bytes| {
                        let start = request.offset as usize;
                        let end = start.saturating_add(request.size as usize).min(bytes.len());
                        if start >= bytes.len() {
                            Vec::new()
                        } else {
                            bytes[start..end].to_vec()
                        }
                    })
            }
            StreamKind::Wal => self
                .wal_store
                .read_range(request.shard_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
            StreamKind::IndexLog => self
                .index_log_store
                .read_range(request.shard_id, request.offset, request.size)
                .map_err(|err| err.to_string()),
        };
        match data {
            Ok(data) => StreamReadResponse {
                status: Status::ok(),
                data,
            },
            Err(err) => StreamReadResponse {
                status: Status::error("stream_read_failed", err.to_string()),
                data: Vec::new(),
            },
        }
    }

    pub fn scan_stream(&self, request: ScanStreamRequest) -> ScanStreamResponse {
        if request.start_offset > request.end_offset {
            return ScanStreamResponse {
                status: Status::error("invalid_stream_range", "start_offset is after end_offset"),
                records: Vec::new(),
                end_of_stream: true,
            };
        }
        // A zero budget means "no budget named", not "a budget of zero bytes".
        //
        // The byte-addressed kinds below clamp the window to `max_bytes`, so zero clamped the
        // read to nothing: no records, and `end_of_stream` FALSE, because the end of the window
        // had not been reached. A caller walking `while !end_of_stream` then asked again at the
        // same offset, for ever -- a walk that cannot advance.
        //
        // The record-framed kinds never had this. `scan_collect` keeps its first record whatever
        // the budget says (`total > 0 &&`), for exactly this reason, and its comment says so. That
        // rule belongs to the whole surface rather than one half of it: the two halves answered
        // one and zero records to the same zero budget. The rest of this tree already reads an
        // unset budget as no limit rather than as a limit of nothing.
        let max_bytes = if request.max_bytes == 0 {
            u64::MAX
        } else {
            request.max_bytes
        };
        let size = request
            .end_offset
            .saturating_sub(request.start_offset)
            .min(max_bytes);
        if request.stream_kind == StreamKind::Wal || request.stream_kind == StreamKind::IndexLog {
            let records = match request.stream_kind {
                StreamKind::Wal => self
                    .wal_store
                    .scan_bounded(
                        request.shard_id,
                        request.start_offset,
                        request.end_offset,
                        max_bytes,
                    )
                    .map_err(|err| err.to_string()),
                StreamKind::IndexLog => self
                    .index_log_store
                    .scan_bounded(
                        request.shard_id,
                        request.start_offset,
                        request.end_offset,
                        max_bytes,
                    )
                    .map_err(|err| err.to_string()),
                StreamKind::Index | StreamKind::Block => unreachable!(),
            };
            return match records {
                // `truncated` is the whole point of asking: the walk stops both when the window
                // ends and when max_bytes runs out, and this used to answer "end of stream" for
                // either. A caller reading a range larger than its budget was handed a prefix
                // and told it had the lot.
                Ok((records, truncated)) => ScanStreamResponse {
                    status: Status::ok(),
                    records: records
                        .into_iter()
                        .map(|(offset, data)| StreamRecord { offset, data })
                        .collect(),
                    end_of_stream: !truncated,
                },
                Err(err) => ScanStreamResponse {
                    status: Status::error("stream_scan_failed", err.to_string()),
                    records: Vec::new(),
                    end_of_stream: true,
                },
            };
        }
        let read = self.read_stream(StreamReadRequest {
            shard_id: request.shard_id,
            stream_kind: request.stream_kind,
            block_slab_id: request.block_slab_id,
            offset: request.start_offset,
            size,
        });
        // A byte-addressed read is capped the same way -- `size` above is the window clamped to
        // max_bytes -- so it ends the stream when it reached the offset that was asked for.
        //
        // It also ends when the read came back with NOTHING. That case used to fall through to
        // the arithmetic below, where zero bytes can never reach `end_offset`, so a scan that
        // found nothing answered end_of_stream FALSE. A caller walking `while !end_of_stream`
        // advances by the bytes it was handed, is handed none, asks again at the same offset and
        // is told the same thing -- a walk that cannot terminate.
        //
        // All three byte-addressed kinds can land here. `Index` returns an empty window for a
        // start at or past the end of the served index, and `logical_range_from_slab` does the
        // same for `Block` and `Page` on both its raw and its framed path.
        //
        // An empty answer means the stream ended inside the window rather than that nothing was
        // asked for. A window of zero width (`start_offset == end_offset`, which the check above
        // permits) is the one case where nothing WAS asked for, and it already answered true by
        // the arithmetic below -- `start + 0 >= end` holds when the two are equal -- so the new
        // clause agrees with it rather than changing it. Everywhere else `size` is at least one
        // byte, because a zero budget has already been read as no budget.
        //
        // This is the rule the record-framed half already follows: `scan_bounded` reports
        // `truncated` only when the BUDGET cut the walk short, so a walk that simply ran out of
        // records reports the end of the stream. The two halves now answer alike.
        let end_of_stream = !read.status.ok
            || read.data.is_empty()
            || request
                .start_offset
                .saturating_add(read.data.len() as u64)
                >= request.end_offset;
        ScanStreamResponse {
            status: read.status.clone(),
            records: if read.status.ok && !read.data.is_empty() {
                vec![StreamRecord {
                    offset: request.start_offset,
                    data: read.data,
                }]
            } else {
                Vec::new()
            },
            end_of_stream,
        }
    }

    pub fn batch_execute(&self, request: BatchExecuteRequest) -> BatchExecuteResponse {
        // Every command here executes through `execute_on_shard`, which STAGES an outcome item,
        // and the records this path appends are built from the commands -- so nothing ever took
        // what was staged. Those items stayed in the thread's buffer, and the next write on that
        // thread appended them as its own doing. Threads are reused across requests, so "the next
        // write" is routinely a different request entirely.
        //
        // A guard rather than a drain at the end, because this function returns early on a
        // missing shard and on a failed durable commit, and those exits leaked too. Discarding is
        // correct while batch records carry their commands: replay re-executes them, so the items
        // describe work that is already accounted for.
        struct DrainStagedOnExit;
        impl Drop for DrainStagedOnExit {
            fn drop(&mut self) {
                let _ = super::block_in_wal::take_outcomes();
            }
        }
        let _drain_staged = DrainStagedOnExit;
        let command_count = request.commands.len();
        let mut responses = Vec::with_capacity(command_count);
        if command_count == 0 {
            return BatchExecuteResponse {
                status: Status::ok(),
                responses,
            };
        }
        let mut shards = self.shards.write().expect("engine lock poisoned");
        let Some(shard) = shards.get_mut(&request.shard_id) else {
            // returns a BATCH-LEVEL topology error with ZERO CmdResponse entries when the
            // partition is missing/not-primary (a topology error: the client should
            // refresh its shard map), not an OK batch full of per-command errors. The Rust client
            // treats a batch-level shard_not_loaded status as topology-retryable
            // (client/client_retry.rs status_is_topology_retryable) and refreshes + retries -- but it
            // keys on the batch-level status, which the old ok+N-errors shape left ok, so the
            // client never refreshed and kept hitting the wrong node. Return it batch-level.
            return BatchExecuteResponse {
                status: Status::error("shard_not_loaded", "shard is not loaded on this server"),
                responses: Vec::new(),
            };
        };
        // Mirror execute_with_storage_override: a shard replaying its WAL on load is present
        // but not yet serving. Reject the whole batch with a retryable status so batch writes
        // cannot interleave with replay (anchor regression / double-apply) -- the batch path
        // must not be a hole around the single-execute recovery gate. Replay itself never
        // routes through batch_execute, but replaying_wal() is checked for symmetry.
        if !replaying_wal()
            && self
                .infos
                .read()
                .expect("info lock poisoned")
                .get(&request.shard_id)
                .map(|info| info.recovering)
                .unwrap_or(false)
        {
            // Batch-level topology-retryable error with empty responses, as above (returns a
            // partition-level error here, not a per-command one), so the client refreshes topology
            // and retries rather than seeing an ok batch of failed commands.
            return BatchExecuteResponse {
                status: Status::error(
                    "shard_not_loaded",
                    "shard is recovering (WAL replay in progress)",
                ),
                responses: Vec::new(),
            };
        }
        let readonly = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .map(|info| info.readonly)
            .unwrap_or(false);
        let config = self
            .configs
            .read()
            .expect("config lock poisoned")
            .get(&request.shard_id)
            .cloned()
            .unwrap_or_default();
        let info = self
            .infos
            .read()
            .expect("info lock poisoned")
            .get(&request.shard_id)
            .cloned();
        let start_routing_bucket = info
            .as_ref()
            .map(|info| info.start_routing_bucket)
            .unwrap_or_default();
        let end_routing_bucket = info
            .as_ref()
            .map(|info| info.end_routing_bucket)
            .unwrap_or(u32::MAX);
        // Phase-1 flat-append fast-skip, the same one `execute_with_storage_override` applies to
        // single commands. Without it the batch path pays the reconcile scan on EVERY batch, and
        // that scan walks and CLONES every live model-map entry in the shard just to re-confirm
        // that `bucket_index` -- which each mutating command already upserts before returning --
        // is in sync. Measured on a 290 MB store, that made an ingest 1.65 s of pure proxy CPU
        // against 0.15 s on a 26 MB one, and it was the single hottest frame in a stack profile
        // of the write path. Every mem0 write arrives here, so the guarded path was the one that
        // mattered and the unguarded one was the one being used.
        //
        // `promote_scan_done` is `#[serde(skip)]`, so a freshly loaded shard still pays exactly
        // one full reconcile before the skip engages, and with the gate off the scan runs on
        // every batch precisely as before.
        if !(self.wal_store.flat_append() && shard.promote_scan_done) {
            self.promote_scans
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if promote_model_maps_to_bucket_index_authority(
                request.shard_id,
                shard,
                start_routing_bucket,
                end_routing_bucket,
            ) {
                reconcile_secondary_views_from_bucket_index(&self.block_store, shard, None);
            }
            // Latch only once the shard actually holds model-map state: `promote` returns false
            // without establishing anything on an empty shard, so guarding on non-emptiness
            // avoids latching before the first real write.
            if self.wal_store.flat_append() && shard_has_model_entries(shard) {
                shard.promote_scan_done = true;
            }
        }
        let mut mutated_any = false;
        // Set if any command in this batch rebuilt the bucket index, which invalidates the
        // per-batch record of touched buckets and forces the full sweep below.
        let mut rebuilt_bucket_index = false;
        let mut wal_commands = Vec::new();
        // Accumulate the object keys every mutating command in the batch touched, for the
        // single O(delta) index-log append below (delta path). Empty when the flag is off.
        let mut delta_command_keys: Vec<String> = Vec::new();
        // Did ANY command in this batch remove something? Only then does the per-key membership
        // capture below have anything to preserve -- see the single-command execute path, whose
        // guard this mirrors.
        let mut batch_membership_shrank = false;
        // Some(components) while every mutating command in the batch is a pure page-upsert;
        // one non-upsert write downgrades the whole record to snapshot semantics.
        let mut batch_upsert_components: Option<Vec<(&'static str, String, Option<String>)>> =
            Some(Vec::new());
        // Every container element any command in this batch added or replaced. Unlike the
        // components above this is never downgraded to `None`: it is a list of elements, not a
        // claim that the whole record is a pure upsert, so one non-upsert command in the batch
        // does not make the others' identities uncarryable.
        let mut batch_touched_container_elements: Vec<TouchedContainerElement> = Vec::new();
        for command in request.commands {
            let write_command = is_write_command(&command);
            if readonly && write_command {
                responses.push(ExecuteResponse {
                    status: Status::error("readonly_shard", "readonly shard rejects write command"),
                    response: CommandResponse::Empty,
                });
                continue;
            }
            // Exempt under raft apply and under replay, as the single-command path is: a
            // follower that refuses what its leader committed diverges from the leader, and a
            // replay that refuses a record already in the log costs the whole shard load.
            //
            // Both are SYMMETRY here, deliberately and stated as such. Today neither reaches
            // this function -- execute_raft_apply_batch and batch_execute_replicated both loop
            // over the single-command path, and replay never batches -- exactly as the
            // recovering-shard gate above says of replaying_wal(). A future batched apply that
            // does route here would otherwise arrive at an ungated limit, and the failure it
            // produces (a follower quietly skipping a committed entry) is not one that shows up
            // as an error anywhere.
            if !raft_applying() && !replaying_wal() {
                // Charged per COMMAND, not per batch. The single-command path spends one token a
                // command, and a batch is not one unit of work -- a limit that counted it as one
                // would be escaped by batching, which is how most traffic arrives here.
                let kind = if write_command {
                    quota::QuotaKind::Write
                } else {
                    quota::QuotaKind::Read
                };
                if !self.charge_quota(request.shard_id, kind) {
                    responses.push(ExecuteResponse {
                        status: Status::error(
                            "quota_exhausted",
                            format!(
                                "shard {} is over its {} rate limit",
                                request.shard_id,
                                match kind {
                                    quota::QuotaKind::Write => "write",
                                    quota::QuotaKind::Read => "read",
                                }
                            ),
                        ),
                        response: CommandResponse::Empty,
                    });
                    continue;
                }
                if let Err(status) =
                    self.check_admission(request.shard_id, write_command, &config, &info)
                {
                    responses.push(ExecuteResponse {
                        status,
                        response: CommandResponse::Empty,
                    });
                    continue;
                }
            }
            // Exempt under raft apply and under replay, as the single-command path is and for
            // the same reason. Symmetry here, on the same footing as the admission guard above:
            // neither reaches this function today.
            if !raft_applying()
                && !replaying_wal()
                && write_command
                && config
                    .maxmemory_bytes
                    // Current on-disk footprint (GC-decremented), not cumulative-ever
                    // bytes_written -- see the single-command execute path.
                    .map(|limit| self.block_store.slab_summary().total_known_physical_bytes >= limit)
                    .unwrap_or(false)
            {
                responses.push(ExecuteResponse {
                    status: Status::error(
                        "storage_quota_exceeded",
                        "shard maxmemory_bytes limit has been reached",
                    ),
                    response: CommandResponse::Empty,
                });
                continue;
            }
            if let Err(status) = validate_command_preconditions(
                &self.cache,
                &self.block_store,
                request.shard_id,
                shard,
                &command,
            ) {
                responses.push(ExecuteResponse {
                    status,
                    response: CommandResponse::Empty,
                });
                continue;
            }
            let command_for_post_write = command.clone();
            // What the touched keys held before this command, so the capture below can be
            // skipped when nothing was removed. Sizes only -- no allocation.
            let membership_before: Vec<(String, usize, Option<u64>)> =
                command_object_keys(&command_for_post_write)
                    .into_iter()
                    .map(|key| {
                        let size = key_membership_size(shard, &key);
                        let deadline = shard.expires_at_ms().get(key.as_str()).copied();
                        (key, size, deadline)
                    })
                    .collect();
            let outcome = execute_on_shard(
                &self.cache,
                &self.block_store,
                config.feature_max_size,
                config.async_storage,
                config.control_rollup_enabled(),
                config.control_coalesce_persist_enabled(),
                config.control_distinct_sketch_enabled(),
                request.shard_id,
                start_routing_bucket,
                end_routing_bucket,
                shard,
                command,
            );
            // LRU recency: stamp the bucket(s) this command
            // touched, read or write, so eviction can prefer least-recently-used buckets.
            {
                let now = now_ms();
                crate::alloc_probe::in_class(crate::alloc_probe::AllocClass::RecencyStamp, || {
                    for_each_touched_key(&command_for_post_write, |key| {
                        let recency_bucket =
                            block_routing_bucket(key, start_routing_bucket, end_routing_bucket);
                        shard.bucket_recency.insert(recency_bucket, now);
                    });
                });
            }
            if outcome.mutated {
                mutated_any = true;
                let object_keys = command_object_keys(&command_for_post_write);
                delta_command_keys.extend(object_keys.iter().cloned());
                // Same question as the single-command path, through the SAME function: a
                // membership shrink OR a deadline change. Two spellings of this test is how
                // one of them keeps a bug the other fixed, so there is only one.
                if !batch_membership_shrank {
                    batch_membership_shrank = delta_key_state_change(shard, &membership_before);
                }
                match (
                    &mut batch_upsert_components,
                    command_upsert_components(&command_for_post_write, shard),
                ) {
                    (Some(collected), Some(components)) => collected.extend(components),
                    (state, _) => *state = None,
                }
                batch_touched_container_elements.extend(command_touched_container_elements(
                    &command_for_post_write,
                    shard,
                ));
                // The keys this write touched, for the maintenance below. `object_keys` is
                // consumed by the loop, so they are kept as they go past.
                let mut object_keys_for_maintenance: Vec<String> = Vec::new();
                if object_keys.is_empty() {
                    rebuild_bucket_block_ownership(
                        request.shard_id,
                        shard,
                        start_routing_bucket,
                        end_routing_bucket,
                    );
                } else {
                    for object_key in object_keys {
                        object_keys_for_maintenance.push(object_key.clone());
                        // `mark_object_dirty` records the key together with the
                        // routing bucket it computes on the next line. Inserting it here as well
                        // was redundant when the set was flat and is wrong now: this site does
                        // not know the bucket, and a key filed without one belongs to no bucket.
                        mark_object_dirty(
                            shard,
                            &object_key,
                            start_routing_bucket,
                            end_routing_bucket,
                        );
                    }
                }
                // The same maintenance the single-command path does. An ingest's writes are
                // split across both paths, which is why covering only the other one removed
                // exactly half the rebuild work and left the rest: 2404 page visits per add
                // became 1205, and stayed there until this site was covered too.
                //
                // Same two guards: every touched key must report that it synced, or the rebuild
                // still runs; and an empty bucket_map still rebuilds, because maintenance updates
                // an index rather than constructing one.
                let maintained_bucket_index = if shard.bucket_index.bucket_map.is_empty() {
                    false
                } else {
                    // Prefer the components this command actually wrote. The whole-object sync
                    // below re-upserts every field the object holds, which is quadratic in that
                    // count; this is one upsert per written component. It reports false when it
                    // cannot model the write, and the whole-object sync then runs.
                    let written = command_upsert_components(&command_for_post_write, shard);
                    let narrow = match written.as_deref() {
                        Some(components) => {
                            crate::engine::storage_bucket_internals::sync_blocks_for_written_components(
                                shard,
                                request.shard_id,
                                components,
                            )
                        }
                        None => false,
                    };
                    narrow
                        || (!object_keys_for_maintenance.is_empty()
                            && object_keys_for_maintenance.iter().all(|object_key| {
                                crate::engine::storage_bucket_internals::sync_context_blocks_for_object(
                                    shard,
                                    request.shard_id,
                                    object_key,
                                )
                            }))
                };
                if !maintained_bucket_index
                    && !defer_bucket_index_reconstruct()
                    // A command that writes no page cannot have changed the page index, so a
                    // rebuild after one recomputes what it already held. The single-command path
                    // has always skipped it here; this path did not, so the same command walked
                    // the whole store when it arrived in a batch and nothing when it arrived
                    // alone -- measured at one visit per page in the shard, per command.
                    && !command_writes_no_block(&command_for_post_write)
                    && (!command_updates_bucket_index_directly(&command_for_post_write)
                        || shard.bucket_index.bucket_map.is_empty())
                {
                    rebuild_bucket_first_index(
                        request.shard_id,
                        shard,
                        start_routing_bucket,
                        end_routing_bucket,
                    );
                    // The rebuild replaced bucket_map wholesale, so the record of which buckets
                    // this batch touched no longer describes it.
                    rebuilt_bucket_index = true;
                }
                if write_command {
                    wal_commands.push(command_for_post_write);
                }
            }
            responses.push(ExecuteResponse {
                status: Status::ok(),
                response: outcome.response,
            });
        }
        if mutated_any {
            if rebuilt_bucket_index {
                // The reconstruct on the line above already recomputed every bucket's object
                // index from its page index, so the sweep's own rebuild would redo that identical
                // scan. Flags still refresh; only the duplicate scan is dropped.
                refresh_bucket_runtime_flags_after_reconstruct(shard);
            } else {
                // Refresh only the buckets this batch disturbed. The full sweep is
                // O(total pages), so running it per batch left bulk ingest quadratic in the
                // corpus -- with a smaller constant than the per-write sweep, but the same shape.
                refresh_pending_bucket_runtime_flags(shard);
            }
            // Every write records a WAL entry before any page is written. async_storage only
            // changes whether the commit BLOCKS: sync -> fsync, async (or bulk backfill) ->
            // buffered, no fsync (a fire-and-forget commit).
            //
            // The batch is logged as ONE crash-atomic group: all commands are appended under a
