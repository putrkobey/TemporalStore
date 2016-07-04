// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! RaftCluster snapshot build/install/transfer methods, split from raft.rs.
use super::*;

impl RaftCluster {
    pub fn build_install_snapshot_request(
        &self,
        target_id: RaftNodeId,
    ) -> Result<InstallSnapshotRequest, RaftError> {
        self.build_install_snapshot_request_with_external_ref(target_id, None)
    }

    pub fn build_install_snapshot_request_with_external_ref(
        &self,
        target_id: RaftNodeId,
        external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    ) -> Result<InstallSnapshotRequest, RaftError> {
        let snapshot = self.create_snapshot()?;
        self.build_install_snapshot_request_from(target_id, snapshot, external_snapshot_ref)
    }

    /// The bookkeeping half of building an install request, against a snapshot the caller already
    /// has.
    ///
    /// Split out because a state-image snapshot IS the whole shard: building one costs a walk of
    /// the corpus, an index encode and a read of every slab. A caller that has already built one
    /// in order to decide something about it must not pay for a second.
    fn build_install_snapshot_request_from(
        &self,
        target_id: RaftNodeId,
        mut snapshot: RaftSnapshot,
        external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    ) -> Result<InstallSnapshotRequest, RaftError> {
        snapshot.external_snapshot_ref = external_snapshot_ref.clone();
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let shard_id = inner.shard_id;
        let term = leader.current_term;
        let leader_id = inner.leader_id;
        let logical_time_ms = inner.logical_time_ms;
        {
            let target = inner
                .nodes
                .get_mut(&target_id)
                .ok_or(RaftError::NodeNotFound(target_id))?;
            if target.pipeline_state.snapshot_sending || target.pipeline_state.snapshot_installing {
                target.pipeline_state.snapshot_backpressure_rejections = target
                    .pipeline_state
                    .snapshot_backpressure_rejections
                    .saturating_add(1);
                target.pipeline_state.snapshot_send_failed =
                    target.pipeline_state.snapshot_send_failed.saturating_add(1);
                inner.persist_configured_wal()?;
                return Err(RaftError::SnapshotBackpressure { node_id: target_id });
            }
            target.pipeline_state.snapshot_sending = true;
            target.pipeline_state.snapshot_installing = true;
            target.pipeline_state.snapshot_send_started_ms = Some(logical_time_ms);
            target.pipeline_state.snapshot_send_elapsed_ms = 0;
            target.pipeline_state.snapshot_installed_index = snapshot.last_included_index;
            target.pipeline_state.snapshot_send_attempts = target
                .pipeline_state
                .snapshot_send_attempts
                .saturating_add(1);
            target.pipeline_state.snapshot_install_received_chunks = 0;
            target.pipeline_state.snapshot_install_total_chunks = 1;
        }
        inner.persist_configured_wal()?;
        Ok(InstallSnapshotRequest {
            rpc: None,
            shard_id,
            term,
            leader_id,
            target_id,
            external_snapshot_ref,
            snapshot,
        })
    }

    pub fn plan_snapshot_bootstrap(
        &self,
        target_id: RaftNodeId,
        policy: RaftSnapshotTransferPolicy,
        external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    ) -> Result<RaftReplicaBootstrapPlan, RaftError> {
        let snapshot = self.create_snapshot()?;
        let transfer = decide_snapshot_transfer(&snapshot, policy, external_snapshot_ref.clone())?;
        Ok(RaftReplicaBootstrapPlan {
            shard_id: snapshot.shard_id,
            target_id,
            catch_up_from_index: snapshot.last_included_index.saturating_add(1),
            last_included_index: snapshot.last_included_index,
            transfer,
        })
    }

    pub fn build_install_snapshot_request_with_policy(
        &self,
        target_id: RaftNodeId,
        policy: RaftSnapshotTransferPolicy,
        external_snapshot_ref: Option<RaftExternalSnapshotRef>,
    ) -> Result<InstallSnapshotRequest, RaftError> {
        let snapshot = self.create_snapshot()?;
        let transfer = decide_snapshot_transfer(&snapshot, policy, external_snapshot_ref.clone())?;
        // The snapshot built above is the one that gets sent. Asking `create_snapshot` again here
        // built a SECOND state image -- a second corpus walk, a second index encode and a second
        // read of every slab -- and threw the first one away, having consulted only its size.
        let carried_ref = match transfer.mode {
            RaftSnapshotTransferMode::PeerStreaming => None,
            RaftSnapshotTransferMode::ExternalStore => transfer.external_snapshot_ref,
        };
        self.build_install_snapshot_request_from(target_id, snapshot, carried_ref)
    }

    pub fn receive_install_snapshot(
        &self,
        request: InstallSnapshotRequest,
    ) -> Result<InstallSnapshotResponse, RaftError> {
        {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            if request.shard_id != inner.shard_id {
                return Ok(InstallSnapshotResponse {
                    term: 0,
                    success: false,
                    last_included_index: 0,
                    reject_reason: Some("shard_mismatch".to_string()),
                });
            }
            let node = inner
                .nodes
                .get_mut(&request.target_id)
                .ok_or(RaftError::NodeNotFound(request.target_id))?;
            if request.term < node.current_term {
                let current_term = node.current_term;
                node.pipeline_state.snapshot_retry_count =
                    node.pipeline_state.snapshot_retry_count.saturating_add(1);
                node.pipeline_state.snapshot_install_rejected = node
                    .pipeline_state
                    .snapshot_install_rejected
                    .saturating_add(1);
                node.pipeline_state.snapshot_send_failed =
                    node.pipeline_state.snapshot_send_failed.saturating_add(1);
                let _ = node;
                inner.persist_configured_wal()?;
                return Ok(InstallSnapshotResponse {
                    term: current_term,
                    success: false,
                    last_included_index: 0,
                    reject_reason: Some("stale_term".to_string()),
                });
            }
            node.current_term = request.term;
            node.role = RaftRole::Follower;
            node.voted_for = None;
            node.pipeline_state.snapshot_install_started = node
                .pipeline_state
                .snapshot_install_started
                .saturating_add(1);
            node.pipeline_state.snapshot_installing = true;
            node.pipeline_state.snapshot_installed_index = request.snapshot.last_included_index;
            node.pipeline_state.snapshot_install_received_chunks = 0;
            node.pipeline_state.snapshot_install_total_chunks = 1;
        }
        // The request owns the snapshot and the snapshot owns the whole state image, so
        // hand it to the install by MOVE. Cloning it here made a second copy of the entire
        // shard, sized by the store rather than by anything about this request.
        let installed_index = request.snapshot.last_included_index;
        let result = self.install_snapshot(request.target_id, request.snapshot);
        {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            if let Some(node) = inner.nodes.get_mut(&request.target_id) {
                node.pipeline_state.snapshot_installing = false;
                node.pipeline_state.snapshot_sending = false;
                node.pipeline_state.snapshot_send_started_ms = None;
                node.pipeline_state.snapshot_send_elapsed_ms = 0;
                node.pipeline_state.snapshot_installed_index = installed_index;
                node.pipeline_state.snapshot_install_received_chunks = 1;
                node.pipeline_state.snapshot_install_total_chunks = 1;
                if result.is_ok() {
                    node.pipeline_state.snapshot_install_completed = node
                        .pipeline_state
                        .snapshot_install_completed
                        .saturating_add(1);
                    node.pipeline_state.snapshot_send_completed = node
                        .pipeline_state
                        .snapshot_send_completed
                        .saturating_add(1);
                } else {
                    node.pipeline_state.snapshot_install_rolled_back = node
                        .pipeline_state
                        .snapshot_install_rolled_back
                        .saturating_add(1);
                    node.pipeline_state.snapshot_install_progress_per_mille = 0;
                    node.pipeline_state.snapshot_send_failed =
                        node.pipeline_state.snapshot_send_failed.saturating_add(1);
                }
            }
            inner.persist_configured_wal()?;
        }
        let term = self
            .hard_state(request.target_id)
            .map(|state| state.current_term)
            .unwrap_or(request.term);
        match result {
            Ok(()) => Ok(InstallSnapshotResponse {
                term,
                success: true,
                last_included_index: installed_index,
                reject_reason: None,
            }),
            Err(err) => Ok(InstallSnapshotResponse {
                term,
                success: false,
                last_included_index: 0,
                reject_reason: Some(err.to_string()),
            }),
        }
    }

    pub fn finish_snapshot_send(
        &self,
        target_id: RaftNodeId,
        success: bool,
    ) -> Result<(), RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let node = inner
            .nodes
            .get_mut(&target_id)
            .ok_or(RaftError::NodeNotFound(target_id))?;
        node.pipeline_state.snapshot_sending = false;
        node.pipeline_state.snapshot_installing = false;
        node.pipeline_state.snapshot_send_started_ms = None;
        node.pipeline_state.snapshot_send_elapsed_ms = 0;
        if success {
            node.pipeline_state.snapshot_send_completed = node
                .pipeline_state
                .snapshot_send_completed
                .saturating_add(1);
        } else {
            node.pipeline_state.snapshot_send_failed =
                node.pipeline_state.snapshot_send_failed.saturating_add(1);
        }
        let _ = node;
        inner.persist_configured_wal()
    }

    pub fn build_install_snapshot_chunks(
        &self,
        target_id: RaftNodeId,
        max_entries_per_chunk: usize,
    ) -> Result<Vec<InstallSnapshotChunkRequest>, RaftError> {
        let mut snapshot = self.create_snapshot()?;
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .ok_or(RaftError::LeaderUnavailable)?;
        let leader_term = leader.current_term;
        let leader_id = inner.leader_id;
        let max_inflights_replicate = inner.config.max_inflights_replicate;
        let logical_time_ms = inner.logical_time_ms;
        let snapshot_during_membership_change = inner.joint_membership.is_some();
        let chunk_size = max_entries_per_chunk.max(1);
        let chunk_count = snapshot.entries.len().max(1).div_ceil(chunk_size);
        let snapshot_id = format!(
            "{}-{}-{}",
            snapshot.shard_id, snapshot.last_included_term, snapshot.last_included_index
        );
        {
            let target = inner
                .nodes
                .get_mut(&target_id)
                .ok_or(RaftError::NodeNotFound(target_id))?;
            if target.pipeline_state.snapshot_sending || target.pipeline_state.snapshot_installing {
                target.pipeline_state.snapshot_backpressure_rejections = target
                    .pipeline_state
                    .snapshot_backpressure_rejections
                    .saturating_add(1);
                target.pipeline_state.snapshot_send_failed =
                    target.pipeline_state.snapshot_send_failed.saturating_add(1);
                inner.persist_configured_wal()?;
                return Err(RaftError::SnapshotBackpressure { node_id: target_id });
            }
            target.pipeline_state.snapshot_sending = true;
            target.pipeline_state.snapshot_installing = true;
            target.pipeline_state.snapshot_send_started_ms = Some(logical_time_ms);
            target.pipeline_state.snapshot_send_elapsed_ms = 0;
            target.pipeline_state.snapshot_installed_index = snapshot.last_included_index;
            target.pipeline_state.snapshot_send_attempts = target
                .pipeline_state
                .snapshot_send_attempts
                .saturating_add(chunk_count as u64);
            target.pipeline_state.snapshot_install_received_chunks = 0;
            target.pipeline_state.snapshot_install_total_chunks = chunk_count as u64;
            target.pipeline_state.snapshot_install_progress_per_mille = 0;
            target.pipeline_state.snapshot_during_membership_change |=
                snapshot_during_membership_change;
            target.pipeline_state.snapshot_rejoin_after_compacted_log |=
                target.commit_index < snapshot.last_included_index;
            if chunk_count as u64 > max_inflights_replicate {
                target.pipeline_state.snapshot_backpressure_rejections = target
                    .pipeline_state
                    .snapshot_backpressure_rejections
                    .saturating_add(1);
                target.pipeline_state.snapshot_rate_limit_rejections = target
                    .pipeline_state
                    .snapshot_rate_limit_rejections
                    .saturating_add(1);
            }
        }
        inner.persist_configured_wal()?;
        let mut chunks = Vec::new();
        if snapshot.entries.is_empty() {
            // An image snapshot carries no entries, so this is the only chunk and it is the
            // only thing that will ever want the image. TAKE it: cloning here duplicated the
            // whole shard for the length of this call, on the send side of every install.
            let state_image = snapshot.state_image.take();
            chunks.push(InstallSnapshotChunkRequest {
                rpc: None,
                shard_id: snapshot.shard_id,
                term: leader_term,
                leader_id,
                target_id,
                snapshot_id,
                last_included_term: snapshot.last_included_term,
                last_included_index: snapshot.last_included_index,
                chunk_index: 0,
                chunk_count: 1,
                entries: Vec::new(),
                state_image,
            });
            return Ok(chunks);
        }
        for (chunk_index, entries) in snapshot.entries.chunks(chunk_size).enumerate() {
            chunks.push(InstallSnapshotChunkRequest {
                rpc: None,
                shard_id: snapshot.shard_id,
                term: leader_term,
                leader_id,
                target_id,
                snapshot_id: snapshot_id.clone(),
                last_included_term: snapshot.last_included_term,
                last_included_index: snapshot.last_included_index,
                chunk_index: chunk_index as u64,
                chunk_count: chunk_count as u64,
                entries: entries.to_vec(),
                state_image: None,
            });
        }
        Ok(chunks)
    }

    pub fn receive_install_snapshot_chunk(
        &self,
        mut request: InstallSnapshotChunkRequest,
    ) -> Result<InstallSnapshotChunkResponse, RaftError> {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        if request.shard_id != inner.shard_id {
            return Ok(InstallSnapshotChunkResponse {
                term: 0,
                success: false,
                snapshot_complete: false,
                received_chunks: 0,
                last_included_index: 0,
                reject_reason: Some("shard_mismatch".to_string()),
            });
        }
        if request.chunk_count == 0 || request.chunk_index >= request.chunk_count {
            return Err(RaftError::InvalidSnapshotChunk(
                "chunk index is outside chunk count".to_string(),
            ));
        }
        let node = inner
            .nodes
            .get_mut(&request.target_id)
            .ok_or(RaftError::NodeNotFound(request.target_id))?;
        if request.term < node.current_term {
            let current_term = node.current_term;
            node.pipeline_state.snapshot_retry_count =
                node.pipeline_state.snapshot_retry_count.saturating_add(1);
            node.pipeline_state.snapshot_install_rejected = node
                .pipeline_state
                .snapshot_install_rejected
                .saturating_add(1);
            node.pipeline_state.snapshot_send_failed =
                node.pipeline_state.snapshot_send_failed.saturating_add(1);
            let _ = node;
            inner.persist_configured_wal()?;
            return Ok(InstallSnapshotChunkResponse {
                term: current_term,
                success: false,
                snapshot_complete: false,
                received_chunks: 0,
                last_included_index: 0,
                reject_reason: Some("stale_term".to_string()),
            });
        }
        node.current_term = request.term;
        node.role = RaftRole::Follower;
        node.voted_for = None;
        if node.pipeline_state.snapshot_install_received_chunks == 0 {
            node.pipeline_state.snapshot_install_started = node
                .pipeline_state
                .snapshot_install_started
                .saturating_add(1);
        }
        node.pipeline_state.snapshot_installing = true;
        node.pipeline_state.snapshot_installed_index = request.last_included_index;
        node.pipeline_state.snapshot_install_total_chunks = request.chunk_count;
        let key = (request.target_id, request.snapshot_id.clone());
        let pending = inner
            .pending_snapshots
            .entry(key.clone())
            .or_insert_with(|| PendingSnapshotChunks {
                shard_id: request.shard_id,
                last_included_term: request.last_included_term,
                last_included_index: request.last_included_index,
                chunks: vec![None; request.chunk_count as usize],
                state_image: None,
            });
        if pending.shard_id != request.shard_id
            || pending.last_included_term != request.last_included_term
            || pending.last_included_index != request.last_included_index
            || pending.chunks.len() != request.chunk_count as usize
        {
            if let Some(node) = inner.nodes.get_mut(&request.target_id) {
                node.pipeline_state.snapshot_retry_count =
                    node.pipeline_state.snapshot_retry_count.saturating_add(1);
                node.pipeline_state.snapshot_install_rejected = node
                    .pipeline_state
                    .snapshot_install_rejected
                    .saturating_add(1);
                node.pipeline_state.snapshot_install_rolled_back = node
                    .pipeline_state
                    .snapshot_install_rolled_back
                    .saturating_add(1);
                node.pipeline_state.snapshot_send_failed =
                    node.pipeline_state.snapshot_send_failed.saturating_add(1);
            }
            inner.pending_snapshots.remove(&key);
            inner.persist_configured_wal()?;
            return Err(RaftError::InvalidSnapshotChunk(
                "chunk metadata changed within snapshot".to_string(),
            ));
        }
        let duplicate_chunk = pending.chunks[request.chunk_index as usize].is_some();
        // S2: the state image rides on chunk 0; retain it for the reassembled snapshot.
        if request.state_image.is_some() {
            // Nothing reads the image off the request after this, so move it into the
            // pending buffer rather than copying the whole shard a second time -- and this
            // copy ran while the cluster write lock was held.
            pending.state_image = request.state_image.take();
        }
        pending.chunks[request.chunk_index as usize] = Some(request.entries);
        let received_chunks = pending
            .chunks
            .iter()
            .filter(|chunk| chunk.is_some())
            .count() as u64;
        if let Some(node) = inner.nodes.get_mut(&request.target_id) {
            if duplicate_chunk {
                node.pipeline_state.snapshot_chunk_retry_count = node
                    .pipeline_state
                    .snapshot_chunk_retry_count
                    .saturating_add(1);
                node.pipeline_state.snapshot_retry_count =
                    node.pipeline_state.snapshot_retry_count.saturating_add(1);
            }
            node.pipeline_state.snapshot_install_received_chunks = received_chunks;
            node.pipeline_state.snapshot_install_total_chunks = request.chunk_count;
            node.pipeline_state.snapshot_install_progress_per_mille =
                received_chunks.saturating_mul(1_000) / request.chunk_count.max(1);
        }
        if received_chunks < request.chunk_count {
            let term = inner
                .nodes
                .get(&request.target_id)
                .map(|node| node.current_term)
                .unwrap_or(request.term);
            inner.persist_configured_wal()?;
            return Ok(InstallSnapshotChunkResponse {
                term,
                success: true,
                snapshot_complete: false,
                received_chunks,
                last_included_index: 0,
                reject_reason: None,
            });
        }

        let pending = inner
            .pending_snapshots
            .remove(&key)
            .expect("complete pending snapshot must exist");
        let state_image = pending.state_image;
        let entries = pending
            .chunks
            .into_iter()
            .flat_map(|chunk| chunk.unwrap_or_default())
            .collect::<Vec<_>>();
        drop(inner);
        let install_result = self.install_snapshot(
            request.target_id,
            RaftSnapshot {
                shard_id: request.shard_id,
                last_included_term: request.last_included_term,
                last_included_index: request.last_included_index,
                external_snapshot_ref: None,
                entries,
                state_image,
                state_image_externalized: false,
            },
        );
        {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            if let Some(node) = inner.nodes.get_mut(&request.target_id) {
                node.pipeline_state.snapshot_installing = false;
                node.pipeline_state.snapshot_sending = false;
                node.pipeline_state.snapshot_installed_index = request.last_included_index;
                node.pipeline_state.snapshot_install_received_chunks = received_chunks;
                node.pipeline_state.snapshot_install_total_chunks = request.chunk_count;
                node.pipeline_state.snapshot_install_progress_per_mille =
                    received_chunks.saturating_mul(1_000) / request.chunk_count.max(1);
                if install_result.is_ok() {
                    node.pipeline_state.snapshot_install_completed = node
                        .pipeline_state
                        .snapshot_install_completed
                        .saturating_add(1);
                    node.pipeline_state.snapshot_send_completed = node
                        .pipeline_state
                        .snapshot_send_completed
                        .saturating_add(1);
                } else {
                    node.pipeline_state.snapshot_install_rolled_back = node
                        .pipeline_state
                        .snapshot_install_rolled_back
                        .saturating_add(1);
                    node.pipeline_state.snapshot_send_failed =
                        node.pipeline_state.snapshot_send_failed.saturating_add(1);
                }
            }
            inner.persist_configured_wal()?;
        }
        install_result?;
        let term = self
            .hard_state(request.target_id)
            .map(|state| state.current_term)
            .unwrap_or(request.term);
        Ok(InstallSnapshotChunkResponse {
            term,
            success: true,
            snapshot_complete: true,
            received_chunks,
            last_included_index: request.last_included_index,
            reject_reason: None,
        })
    }

    pub fn create_snapshot(&self) -> Result<RaftSnapshot, RaftError> {
        // S2 (default ON): snapshot the engine's STATE IMAGE at the leader's applied index
        // instead of the committed entry history, so a far-behind follower installs in
        // O(state). The build reads the whole served index and every slab, which takes long
        // enough on a real store to matter -- and it used to run holding the cluster read
        // lock, which every apply needs the write half of. On the live cluster that surfaced
        // as proposes stalling for the length of each image build. The build now runs off the
        // lock and proves consistency by watermark instead; any failure there falls through
        // to the classic entry-carrying snapshot below, so the image path can never make
        // snapshotting worse.
        if self
            .inner
            .read()
            .expect("raft cluster lock poisoned")
            .snapshot_state_image
        {
            if let Some(snapshot) = self.create_state_image_snapshot()? {
                return Ok(snapshot);
            }
        }
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .filter(|node| node.alive && node.role == RaftRole::Leader)
            .ok_or(RaftError::LeaderUnavailable)?;
        let mut entries_by_index = BTreeMap::new();
        if let Some(snapshot) = &leader.installed_snapshot {
            for entry in snapshot
                .entries
                .iter()
                .filter(|entry| entry.index <= leader.commit_index)
            {
                entries_by_index.insert(entry.index, entry.clone());
            }
        }
        for entry in leader
            .log
            .iter()
            .filter(|entry| entry.index <= leader.commit_index)
        {
            entries_by_index.insert(entry.index, entry.clone());
        }
        let entries = entries_by_index.into_values().collect::<Vec<_>>();
        let last_included_term = entries
            .last()
            .map(|entry| entry.term)
            .unwrap_or(leader.current_term);
        Ok(RaftSnapshot {
            shard_id: inner.shard_id,
            last_included_term,
            last_included_index: leader.commit_index,
            external_snapshot_ref: None,
            entries,
            state_image: None,
            state_image_externalized: false,
        })
    }

    /// Build the S2 state-image snapshot WITHOUT holding the cluster lock across the image
    /// build. Applies advance `applied_index` only under the cluster write lock, so an applied
    /// index that reads the same before and after the build proves the engine was quiescent in
    /// between -- the image is exact at that watermark. A cluster that keeps applying gets
    /// three such attempts; the final attempt builds under the read lock, which is consistent
    /// by construction and no less available than the always-locked build this replaces.
    ///
    /// `Ok(None)` means "no image snapshot here" -- nothing applied yet, or the engine could
    /// not serve some part of the image -- and the caller falls through to the entry-carrying
    /// form.
    fn create_state_image_snapshot(&self) -> Result<Option<RaftSnapshot>, RaftError> {
        for _ in 0..3 {
            let (shard_id, watermark, engine) = {
                let inner = self.inner.read().expect("raft cluster lock poisoned");
                let leader = inner
                    .nodes
                    .get(&inner.leader_id)
                    .filter(|node| node.alive && node.role == RaftRole::Leader)
                    .ok_or(RaftError::LeaderUnavailable)?;
                if leader.applied_index == 0 {
                    return Ok(None);
                }
                (inner.shard_id, leader.applied_index, leader.engine.clone())
            };
            let Some(image) = build_state_image(&engine, shard_id) else {
                return Ok(None);
            };
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            let leader = inner
                .nodes
                .get(&inner.leader_id)
                .filter(|node| node.alive && node.role == RaftRole::Leader)
                .ok_or(RaftError::LeaderUnavailable)?;
            if leader.applied_index == watermark {
                return Ok(Some(state_image_snapshot_at(
                    inner.shard_id,
                    leader,
                    watermark,
                    image,
                )));
            }
            // Something applied while the build ran, so the image may straddle two states.
            // Drop it and capture again from the newer watermark.
        }
        // The cluster would not go quiescent for three builds; take the last one under the
        // read lock, exactly as the original path always did.
        let inner = self.inner.read().expect("raft cluster lock poisoned");
        let leader = inner
            .nodes
            .get(&inner.leader_id)
            .filter(|node| node.alive && node.role == RaftRole::Leader)
            .ok_or(RaftError::LeaderUnavailable)?;
        if leader.applied_index == 0 {
            return Ok(None);
        }
        Ok(build_state_image(&leader.engine, inner.shard_id).map(|image| {
            state_image_snapshot_at(inner.shard_id, leader, leader.applied_index, image)
        }))
    }

    /// Test hook: how many indexes a node's applied SET holds, and whether one specific index
    /// is in it. The set and the `applied_index` scalar are filled separately, so a test that
    /// reads only the scalar cannot see the set fall short of it.
    #[cfg(test)]
    pub(crate) fn applied_set_len_for_test(&self, node_id: RaftNodeId) -> usize {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .nodes
            .get(&node_id)
            .map(|node| node.applied.len())
            .unwrap_or(0)
    }

    #[cfg(test)]
    pub(crate) fn applied_set_contains_for_test(&self, node_id: RaftNodeId, index: u64) -> bool {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .nodes
            .get(&node_id)
            .map(|node| node.applied.contains(&index))
            .unwrap_or(false)
    }

    /// Test hook: the engine a node serves from, so a test can ask what shards it hosts.
    #[cfg(test)]
    pub(crate) fn node_engine_for_test(&self, node_id: RaftNodeId) -> Option<TemporalEngine> {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .nodes
            .get(&node_id)
            .map(|node| node.engine.clone())
    }

    /// Test hook: retune the compaction threshold on a live cluster.
    #[cfg(test)]
    pub(crate) fn set_max_applied_log_bytes_for_test(&self, bytes: u64) {
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        inner.config.max_applied_log_bytes = bytes;
    }

    /// Entries this node is holding IN MEMORY. The log is trimmed only when a snapshot is
    /// installed, so this is what a node's residency actually costs.
    pub fn in_memory_log_entries(&self, node_id: RaftNodeId) -> usize {
        self.inner
            .read()
            .expect("raft cluster lock poisoned")
            .nodes
            .get(&node_id)
            .map(|node| node.log.len())
            .unwrap_or(0)
    }

    /// Compact the log THIS process's own node is holding, when that node is a follower.
    ///
    /// The leader's compaction trims the leader. In-process that trims everyone, because one
    /// process owns every node -- but a deployed follower is a separate process that learns
    /// only by RPC, and a follower that stays CAUGHT UP never falls behind the leader's
    /// retained range, so it is never sent a snapshot and nothing ever trims it. Its log then
    /// grows with the corpus instead of with the state, in memory and in its own WAL record
    /// alike. Measured: a caught-up follower held every one of 400 entries.
    ///
    /// A follower compacts to its APPLIED index and no further: those entries are committed and
    /// already in its state machine, so nothing that could still be truncated by a conflict is
    /// discarded. The snapshot it installs carries the marker that keeps `prev_log_index` /
    /// `prev_log_term` answerable across the trim, exactly as a leader-sent snapshot would.
    fn maybe_compact_local_follower(&self) -> Result<Option<RaftSnapshotTriggerReport>, RaftError> {
        let (node_id, shard_id, watermark, engine, applied_log_bytes, limit, report_base) = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            let Some(node_id) = inner.local_node_id else {
                // The in-process model: the leader's compaction already trims every node.
                return Ok(None);
            };
            if node_id == inner.leader_id {
                return Ok(None);
            }
            if !inner.config.can_trigger_snapshot {
                return Ok(None);
            }
            let Some(node) = inner.nodes.get(&node_id) else {
                return Ok(None);
            };
            let last_snapshot_index = node
                .installed_snapshot
                .as_ref()
                .map(|snapshot| snapshot.last_included_index)
                .unwrap_or_default();
            if node.applied_index <= last_snapshot_index {
                return Ok(None);
            }
            let logical = raft_log_bytes_after(&node.log, last_snapshot_index);
            let applied_log_bytes = inner
                .wal
                .as_ref()
                .map(|wal| wal.node_log_bytes_after(inner.shard_id, node_id, last_snapshot_index))
                .unwrap_or(0)
                .max(logical);
            // The same bound, for the same reason, on the copy of this cadence a deployed
            // follower runs. A follower's compaction builds its own state image off its own
            // engine, so it pays the identical whole-shard read for the identical constant
            // threshold; a bound applied to only one of two live copies leaves the other with
            // the defect.
            let limit = effective_max_applied_log_bytes(
                inner.config.max_applied_log_bytes,
                installed_image_bytes(node),
                inner.config.snapshot_image_fraction_divisor,
            );
            if applied_log_bytes < limit {
                return Ok(None);
            }
            (
                node_id,
                inner.shard_id,
                node.applied_index,
                node.engine.clone(),
                applied_log_bytes,
                limit,
                (inner.leader_id, node.applied_index, last_snapshot_index),
            )
        };

        // Built off the cluster lock, then confirmed by watermark -- the same discipline the
        // leader's image build uses.
        let Some(image) = build_state_image(&engine, shard_id) else {
            return Ok(None);
        };
        // And the engine this compaction publishes is rebuilt here too, still holding nothing.
        // It is a pure function of the snapshot, and the snapshot is fully determined by the
        // image and the watermark -- both already in hand -- so nothing about it needs the guard.
        // The watermark re-check below is unchanged and still decides everything; all that moves
        // is where the rebuild happens. Worth one whole engine per compaction tick on every
        // deployed follower: 1 of 1 rebuilds and 140 slab bytes per record, all of it previously
        // inside the write guard.
        let mut snapshot = RaftSnapshot {
            shard_id,
            // Filled under the guard below, from the log as it stands once the watermark has been
            // re-checked. The rebuild does not read it, so it costs nothing to leave until then,
            // and leaving it until then is what keeps this identical to the always-locked form.
            last_included_term: 0,
            last_included_index: watermark,
            external_snapshot_ref: None,
            entries: Vec::new(),
            state_image: Some(image),
            state_image_externalized: false,
        };
        #[cfg(test)]
        self.perform_armed_follower_window_interleaving(node_id, watermark);
        let rebuilt = rebuild_snapshot_engine(&snapshot);
        let mut inner = self.inner.write().expect("raft cluster lock poisoned");
        #[cfg(test)]
        let _guard_mark = crate::snapshot_probe::GuardMark::new();
        let Some(node) = inner.nodes.get_mut(&node_id) else {
            return Ok(None);
        };
        if node.applied_index != watermark {
            // Something applied while the image was building; the next tick tries again. The
            // engine rebuilt above is dropped with it -- the safe-but-wasteful direction, and the
            // only one this check can fail in.
            return Ok(None);
        }
        snapshot.last_included_term =
            node_term_at_log_or_snapshot_index(node, watermark).unwrap_or(node.current_term);
        publish_snapshot_state(node, snapshot, rebuilt);
        inner.persist_configured_wal()?;
        Ok(Some(RaftSnapshotTriggerReport {
            triggered: true,
            reason: "follower_applied_log_bytes_threshold".to_string(),
            leader_id: report_base.0,
            applied_index: report_base.1,
            last_snapshot_index: report_base.2,
            applied_log_bytes,
            max_applied_log_bytes: limit,
        }))
    }

    pub fn maybe_trigger_snapshot(&self) -> Result<RaftSnapshotTriggerReport, RaftError> {
        // A deployed follower compacts its own log; only the leader runs the check below.
        if let Some(report) = self.maybe_compact_local_follower()? {
            return Ok(report);
        }
        let (should_trigger, report) = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            let leader = inner
                .nodes
                .get(&inner.leader_id)
                .filter(|node| node.alive && node.role == RaftRole::Leader)
                .ok_or(RaftError::LeaderUnavailable)?;
            let last_snapshot_index = leader
                .installed_snapshot
                .as_ref()
                .map(|snapshot| snapshot.last_included_index)
                .unwrap_or_default();
            // What the threshold is meant to bound is the log ON DISK, so judge it by that
            // when this node keeps one. The logical command bytes understate the footprint by
            // the entire encoding overhead -- measured at 7x on a 30k-write corpus, which is
            // the difference between an 8 MB bound firing at 8 MB and firing at 56 MB. The
            // in-process model (no WAL) keeps the logical measure, which is all it has.
            let logical_log_bytes = raft_log_bytes_after(&leader.log, last_snapshot_index);
            let applied_log_bytes = inner
                .local_node_id
                .and_then(|node_id| {
                    inner
                        .wal
                        .as_ref()
                        .map(|wal| wal.node_log_bytes_after(inner.shard_id, node_id, last_snapshot_index))
                })
                .unwrap_or(0)
                .max(logical_log_bytes);
            // What this snapshot will READ is the whole shard; what it discards is the log
            // above. A constant threshold relates the two not at all, so raise it in
            // proportion to the image the LAST snapshot here produced -- a length already in
            // hand, never a build -- and a snapshot never reads more than
            // `SNAPSHOT_IMAGE_FRACTION_DIVISOR` bytes of image per byte of log it discards.
            // The configured value stays a FLOOR: a leader with no installed image reports 0
            // and the floor decides alone, which is why the first snapshot on a fresh shard is
            // untouched and only the routine ones after it are paced.
            let last_image_bytes = installed_image_bytes(leader);
            let effective_limit = effective_max_applied_log_bytes(
                inner.config.max_applied_log_bytes,
                last_image_bytes,
                inner.config.snapshot_image_fraction_divisor,
            );
            let mut report = RaftSnapshotTriggerReport {
                triggered: false,
                reason: "below_threshold".to_string(),
                leader_id: inner.leader_id,
                applied_index: leader.applied_index,
                last_snapshot_index,
                applied_log_bytes,
                max_applied_log_bytes: effective_limit,
            };
            if !inner.config.can_trigger_snapshot {
                report.reason = "disabled".to_string();
                return Ok(report);
            }
            if leader.applied_index <= last_snapshot_index {
                report.reason = "no_new_applied_logs".to_string();
                return Ok(report);
            }
            if applied_log_bytes < effective_limit {
                if effective_limit > inner.config.max_applied_log_bytes {
                    report.reason = "below_threshold_raised_for_the_image_it_would_read"
                        .to_string();
                }
                return Ok(report);
            }
            // Hold while a live follower still needs what this would discard, so catching it up
            // stays a matter of sending entries rather than installing a snapshot -- but only up
            // to a ceiling, past which a snapshot is the cheaper path anyway and an absent peer
            // must not pin the log open.
            //
            // Only in a deployed process, which is the one that tracks peer progress by what
            // peers acknowledge. The in-process cluster maintains that differently, and holding
            // on it would stop compaction happening at all.
            let ceiling = inner.config.max_retained_log_bytes;
            if ceiling > 0 && applied_log_bytes < ceiling {
                if let Some(local) = inner.local_node_id {
                    let behind = inner
                        .nodes
                        .values()
                        .filter(|node| {
                            node.id != local
                                && node.alive
                                && node.replica_role.participates_in_quorum()
                        })
                        .any(|node| node.pipeline_state.match_index < leader.applied_index);
                    if behind {
                        report.reason = "held_for_a_follower_still_catching_up".to_string();
                        return Ok(report);
                    }
                }
            }
            report.triggered = true;
            report.reason = "applied_log_bytes_threshold".to_string();
            (true, report)
        };

        if should_trigger {
            let snapshot = self.create_snapshot()?;

            // The engines are built HERE, holding NOTHING.
            //
            // An install is two halves. The expensive half rebuilds a whole engine from the image
            // -- every slab written again, the served index read back in, 140 bytes per record per
            // engine -- and it reads nothing but the snapshot. The cheap half moves one node's
            // bookkeeping onto that engine. Only the cheap half has to be atomic with the check
            // that says the node may take the snapshot, so only the cheap half needs the cluster
            // write guard, which is the half of the lock every `propose` needs. Before this, both
            // halves ran under it: three whole engine rebuilds and 420,000 slab bytes at 1,000
            // records, 1,680,000 at 4,000 -- 100% of the install, at both sizes.
            //
            // The targets are chosen twice. This first pass only sizes the build: it decides how
            // many engines to make and for whom, and it is allowed to be wrong.
            let provisional = {
                let inner = self.inner.read().expect("raft cluster lock poisoned");
                install_targets(&inner, snapshot.last_included_index)
            };
            #[cfg(test)]
            self.perform_armed_install_window_interleaving();
            let mut prebuilt = provisional
                .iter()
                .map(|node_id| (*node_id, rebuild_snapshot_engine(&snapshot)))
                .collect::<Vec<_>>();
            // The per-node COPY of the image is a pure function of the snapshot for exactly the
            // same reason the engine is, so it is made here too. One fewer than the targets: the
            // last install takes the original, which is dropped when the loop ends anyway.
            let mut spare_images = (1..provisional.len())
                .map(|_| snapshot.clone())
                .collect::<Vec<_>>();

            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            #[cfg(test)]
            let _guard_mark = crate::snapshot_probe::GuardMark::new();
            // And this second pass is the authoritative one, read under the guard the publish
            // happens under, so nothing it saw can have changed by the time it is acted on. Both
            // things it reads can move while the engines build: a node's `commit_index` and
            // `applied_index` advance with every apply, and `alive` is flipped by `set_alive`,
            // which takes this same write guard. So the set is recomputed rather than carried.
            //
            // The direction that matters is one-way. A node that has DROPPED out -- it applied
            // past the snapshot while the engine was building -- must not be published into:
            // `publish_snapshot_state` sets `applied_index` back to the snapshot's index rather
            // than raising it to it, so it would serve a shard that had gone backwards until the
            // retained entries were applied again. A node that has newly JOINED the set -- it was
            // dead and was revived -- has no engine waiting for it, and is simply built for here,
            // under the guard. That arm keeps the installed set exactly what it would have been
            // without any of this, which is worth more than the rebuild it costs; it is priced and
            // forced in `a_node_revived_while_the_engines_build_is_still_installed_into`.
            //
            // Whatever is left over -- an engine built for a node that has since dropped out, a
            // spare copy of the image -- is NOT discarded here. `prebuilt` and `spare_images` are
            // declared above `inner`, so they are dropped after it: freeing a whole engine is
            // itself work, and it happens once the guard has been released.
            let targets = install_targets(&inner, snapshot.last_included_index);
            let mut carried = Some(snapshot);
            let mut targets = targets.into_iter().peekable();
            while let Some(node_id) = targets.next() {
                let is_last = targets.peek().is_none();
                // Both halves of what this node needs come off the guard where one was built for
                // it. The `None` arms are the newly-qualified node -- nothing was built for it,
                // and it is built for here, under the guard, so that WHICH nodes install is the
                // same set it has always been.
                let engine = match prebuilt.iter().position(|(id, _)| *id == node_id) {
                    Some(position) => prebuilt.swap_remove(position).1,
                    None => rebuild_snapshot_engine(carried.as_ref().expect("held below")),
                };
                let Some(held) = carried.as_ref() else {
                    break;
                };
                let for_this_node = if is_last {
                    carried.take().expect("held above")
                } else {
                    spare_images.pop().unwrap_or_else(|| held.clone())
                };
                let Some(node) = inner.nodes.get_mut(&node_id) else {
                    continue;
                };
                publish_snapshot_state(node, for_this_node, engine);
            }
            inner.persist_configured_wal()?;
        }
        Ok(report)
    }

    /// Perform whatever interleaving a test armed for the window in which the install loop holds
    /// no cluster guard. Compiled out of everything but the test build.
    #[cfg(test)]
    fn perform_armed_install_window_interleaving(&self) {
        match crate::snapshot_probe::take_install_window() {
            None => {}
            Some(crate::snapshot_probe::InstallWindow::ApplyOneMoreEntry) => {
                let _ = self.propose(Command::StringSet {
                    key: "an-entry-applied-while-the-engines-build".to_string(),
                    value: vec![b'z'; 16],
                });
            }
            Some(crate::snapshot_probe::InstallWindow::ReviveNode(node_id)) => {
                let _ = self.set_alive(node_id, true);
            }
        }
    }

    /// The same, for the window a deployed FOLLOWER's own compaction holds nothing across. A
    /// follower does not propose -- it learns by RPC -- so the interleaving is delivered as one
    /// more append, committed, which is what advances its applied index past the watermark.
    #[cfg(test)]
    fn perform_armed_follower_window_interleaving(&self, node_id: RaftNodeId, watermark: u64) {
        let Some(crate::snapshot_probe::InstallWindow::ApplyOneMoreEntry) =
            crate::snapshot_probe::take_install_window()
        else {
            return;
        };
        let (term, shard_id, leader_id) = {
            let inner = self.inner.read().expect("raft cluster lock poisoned");
            let Some(node) = inner.nodes.get(&node_id) else {
                return;
            };
            (node.current_term, inner.shard_id, inner.leader_id)
        };
        let _ = self.receive_append_entries(AppendEntriesRequest {
            rpc: None,
            shard_id,
            term,
            leader_id,
            target_id: node_id,
            prev_log_index: watermark,
            prev_log_term: term,
            entries: vec![RaftLogEntry {
                leader_time_ms: 0,
                term,
                index: watermark + 1,
                shard_id,
                command: Command::StringSet {
                    key: "an-entry-applied-while-the-follower-engine-builds".to_string(),
                    value: vec![b'z'; 16],
                },
            }],
            leader_commit: watermark + 1,
        });
    }

    pub async fn publish_leader_snapshot_to_store<O>(
        &self,
        snapshot_store: &S3SnapshotStore<O>,
    ) -> Result<RaftSnapshotPublishReport, RaftError>
    where
        O: ObjectStore + 'static,
    {
        let snapshot = self.create_snapshot()?;
        let snapshot_bytes = serde_json::to_vec(&snapshot)
            .map_err(|err| RaftError::SnapshotEncoding(err.to_string()))?;
        let last_log_id = format!(
            "term:{}:index:{}",
            snapshot.last_included_term, snapshot.last_included_index
        );
        let local_snapshot = snapshot_store
            .create_local_snapshot_with_index_bytes(
                snapshot.shard_id,
                last_log_id,
                Bytes::from(snapshot_bytes),
            )
            .await
            .map_err(|err| RaftError::SnapshotStore(err.to_string()))?;
        let snapshot_ref = snapshot_store
            .upload_snapshot(local_snapshot)
            .await
            .map_err(|err| RaftError::SnapshotStore(err.to_string()))?;
        let raft_ref = raft_external_ref_from_snapshot_ref(&snapshot_ref);
        {
            let mut inner = self.inner.write().expect("raft cluster lock poisoned");
            inner.latest_external_snapshot_ref = Some(raft_ref.clone());
            inner.persist_configured_wal()?;
        }
        Ok(RaftSnapshotPublishReport {
            shard_id: snapshot_ref.shard_id,
            last_log_index: snapshot_ref.last_log_index,
            raft_ref,
            meta_ref: shard_snapshot_ref_from_snapshot_ref(&snapshot_ref),
        })
    }

    pub async fn publish_leader_snapshot_and_record_meta<O>(
        &self,
        snapshot_store: &S3SnapshotStore<O>,
        meta: &SingleNodeMeta,
    ) -> Result<RaftSnapshotPublishReport, RaftError>
    where
        O: ObjectStore + 'static,
    {
        let report = self
            .publish_leader_snapshot_to_store(snapshot_store)
            .await?;
        let ack = meta.publish_shard_snapshot(PublishShardSnapshotRequest {
            shard_id: report.shard_id,
            snapshot: report.meta_ref.clone(),
        });
        if ack.status.ok {
            Ok(report)
        } else {
            Err(RaftError::SnapshotStore(format!(
                "metaserver rejected snapshot ref: {}",
                ack.status.code
            )))
        }
    }

    pub async fn bootstrap_replica_from_external_snapshot<O>(
        &self,
        target_id: RaftNodeId,
        snapshot_store: &S3SnapshotStore<O>,
        snapshot_ref: &ShardSnapshotRef,
        destination: PathBuf,
    ) -> Result<RaftReplicaBootstrapPlan, RaftError>
    where
        O: ObjectStore + 'static,
    {
        let local_commit_index = self
            .hard_state(target_id)
            .map(|state| state.commit_index)
            .unwrap_or_default();
        if snapshot_ref.last_log_index < local_commit_index {
            return Err(RaftError::StaleSnapshot {
                snapshot_index: snapshot_ref.last_log_index,
                local_commit_index,
            });
        }
        let local_snapshot = snapshot_store
            .download_snapshot_by_uri(&snapshot_ref.uri, destination)
            .await
            .map_err(|err| RaftError::SnapshotStore(err.to_string()))?;
        let snapshot_bytes = tokio::fs::read(&local_snapshot.index_path)
            .await
            .map_err(|err| RaftError::SnapshotStore(err.to_string()))?;
        validate_downloaded_snapshot_ref(
            self.shard_id(),
            snapshot_ref,
            &local_snapshot.manifest,
            &snapshot_bytes,
        )?;
        let mut snapshot = serde_json::from_slice::<RaftSnapshot>(&snapshot_bytes)
            .map_err(|err| RaftError::SnapshotEncoding(err.to_string()))?;
        snapshot.external_snapshot_ref = Some(RaftExternalSnapshotRef {
            uri: snapshot_ref.uri.clone(),
            checksum: snapshot_ref.checksum.clone(),
            byte_size: snapshot_ref.byte_size,
        });
        // Only two scalars are wanted after the install, so keep those and MOVE the
        // snapshot rather than copying a downloaded whole-shard image to read them.
        let snapshot_shard_id = snapshot.shard_id;
        let last_included_index = snapshot.last_included_index;
        self.install_snapshot(target_id, snapshot)?;
