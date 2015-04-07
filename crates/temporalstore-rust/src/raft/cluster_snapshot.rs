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
