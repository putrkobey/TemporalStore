// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::control::MembershipUpdateRequest;
use crate::raft::RaftNodeId;
use crate::types::{ShardId, Status};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardRole {
    Primary,
    Secondary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ShardReplicaState {
    Creating,
    Loading,
    Normal,
    Freezing,
    Frozen,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardReplica {
    pub shard_id: ShardId,
    pub replica_id: u64,
    pub node_id: RaftNodeId,
    pub role: ShardRole,
    pub state: ShardReplicaState,
    pub load_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardMovePlan {
    pub shard_id: ShardId,
    pub source_replica_id: u64,
    pub source_node_id: RaftNodeId,
    pub target_replica_id: u64,
    pub target_node_id: RaftNodeId,
    pub load_version: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RebalanceStep {
    LoadTarget {
        shard_id: ShardId,
        replica_id: u64,
        node_id: RaftNodeId,
        load_version: u64,
    },
    ReloadTarget {
        shard_id: ShardId,
        replica_id: u64,
        node_id: RaftNodeId,
        load_version: u64,
    },
    UpdateMembership {
        shard_id: ShardId,
        active_replica_ids: Vec<u64>,
        primary_replica_id: u64,
        membership_version: u64,
    },
    FreezeSource {
        shard_id: ShardId,
        replica_id: u64,
        node_id: RaftNodeId,
    },
    UnloadSource {
        shard_id: ShardId,
        replica_id: u64,
        node_id: RaftNodeId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipUpdateTaskOptions {
    pub exclude_self: bool,
    pub success_threshold: usize,
    pub submit_fsm: bool,
}

impl Default for MembershipUpdateTaskOptions {
    fn default() -> Self {
        Self {
            exclude_self: true,
            success_threshold: 0,
            submit_fsm: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembershipUpdatePeerStatus {
    Ok,
    NotFound,
    Failed { code: String, message: String },
}

impl MembershipUpdatePeerStatus {
    pub fn from_status(status: Status) -> Self {
        if status.ok {
            Self::Ok
        } else if status.code == "not_found" {
            Self::NotFound
        } else {
            Self::Failed {
                code: status.code,
                message: status.message,
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipUpdatePeerRequest {
    pub replica_id: u64,
    pub node_id: RaftNodeId,
    pub request: MembershipUpdateRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipUpdateTaskPlan {
    pub shard_id: ShardId,
    pub self_replica_id: u64,
    pub active_replica_ids: Vec<u64>,
    pub primary_replica_id: u64,
    pub membership_version: u64,
    pub requests: Vec<MembershipUpdatePeerRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipUpdateTaskReport {
    pub plan: MembershipUpdateTaskPlan,
    pub success_count: usize,
    pub not_found_count: usize,
    pub failed_count: usize,
    pub success_threshold: usize,
    pub accepted: bool,
    pub should_submit_fsm: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchedulerTaskResult {
    Ok,
    RetryLater,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchedulerTaskKind {
    UpdateMembership(MembershipUpdateTaskPlan),
    RebalanceStep(RebalanceStep),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerTask {
    pub id: u64,
    pub priority: i32,
    pub create_time_ms: u64,
    pub next_run_time_ms: u64,
    pub last_run_time_ms: u64,
    pub retry_times: u64,
    pub kind: SchedulerTaskKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerLifecycleToken {
    pub task_id: u64,
    pub shard_id: ShardId,
    pub operation: String,
    pub load_version: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSchedulerOptions {
    pub base_postpone_ms: u64,
    pub max_postpone_ms: u64,
    pub max_retry_times: u64,
    pub max_inflight: usize,
}

impl Default for TaskSchedulerOptions {
    fn default() -> Self {
        Self {
            base_postpone_ms: 1_000,
            max_postpone_ms: 60_000,
            max_retry_times: 10,
            max_inflight: 1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchedulerRunReport {
    pub task_id: u64,
    pub result: SchedulerTaskResult,
    pub retry_times: u64,
    pub next_run_time_ms: Option<u64>,
    pub queue_len: usize,
    pub aborted: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeterministicTaskScheduler {
    queue: BTreeMap<u64, SchedulerTask>,
    next_task_id: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSchedulerSnapshot {
    pub tasks: Vec<SchedulerTask>,
    pub next_task_id: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionSetMember {
    pub replica_id: u64,
    pub node_id: RaftNodeId,
    pub role: ShardRole,
    pub state: ShardReplicaState,
    pub load_version: u64,
    pub membership_version: u64,
    pub server_addr: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PartitionSetTopology {
    pub namespace: String,
    pub table_name: String,
    pub table_id: u64,
    pub partition_set_id: u64,
    pub shard_id: ShardId,
    pub topology_version: u64,
    pub membership_version: u64,
    pub unit_version: u64,
    pub primary_replica_id: u64,
    pub active_replica_ids: Vec<u64>,
    pub frozen_replica_ids: Vec<u64>,
    pub members: Vec<PartitionSetMember>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkSchedulerTaskExecution {
    pub task_id: u64,
    pub target_node_id: RaftNodeId,
    pub target_addr: String,
    pub result: SchedulerTaskResult,
    pub retry_times: u64,
    pub next_run_time_ms: Option<u64>,
    pub status: Status,
    #[serde(default)]
    pub lifecycle_token: Option<SchedulerLifecycleToken>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RaftPersistedSchedulerState {
    pub scheduler: TaskSchedulerSnapshot,
    pub topology: PartitionSetTopology,
    pub executions: Vec<NetworkSchedulerTaskExecution>,
    pub raft_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebalanceOptions {
    pub max_moves_per_round: usize,
    pub partition_count_safe_gap: usize,
}

impl Default for RebalanceOptions {
    fn default() -> Self {
        Self {
            max_moves_per_round: 10,
            partition_count_safe_gap: 0,
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebalanceRoundReport {
    pub plans: Vec<ShardMovePlan>,
    pub balance_partition_count: BTreeMap<RaftNodeId, usize>,
    pub total_normal: usize,
    pub safe_line: usize,
    pub placement_fail_count: usize,
    pub placement_fail_reasons: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RebalanceController {
    replicas: BTreeMap<u64, ShardReplica>,
    membership_version: u64,
    next_replica_id: u64,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum RebalanceError {
    #[error("replica not found: {0}")]
    ReplicaNotFound(u64),
    #[error("cannot safely move primary replica {0} without a normal secondary")]
    CannotMovePrimary(u64),
    #[error("target node already hosts shard {shard_id}: node={node_id}")]
    TargetAlreadyHostsShard {
        shard_id: ShardId,
        node_id: RaftNodeId,
    },
    #[error("no target node is available")]
    NoTargetNode,
    #[error("move target is not loading: {0}")]
    TargetNotLoading(u64),
    #[error("active membership is empty for shard {0}")]
    EmptyMembership(ShardId),
    #[error("primary replica {primary_replica_id} is not active for shard {shard_id}")]
    PrimaryNotActive {
        shard_id: ShardId,
        primary_replica_id: u64,
    },
    #[error("task not found: {0}")]
    TaskNotFound(u64),
    #[error("scheduler inflight limit is zero")]
    InvalidSchedulerInflight,
    #[error("scheduler snapshot is corrupt: {0}")]
    CorruptSchedulerSnapshot(String),
    #[error("partition-set topology is invalid: {0}")]
    InvalidPartitionSetTopology(String),
    #[error(
        "scheduler task {task_id} references shard {task_shard_id}, expected {topology_shard_id}"
    )]
    SchedulerShardMismatch {
        task_id: u64,
        task_shard_id: ShardId,
        topology_shard_id: ShardId,
    },
}

impl DeterministicTaskScheduler {
    pub fn submit(&mut self, priority: i32, now_ms: u64, kind: SchedulerTaskKind) -> SchedulerTask {
        let id = self.next_task_id;
        self.next_task_id += 1;
        let task = SchedulerTask {
            id,
            priority,
            create_time_ms: now_ms,
            next_run_time_ms: now_ms,
            last_run_time_ms: 0,
            retry_times: 0,
            kind,
        };
        self.queue.insert(id, task.clone());
        task
    }

    pub fn queue_len(&self) -> usize {
        self.queue.len()
    }

    pub fn snapshot(&self) -> Vec<SchedulerTask> {
        let mut tasks = self.queue.values().cloned().collect::<Vec<_>>();
        tasks.sort_by_key(|task| (task.priority, task.next_run_time_ms, task.id));
        tasks
    }

    pub fn export_snapshot(&self) -> TaskSchedulerSnapshot {
        TaskSchedulerSnapshot {
            tasks: self.snapshot(),
            next_task_id: self.next_task_id,
        }
    }

    pub fn encode_snapshot(&self) -> Result<Vec<u8>, RebalanceError> {
        serde_json::to_vec(&self.export_snapshot())
            .map_err(|err| RebalanceError::CorruptSchedulerSnapshot(err.to_string()))
    }

    pub fn restore_snapshot(snapshot: TaskSchedulerSnapshot) -> Result<Self, RebalanceError> {
        let mut queue = BTreeMap::new();
        let mut max_task_id = 0;
        for task in snapshot.tasks {
            max_task_id = max_task_id.max(task.id);
            if queue.insert(task.id, task).is_some() {
                return Err(RebalanceError::CorruptSchedulerSnapshot(
                    "duplicate task id".to_string(),
                ));
            }
        }
        Ok(Self {
            queue,
            next_task_id: snapshot.next_task_id.max(max_task_id + 1),
        })
    }

    pub fn decode_snapshot(bytes: &[u8]) -> Result<Self, RebalanceError> {
        let snapshot = serde_json::from_slice(bytes)
            .map_err(|err| RebalanceError::CorruptSchedulerSnapshot(err.to_string()))?;
        Self::restore_snapshot(snapshot)
    }

    pub fn has_update_membership_task(&self, shard_id: ShardId, membership_version: u64) -> bool {
        self.queue.values().any(|task| {
            matches!(
                &task.kind,
                SchedulerTaskKind::UpdateMembership(plan)
                    if plan.shard_id == shard_id
                        && plan.membership_version == membership_version
            )
        })
    }

    pub fn pop_first_available(&mut self, now_ms: u64) -> Option<SchedulerTask> {
        let task_id = self
            .queue
            .values()
            .filter(|task| task.next_run_time_ms <= now_ms)
            .min_by_key(|task| (task.priority, task.next_run_time_ms, task.id))
            .map(|task| task.id)?;
        self.queue.remove(&task_id)
    }

    pub fn run_next(
        &mut self,
        now_ms: u64,
        result: SchedulerTaskResult,
        options: TaskSchedulerOptions,
    ) -> Result<Option<SchedulerRunReport>, RebalanceError> {
        if options.max_inflight == 0 {
            return Err(RebalanceError::InvalidSchedulerInflight);
        }
        let Some(mut task) = self.pop_first_available(now_ms) else {
            return Ok(None);
        };
        task.last_run_time_ms = now_ms;
        let mut aborted = false;
        let next_run_time_ms = match result {
            SchedulerTaskResult::Ok | SchedulerTaskResult::Aborted => None,
            SchedulerTaskResult::RetryLater => {
                task.retry_times += 1;
                if task.retry_times > options.max_retry_times {
                    aborted = true;
                    None
                } else {
                    let postpone = task
                        .retry_times
                        .saturating_mul(options.base_postpone_ms)
                        .min(options.max_postpone_ms);
                    task.next_run_time_ms = now_ms.saturating_add(postpone);
                    let next = task.next_run_time_ms;
                    self.queue.insert(task.id, task.clone());
                    Some(next)
                }
            }
        };
        Ok(Some(SchedulerRunReport {
            task_id: task.id,
            result,
            retry_times: task.retry_times,
            next_run_time_ms,
            queue_len: self.queue.len(),
            aborted,
        }))
    }
}

impl SchedulerTask {
    pub fn lifecycle_token(&self) -> Option<SchedulerLifecycleToken> {
        let SchedulerTaskKind::RebalanceStep(step) = &self.kind else {
            return None;
        };
        let (shard_id, operation, load_version) = match step {
            RebalanceStep::LoadTarget {
                shard_id,
                load_version,
                ..
            } => (*shard_id, "load", *load_version),
            RebalanceStep::ReloadTarget {
                shard_id,
                load_version,
                ..
            } => (*shard_id, "reload", *load_version),
            RebalanceStep::UnloadSource { shard_id, .. } => (*shard_id, "unload", 0),
            RebalanceStep::FreezeSource { shard_id, .. } => (*shard_id, "freeze", 0),
            RebalanceStep::UpdateMembership { shard_id, .. } => (*shard_id, "membership", 0),
        };
        Some(SchedulerLifecycleToken {
            task_id: self.id,
            shard_id,
            operation: operation.to_string(),
            load_version,
            generation: self.create_time_ms,
        })
    }
}

impl PartitionSetTopology {
    pub fn from_replicas(
        namespace: impl Into<String>,
        table_name: impl Into<String>,
        table_id: u64,
        partition_set_id: u64,
        shard_id: ShardId,
        topology_version: u64,
        membership_version: u64,
        unit_version: u64,
        server_addrs: &BTreeMap<RaftNodeId, String>,
        replicas: impl IntoIterator<Item = ShardReplica>,
    ) -> Result<Self, RebalanceError> {
        let mut members = replicas
            .into_iter()
            .filter(|replica| replica.shard_id == shard_id)
            .map(|replica| PartitionSetMember {
                replica_id: replica.replica_id,
                node_id: replica.node_id,
                role: replica.role,
                state: replica.state,
                load_version: replica.load_version,
                membership_version,
                server_addr: server_addrs
                    .get(&replica.node_id)
                    .cloned()
                    .unwrap_or_default(),
            })
            .collect::<Vec<_>>();
        members.sort_by_key(|member| member.replica_id);
        let active_replica_ids = members
            .iter()
            .filter(|member| matches!(member.state, ShardReplicaState::Normal))
            .map(|member| member.replica_id)
            .collect::<Vec<_>>();
        let frozen_replica_ids = members
            .iter()
            .filter(|member| {
                matches!(
                    member.state,
                    ShardReplicaState::Freezing | ShardReplicaState::Frozen
                )
            })
            .map(|member| member.replica_id)
            .collect::<Vec<_>>();
        let primary_replica_id = members
            .iter()
            .find(|member| member.role == ShardRole::Primary)
            .map(|member| member.replica_id)
            .unwrap_or_default();
        let topology = Self {
            namespace: namespace.into(),
            table_name: table_name.into(),
            table_id,
            partition_set_id,
            shard_id,
            topology_version,
            membership_version,
            unit_version,
            primary_replica_id,
            active_replica_ids,
            frozen_replica_ids,
            members,
        };
        topology.validate()?;
        Ok(topology)
    }

    pub fn validate(&self) -> Result<(), RebalanceError> {
        if self.namespace.is_empty() || self.table_name.is_empty() {
            return Err(RebalanceError::InvalidPartitionSetTopology(
                "namespace/table_name are required".to_string(),
            ));
        }
        if self.topology_version == 0 || self.membership_version == 0 {
            return Err(RebalanceError::InvalidPartitionSetTopology(
                "topology_version and membership_version must be non-zero".to_string(),
            ));
        }
        if self.members.is_empty() {
            return Err(RebalanceError::InvalidPartitionSetTopology(
                "partition set must contain at least one member".to_string(),
            ));
        }
        let member_ids = self
            .members
            .iter()
            .map(|member| member.replica_id)
            .collect::<BTreeSet<_>>();
        if member_ids.len() != self.members.len() {
            return Err(RebalanceError::InvalidPartitionSetTopology(
                "duplicate replica ids".to_string(),
            ));
        }
        if !member_ids.contains(&self.primary_replica_id) {
            return Err(RebalanceError::InvalidPartitionSetTopology(
                "primary replica is not a member".to_string(),
            ));
        }
        let active = self
            .active_replica_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if !active.is_subset(&member_ids) {
            return Err(RebalanceError::InvalidPartitionSetTopology(
                "active replica ids must be members".to_string(),
            ));
        }
        if !active.contains(&self.primary_replica_id) {
            return Err(RebalanceError::InvalidPartitionSetTopology(
                "primary replica must be active".to_string(),
            ));
        }
        let frozen = self
            .frozen_replica_ids
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        if !frozen.is_subset(&member_ids) {
            return Err(RebalanceError::InvalidPartitionSetTopology(
                "frozen replica ids must be members".to_string(),
            ));
        }
        Ok(())
    }

    pub fn member_addr(&self, node_id: RaftNodeId) -> Option<&str> {
        self.members
            .iter()
            .find(|member| member.node_id == node_id)
            .map(|member| member.server_addr.as_str())
    }
}

impl RaftPersistedSchedulerState {
    pub fn validate(&self) -> Result<(), RebalanceError> {
        self.topology.validate()?;
        let scheduler = DeterministicTaskScheduler::restore_snapshot(self.scheduler.clone())?;
        for task in scheduler.snapshot() {
            if let Some(token) = task.lifecycle_token() {
                if token.shard_id != self.topology.shard_id {
                    return Err(RebalanceError::SchedulerShardMismatch {
                        task_id: task.id,
                        task_shard_id: token.shard_id,
                        topology_shard_id: self.topology.shard_id,
                    });
                }
            }
        }
        for execution in &self.executions {
            if !self
                .topology
                .members
                .iter()
                .any(|member| member.node_id == execution.target_node_id)
            {
                return Err(RebalanceError::InvalidPartitionSetTopology(format!(
                    "execution target node {} is not a partition-set member",
                    execution.target_node_id
                )));
            }
            if let Some(token) = &execution.lifecycle_token {
                if token.shard_id != self.topology.shard_id {
                    return Err(RebalanceError::SchedulerShardMismatch {
                        task_id: execution.task_id,
                        task_shard_id: token.shard_id,
                        topology_shard_id: self.topology.shard_id,
                    });
                }
            }
        }
        Ok(())
    }
}

impl RebalanceController {
    pub fn with_replicas(replicas: impl IntoIterator<Item = ShardReplica>) -> Self {
        let mut max_replica_id = 0;
        let mut map = BTreeMap::new();
        for replica in replicas {
            max_replica_id = max_replica_id.max(replica.replica_id);
            map.insert(replica.replica_id, replica);
        }
        Self {
            replicas: map,
            membership_version: 1,
            next_replica_id: max_replica_id + 1,
        }
    }

    pub fn replicas(&self) -> Vec<ShardReplica> {
        self.replicas.values().cloned().collect()
    }

    pub fn membership_version(&self) -> u64 {
        self.membership_version
    }

    pub fn node_loads(&self) -> BTreeMap<RaftNodeId, usize> {
        let mut loads = BTreeMap::new();
        for replica in self
            .replicas
            .values()
            .filter(|replica| matches!(replica.state, ShardReplicaState::Normal))
        {
            *loads.entry(replica.node_id).or_default() += 1;
        }
        loads
    }

    pub fn plan_rebalance_round(
        &self,
        all_node_ids: impl IntoIterator<Item = RaftNodeId>,
        options: RebalanceOptions,
    ) -> Vec<ShardMovePlan> {
        self.plan_rebalance_round_report(all_node_ids, options)
            .plans
    }

    pub fn plan_rebalance_round_report(
        &self,
        all_node_ids: impl IntoIterator<Item = RaftNodeId>,
        options: RebalanceOptions,
    ) -> RebalanceRoundReport {
        let all_nodes = all_node_ids.into_iter().collect::<BTreeSet<_>>();
        let mut simulated_loads = self.node_loads();
        for node_id in &all_nodes {
            simulated_loads.entry(*node_id).or_default();
        }
        let total_normal = simulated_loads.values().sum::<usize>();
        let safe_line = if all_nodes.is_empty() || total_normal == 0 {
            0
        } else {
            total_normal.div_ceil(all_nodes.len()) + options.partition_count_safe_gap
        };
        let mut report = RebalanceRoundReport {
            balance_partition_count: simulated_loads.clone(),
            total_normal,
            safe_line,
            ..RebalanceRoundReport::default()
        };

        if all_nodes.len() < 2 || options.max_moves_per_round == 0 {
            return report;
        }
        if total_normal == 0 {
            return report;
        }
        let mut next_replica_id = self.next_replica_id;
        let mut candidates = self
            .replicas
            .values()
            .filter(|replica| replica.state == ShardReplicaState::Normal)
            .cloned()
            .collect::<Vec<_>>();
        candidates.sort_by_key(|replica| (replica.node_id, replica.shard_id, replica.replica_id));

        for source in candidates {
            if report.plans.len() >= options.max_moves_per_round {
                break;
            }
            if simulated_loads
                .get(&source.node_id)
                .copied()
                .unwrap_or_default()
                <= safe_line
            {
                continue;
            }
            if source.role == ShardRole::Primary && !self.has_normal_secondary(source.shard_id) {
                record_placement_fail(&mut report, "primary_without_normal_secondary");
                continue;
            }
            let Some(target_node_id) = least_loaded_target(
                &simulated_loads,
                &all_nodes,
                source.shard_id,
                &self.replicas,
            ) else {
                record_placement_fail(&mut report, "no_target_node");
                continue;
            };
            if target_node_id == source.node_id {
                record_placement_fail(&mut report, "target_is_source");
                continue;
            }
            report.plans.push(ShardMovePlan {
                shard_id: source.shard_id,
                source_replica_id: source.replica_id,
                source_node_id: source.node_id,
                target_replica_id: next_replica_id,
                target_node_id,
                load_version: source.load_version + 1,
            });
            next_replica_id += 1;
            *simulated_loads.entry(source.node_id).or_default() -= 1;
            *simulated_loads.entry(target_node_id).or_default() += 1;
        }

        report
    }

    pub fn begin_move(
        &mut self,
        plan: &ShardMovePlan,
    ) -> Result<Vec<RebalanceStep>, RebalanceError> {
        let source = self
            .replicas
            .get(&plan.source_replica_id)
            .ok_or(RebalanceError::ReplicaNotFound(plan.source_replica_id))?;
        if source.role == ShardRole::Primary && !self.has_normal_secondary(source.shard_id) {
            return Err(RebalanceError::CannotMovePrimary(source.replica_id));
        }
        if self.replicas.values().any(|replica| {
            replica.shard_id == plan.shard_id
                && replica.node_id == plan.target_node_id
                && replica.state != ShardReplicaState::Frozen
        }) {
            return Err(RebalanceError::TargetAlreadyHostsShard {
                shard_id: plan.shard_id,
                node_id: plan.target_node_id,
            });
        }

        self.replicas.insert(
            plan.target_replica_id,
            ShardReplica {
                shard_id: plan.shard_id,
                replica_id: plan.target_replica_id,
                node_id: plan.target_node_id,
                role: ShardRole::Secondary,
                state: ShardReplicaState::Loading,
                load_version: plan.load_version,
            },
        );
        self.next_replica_id = self.next_replica_id.max(plan.target_replica_id + 1);

        Ok(vec![RebalanceStep::LoadTarget {
            shard_id: plan.shard_id,
            replica_id: plan.target_replica_id,
            node_id: plan.target_node_id,
            load_version: plan.load_version,
        }])
    }

    pub fn finish_target_load(
        &mut self,
        target_replica_id: u64,
    ) -> Result<Vec<RebalanceStep>, RebalanceError> {
        let (shard_id, source_replica_id, source_node_id, primary_replica_id) = {
            let target = self
                .replicas
                .get(&target_replica_id)
                .ok_or(RebalanceError::ReplicaNotFound(target_replica_id))?;
            if target.state != ShardReplicaState::Loading {
                return Err(RebalanceError::TargetNotLoading(target_replica_id));
            }
            let source = self
                .replicas
                .values()
                .find(|replica| {
                    replica.shard_id == target.shard_id
                        && replica.state == ShardReplicaState::Normal
                        && replica.node_id != target.node_id
                })
                .ok_or(RebalanceError::NoTargetNode)?;
            let primary_replica_id = self
                .primary_replica_id(target.shard_id)
                .unwrap_or(target_replica_id);
            (
                target.shard_id,
                source.replica_id,
                source.node_id,
                primary_replica_id,
            )
        };
