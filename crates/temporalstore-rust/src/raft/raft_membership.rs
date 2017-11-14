// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::meta::{MetaEntityState, ServerMetaInfo, TableTopologyResponse};
use crate::types::ShardId;

use super::{
    JointConsensusMembership, RaftCluster, RaftError, RaftMembership, RaftNodeId, RaftReplicaLag,
};

pub type TemporalRaftProcessNodeEvidence = super::MatrixRaftProcessNodeEvidence;
pub type TemporalRaftProcessOperationalSemanticsEvidence =
    super::MatrixRaftProcessOperationalSemanticsEvidence;
pub type TemporalRaftDataNodeProcessRolloutReport = super::MatrixRaftDataNodeProcessRolloutReport;
pub type TemporalRaftMetaProcessRolloutReport = super::MatrixRaftMetaProcessRolloutReport;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftCatchUpReport {
    pub leader_id: RaftNodeId,
    pub leader_commit_index: u64,
    pub max_entries_per_follower: u64,
    pub replayed_log_entries: u64,
    pub caught_up_voters: Vec<RaftNodeId>,
    pub lagging_voters: Vec<RaftReplicaLag>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftScaleChangeReport {
    pub leader_id: RaftNodeId,
    pub voters: Vec<RaftNodeId>,
    pub live_voters: usize,
    pub majority: usize,
    pub caught_up_voters: Vec<RaftNodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RaftMembershipChangeKind {
    AddVoter,
    RemoveVoter,
    ReplaceVoter,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMembershipChangePlan {
    pub shard_id: ShardId,
    pub kind: RaftMembershipChangeKind,
    pub old_voters: Vec<RaftNodeId>,
    pub new_voters: Vec<RaftNodeId>,
    pub add_voters: Vec<RaftNodeId>,
    pub remove_voters: Vec<RaftNodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RaftMembershipChangeReport {
    pub plan: RaftMembershipChangePlan,
    pub joint_membership: JointConsensusMembership,
    pub committed_membership: RaftMembership,
    pub caught_up_voters: Vec<RaftNodeId>,
    pub leader_id: RaftNodeId,
    pub commit_index: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetaDataRaftMembershipWorkflowReport {
    pub shard_id: ShardId,
