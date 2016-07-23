// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::collections::{HashMap, HashSet};

use crate::types::ShardId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisCommandState {
    pub config: HashMap<String, String>,
    pub keyspace: HashSet<String>,
    pub master: Option<(String, String)>,
    pub authenticated: bool,
    pub loaded_shard_id: Option<ShardId>,
}
