// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const BUCKET_COUNT: u32 = 1 << 30;
pub const BUCKET_MASK: u32 = BUCKET_COUNT - 1;
pub const MIN_BUCKETS_PER_PARTITION: u32 = 1 << 14;
pub const PARTITION_VERSION_MASK: u32 = 0xFFFF;
pub const MAX_TABLE_ID: u32 = 0xFFFF;
pub const PARTITION_INDEX_MASK: u32 = 0xFF;
pub const MAX_PARTITION_SET_INDEX: u32 = 0xFFFF;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum PartitionIdError {
    #[error("table_id out of partition id range: {0}")]
    TableIdOutOfRange(u64),
    #[error("partition_set_index out of partition id range: {0}")]
    PartitionSetIndexOutOfRange(u64),
    #[error("partition_index out of partition id range: {0}")]
    PartitionIndexOutOfRange(u64),
    #[error("partition_version out of partition id range: {0}")]
    PartitionVersionOutOfRange(u64),
    #[error("partition set count must be > 0 and <= {max}: {value}")]
    InvalidPartitionSetCount { value: u32, max: u32 },
    #[error("partition count per set must be > 0 and <= 255: {0}")]
    InvalidPartitionCountPerSet(u32),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId {
    id: u64,
}

impl PartitionId {
    pub fn new(
        table_id: u64,
        partition_set_index: u64,
        partition_index: u64,
        partition_version: u64,
    ) -> Result<Self, PartitionIdError> {
        validate_u16(table_id, PartitionIdError::TableIdOutOfRange)?;
        validate_u16(
            partition_set_index,
            PartitionIdError::PartitionSetIndexOutOfRange,
        )?;
        if partition_index > PARTITION_INDEX_MASK as u64 {
            return Err(PartitionIdError::PartitionIndexOutOfRange(partition_index));
        }
        validate_u16(
            partition_version,
            PartitionIdError::PartitionVersionOutOfRange,
