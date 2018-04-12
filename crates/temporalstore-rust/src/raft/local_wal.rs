// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! LocalRaftWal WAL persistence/recovery methods, split from raft.rs.
use super::*;

/// An append whose bytes are in the file but whose durability barrier has not been taken.
///
/// Writing and flushing are separated so a caller can hold a lock across the write -- record
/// order matters, since each record carries the log and an older one landing after a newer one
/// would regress it on recovery -- and then release that lock before the barrier, which is both
/// the expensive part and the part that concurrent writers can share.
#[must_use = "a staged append is not durable until finish_staged takes its barrier"]
pub struct StagedWalAppend {
    gate: Arc<crate::flush_gate::FlushGate>,
    ticket: crate::flush_gate::FlushTicket,
    file: fs::File,
    shard_id: ShardId,
    node_id: RaftNodeId,
    min_keep_segments: usize,
    slow_fsync_threshold_ms: u64,
    /// Whether this append wrote a base whose snapshot boundary ADVANCED -- the one kind of
    /// record that supersedes on-disk history. Its barrier makes earlier segments prunable.
    compacting_base: bool,
}

impl LocalRaftWal {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            cursors: Arc::new(Mutex::new(BTreeMap::new())),
            flush_gates: Arc::new(crate::flush_gate::FlushRegistry::default()),
        }
    }

    pub fn persist_node(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
    ) -> io::Result<()> {
        self.append_node_record(shard_id, node_id, record)
    }

    pub fn append_node_record(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
    ) -> io::Result<()> {
        let path = self.node_path(shard_id, node_id);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let recovery = self.recover_node(shard_id, node_id)?;
        let envelope = RaftWalEnvelope {
            sequence: recovery.valid_records as u64 + 1,
            checksum: raft_wal_checksum(record)?,
            record: record.clone(),
            // The single-file (non-segmented) path always writes full records.
            delta: None,
        };
        let mut file = OpenOptions::new().create(true).append(true).open(&path)?;
        file.write_all(&wal_proto::encode_record_line(&envelope, true)?)?;
        crate::durability_metrics::record_barrier("raft_wal_append_unsegmented");
        file.sync_data()?;
        Ok(())
    }

    pub fn persist_node_with_retention(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
        keep_last: usize,
    ) -> io::Result<()> {
        self.append_node_record(shard_id, node_id, record)?;
        self.compact_node_records(shard_id, node_id, keep_last)
    }

    /// The file a snapshot's state image lives in, named by the snapshot's boundary index.
    fn image_path(&self, shard_id: ShardId, node_id: RaftNodeId, index: u64) -> PathBuf {
        self.node_segment_dir(shard_id, node_id)
            .join(format!("image-{index:020}.bin"))
    }

    /// Move a record's in-memory image into its own durable file, returning a record that
    /// references it. The file is fsynced BEFORE any record naming it can be written, so a
    /// record that says "my image is in a file" is never durable ahead of the file itself.
    /// Older image files are pruned: recovery folds records forward, so only the newest
    /// snapshot's image is ever read back.
    fn externalize_state_image(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
    ) -> io::Result<Option<RaftWalRecord>> {
        let Some(snapshot) = &record.installed_snapshot else {
            return Ok(None);
        };
        let Some(image) = &snapshot.state_image else {
            return Ok(None);
        };
        let index = snapshot.last_included_index;
        let path = self.image_path(shard_id, node_id, index);
        if !path.exists() {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let message = crate::sdk::v1::WalStateImage {
                index: image.index_bytes.clone(),
                next_block_id: image.next_block_id,
                start_routing_bucket: image
                    .routing_range
                    .map(|range| range.start_routing_bucket),
                end_routing_bucket: image
                    .routing_range
                    .map(|range| range.end_routing_bucket),
                slabs: image
                    .slabs
                    .iter()
                    .map(|slab| crate::sdk::v1::WalStateImageSlab {
                        block_slab_id: slab.block_slab_id,
                        slab: slab.bytes.clone(),
                    })
                    .collect(),
            };
            let bytes = compress_state_image(prost::Message::encode_to_vec(&message));
            let tmp = path.with_extension("tmp");
            {
                let mut file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&tmp)?;
                // Binary frame: the payload here is compressed bytes, and a text frame
                // would let one ending in 0x0A be mistaken for its own terminator.
                file.write_all(&crate::log_framing::encode_frame(&bytes))?;
                file.sync_data()?;
            }
            fs::rename(&tmp, &path)?;
            // Prune superseded images: recovery only ever reattaches the newest.
            if let Ok(entries) = fs::read_dir(self.node_segment_dir(shard_id, node_id)) {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name = name.to_string_lossy();
                    if let Some(stem) = name.strip_prefix("image-").and_then(|rest| {
                        rest.strip_suffix(".bin")
                    }) {
                        if stem.parse::<u64>().map(|old| old < index).unwrap_or(false) {
                            let _ = fs::remove_file(entry.path());
                        }
                    }
                }
            }
        }
        let mut externalized = record.clone();
        if let Some(snapshot) = externalized.installed_snapshot.as_mut() {
            snapshot.state_image = None;
            snapshot.state_image_externalized = true;
        }
        Ok(Some(externalized))
    }

    /// Reattach an externalized image to a recovered record. A record that references a file
    /// which does not exist is COMMITTED corruption -- the file is made durable before any
    /// record naming it, so a torn tail cannot produce this.
    fn reattach_state_image(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &mut RaftWalRecord,
    ) -> io::Result<()> {
        let Some(snapshot) = record.installed_snapshot.as_mut() else {
            return Ok(());
        };
        if !snapshot.state_image_externalized || snapshot.state_image.is_some() {
            return Ok(());
        }
        let path = self.image_path(shard_id, node_id, snapshot.last_included_index);
        let raw = fs::read(&path).map_err(|err| {
            io::Error::other(format!(
                "snapshot image file missing for index {}: {err}",
                snapshot.last_included_index
            ))
        })?;
        // No newline stripping here: `decode_line` strips one itself for a text record, and
        // doing it first would take a real byte off a binary payload that ends in 0x0A.
        let payload = crate::log_framing::decode_line(&raw).map_err(io::Error::other)?;
        let payload = decompress_state_image(payload)?;
        let message = <crate::sdk::v1::WalStateImage as prost::Message>::decode(payload.as_ref())
            .map_err(io::Error::other)?;
        // BOTH ENDS OR NEITHER. A half-carried range is not a range, and treating one
        // present bound as a usable answer would put the other back to a default nobody chose.
        let routing_range = match (message.start_routing_bucket, message.end_routing_bucket) {
            (Some(start_routing_bucket), Some(end_routing_bucket)) => {
                Some(crate::engine::routing_range_stamp::RoutingRangeStamp {
                    start_routing_bucket,
                    end_routing_bucket,
                })
            }
            _ => None,
        };
        snapshot.state_image = Some(RaftSnapshotStateImage {
            index_bytes: message.index,
            next_block_id: message.next_block_id,
            routing_range,
            slabs: message
                .slabs
                .into_iter()
                .map(|slab| RaftSnapshotStateImageSlab {
                    block_slab_id: slab.block_slab_id,
                    bytes: slab.slab,
                })
                .collect(),
        });
        snapshot.state_image_externalized = false;
        Ok(())
    }

    pub fn persist_node_segmented(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
        max_segment_bytes: u64,
        min_keep_segments: usize,
    ) -> io::Result<RaftWalSegmentReport> {
        self.persist_node_segmented_with_fsync_threshold(
            shard_id,
            node_id,
            record,
            max_segment_bytes,
            min_keep_segments,
            u64::MAX,
        )
    }

    /// Write a record's bytes without taking its barrier. See `finish_staged`.
    pub fn stage_node_segmented(
        &self,
        shard_id: ShardId,
        node_id: RaftNodeId,
        record: &RaftWalRecord,
        max_segment_bytes: u64,
        min_keep_segments: usize,
    ) -> io::Result<StagedWalAppend> {
        self.stage_node_segmented_with_fsync_threshold(
            shard_id,
            node_id,
            record,
            max_segment_bytes,
            min_keep_segments,
            u64::MAX,
        )
    }
