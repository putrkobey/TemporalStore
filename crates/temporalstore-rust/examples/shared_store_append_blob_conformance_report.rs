// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use std::sync::Arc;
use std::time::Instant;

use matrixobjectstore_rs::StoreOptions;
use prost::Message;
use serde::Serialize;
use sha2::{Digest, Sha256};
use temporalstore_rust::shared_store::{
    ReplayReport, SharedStoreFlushReport, SharedStoreReplicator, SharedStoreStorageMode,
    SharedStoreWalAppendMode, SharedStoreWalEntry, SharedStoreWalOffsetMetadata,
    SharedStoreWriteReport,
};
use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, MatrixObjectObjectStore, TemporalEngine,
};
use temporalstore_snapshot::object_store::AppendBlobReceipt;

const DEFAULT_ENTRY_COUNT: u64 = 8;
const DEFAULT_VALUE_BYTES: usize = 64;
const PERSISTENT_JOURNAL_FILE: &str = "matrixobjectstore-incremental.journal";

#[derive(Debug, Serialize)]
struct Report {
    schema: &'static str,
    backend: &'static str,
    matrixobject_mode: &'static str,
    entry_count: u64,
    value_bytes: usize,
    direct_publish: PublishPhaseReport,
    sync_writer: WriterPhaseReport,
    async_writer: AsyncWriterPhaseReport,
    journal_reopen: JournalReopenReport,
    summary: Summary,
}

/// This report's OWN reader of the log frame, independent of the one in the crate.
///
/// Being independent is the point: it checks that a frame on the wire says what the writer meant,
/// without borrowing the writer's own decoder to do it. The cost of that independence is that it
/// has to be kept in step with the frame by hand, and nothing makes it fail when it is not --
/// `all_offsets_decode_expected_frame` is a REPORTED FIELD, so a reader that understands none of
/// the frames prints `false` and exits zero.
///
/// That is not hypothetical. When the log frame's digest moved from tag 5 to tag 9, this reader
/// still looked only at tag 5: it matched 0 of 8 frames, reported `false`, and the program exited
/// successfully. Hence the digest dispatch below AND the assertion in `main` -- the dispatch keeps
/// it correct, the assertion is what makes it fail next time instead of whispering.
#[derive(Clone, PartialEq, Message)]
struct ReportWalFrameProto {
    #[prost(uint64, tag = "1")]
    shard_id: u64,
    #[prost(uint64, tag = "2")]
    wal_index: u64,
    #[prost(bytes = "vec", tag = "3")]
    command_payload: Vec<u8>,
    #[prost(uint64, tag = "4")]
    command_byte_size: u64,
    /// The digest as hex. Populated by frames declaring the hex format; empty in a narrow frame.
    #[prost(string, tag = "5")]
    command_sha256: String,
    #[prost(uint32, tag = "6")]
    command_encoding: u32,
    /// The digest as its own 32 bytes, which is what a narrow frame carries instead of tag 5.
    #[prost(bytes = "vec", tag = "9")]
    command_digest: Vec<u8>,
    /// Which digest shape this frame carries. Absent is zero is the hex shape, so a frame written
    /// before the field existed reads correctly without a special case.
    #[prost(uint32, tag = "10")]
    frame_format: u32,
    /// A CRC32C of the payload, which is what the narrowest frame carries instead of either.
    #[prost(fixed32, tag = "11")]
    command_crc32c: u32,
}

/// The digest shapes this reader understands, by the value the frame declares.
///
/// Every format the ENCODER can produce is listed here, including ones it does not yet write by
/// default. That is deliberate: the last time this reader knew fewer formats than the writer could
/// emit, it matched 0 of 8 frames and said so in a field nobody was asserting. Teaching it a
/// format early costs one match arm; learning about one late costs a silent report.
const REPORT_FRAME_FORMAT_HEX_DIGEST: u32 = 0;
const REPORT_FRAME_FORMAT_RAW_DIGEST: u32 = 2;
const REPORT_FRAME_FORMAT_CRC32C: u32 = 3;
const REPORT_FRAME_FORMAT_NO_SIZE: u32 = 4;

/// The crate's CRC32C, not a second copy of it.
///
/// This reader is independent of the writer about the FRAME LAYOUT, which is what it exists to
/// check. It is deliberately not independent about the checksum algorithm: a second
/// implementation of CRC32C here could drift from the one the store actually uses, and then this
/// report would be checking frames against a checksum nothing writes.
fn crc32c_of(bytes: &[u8]) -> u32 {
    temporalstore_rust::checksum::crc32c(bytes)
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

#[derive(Debug, Serialize)]
struct OffsetFrameValidation {
    checked_frames: usize,
    matched_frames: usize,
    all_offsets_decode_expected_frame: bool,
    contiguous_coverage_bytes: u64,
}

#[derive(Debug, Serialize)]
struct OffsetIndexValidation {
    checked_entries: usize,
    matched_entries: usize,
    all_wal_indexes_have_offset_metadata: bool,
    all_metadata_ranges_match_append_receipts: bool,
}

#[derive(Debug, Serialize)]
struct OffsetMetadataMapping {
    shard_id: u64,
    wal_index: u64,
    blob_key: String,
    start_offset: u64,
    end_offset: u64,
    bytes_written: u64,
    command_encoding: u32,
    object_length: u64,
    physical_slab_count: u64,
    first_physical_offset: Option<u64>,
    expected_start_offset: Option<u64>,
    expected_end_offset: Option<u64>,
    expected_bytes_written: Option<u64>,
    matches_expected_offsets: bool,
}

#[derive(Debug, Serialize)]
struct AuthoritativeOffsetLookupReport {
    checked_entries: u64,
    metadata_hits: u64,
    decoded_entries: u64,
    matched_entries: u64,
    range_bytes_read: u64,
    exact_offset_slice_reads: u64,
    range_reads_smaller_than_blob: u64,
    slab_metadata_entries: u64,
    first_physical_offset_entries: u64,
    all_wal_indexes_directly_read_from_offset_metadata: bool,
    all_direct_reads_match_expected_entries: bool,
    all_direct_reads_have_slab_metadata: bool,
    lower_layer_range_reads_exact_offset_slices: bool,
    lower_layer_range_reads_avoid_full_blob_scans: bool,
    lower_layer_blob_offset_reads_proven: bool,
    errors: Vec<String>,
}

#[derive(Debug, Serialize)]
struct PublishPhaseReport {
    shard_id: u64,
    blob_key: String,
    publish_latencies_us: Vec<u128>,
    append_receipts: Vec<AppendBlobReceipt>,
    offset_metadata_mappings: Vec<OffsetMetadataMapping>,
    blob_object_length: u64,
    blob_physical_slab_count: usize,
    offset_frame_validation: OffsetFrameValidation,
    offset_index_validation: OffsetIndexValidation,
    authoritative_offset_lookup: AuthoritativeOffsetLookupReport,
    replay: ReplayPhaseReport,
}

#[derive(Debug, Serialize)]
struct WriterPhaseReport {
    shard_id: u64,
    write_latencies_us: Vec<u128>,
    write_reports: Vec<SharedStoreWriteReport>,
    offset_metadata_mappings: Vec<OffsetMetadataMapping>,
    blob_object_length: u64,
    blob_physical_slab_count: usize,
    authoritative_offset_lookup: AuthoritativeOffsetLookupReport,
    replay: ReplayPhaseReport,
}

#[derive(Debug, Serialize)]
struct AsyncWriterPhaseReport {
    shard_id: u64,
    enqueue_latencies_us: Vec<u128>,
    write_reports: Vec<SharedStoreWriteReport>,
    flush_latency_us: u128,
    flush_report: SharedStoreFlushReport,
    offset_metadata_mappings: Vec<OffsetMetadataMapping>,
    blob_object_length: u64,
    blob_physical_slab_count: usize,
    authoritative_offset_lookup: AuthoritativeOffsetLookupReport,
    replay: ReplayPhaseReport,
}

#[derive(Debug, Serialize)]
struct MatrixObjectCacheStatsReport {
    entries: u64,
    bytes: u64,
    max_bytes: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
    direct_fill_blocks: u64,
    direct_fill_bytes: u64,
    compressed_fill_blocks: u64,
    compressed_fill_bytes: u64,
    ec_fill_blocks: u64,
    ec_fill_bytes: u64,
    pressure: bool,
}

#[derive(Debug, Serialize)]
struct JournalReopenReport {
    snapshot_export_latency_us: u128,
    snapshot_disk_write_latency_us: u128,
    snapshot_disk_read_latency_us: u128,
    snapshot_import_latency_us: u128,
    snapshot_bytes: u64,
    snapshot_sha256: String,
    reopened_offset_metadata_ok: bool,
    cache_before_replay: MatrixObjectCacheStatsReport,
    cache_after_retrieval: MatrixObjectCacheStatsReport,
    cache_metrics_available: bool,
    reopened_replay_recovered_all_records: bool,
    reopened_retrieval_recovered_all_records: bool,
    replay_latency_total_us: u128,
    retrieval_latency_avg_us: u128,
}

#[derive(Debug, Serialize)]
struct ReplayPhaseReport {
    replay_latency_us: u128,
    replay_report: ReplayReport,
    retrieval_latencies_us: Vec<u128>,
    retrieved_records: u64,
    retrieval_ok: bool,
    secondary_replay_latency_us: u128,
    secondary_replay_report: ReplayReport,
    secondary_retrieved_records: u64,
    secondary_retrieval_ok: bool,
    single_node_reload_latency_us: u128,
    single_node_reload_report: ReplayReport,
    single_node_reload_retrieved_records: u64,
