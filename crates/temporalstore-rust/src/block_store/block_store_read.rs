// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! BlockStore read/read_range/install_slab methods, split from block_store.rs.
use super::*;
use super::record::sha256_bytes;

impl BlockStore {
    pub fn read(&self, address: &ElementEntry) -> Result<Vec<u8>, BlockStoreError> {
        // On-demand lazy recovery: if this slab lives only in shared storage after a
        // metadata-only restore, fetch + cache it before serving the read.
        self.ensure_slab_present(address.block_slab_id())?;
        let mut tally = BlockStoreReadTally::start();
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let read = LocalSlabBackend::new(&inner.root).read_range(
            address.block_slab_id(),
            address.offset(),
            address.length(),
            &mut tally,
        )?;
        // Charged before the decode, not after. A read that reached the disk cost the disk
        // whether or not the record on it decodes, and the old order left a failed decode
        // looking like a read that never happened.
        let bytes = read.charge(&mut inner.stats);
        let decoded = decode_block_record(&bytes, address)?;
        // `decode_block_record` just verified this payload against the CRC32C stored in the
        // record envelope, and cross-checked the record header's page id against this address.
        // It cross-checks NOTHING ELSE: the header carries no object id and no routing bucket, so
        // the two arms that name them cannot match -- read the note in `decode_block_record`
        // before relying on either. A second comparison against a digest carried in the index
        // added nothing here: the CRC covers corruption of these bytes, and it is the page id and
        // the stored-length check, not the CRC, that stand against an entry pointing at the wrong
        // page.
        let bytes = decoded.payload;
        inner.stats.logical_bytes_read += decoded.logical_len as u64;
        if decoded.compression == BlockRecordCompression::Zstd {
            inner.stats.compressed_records_read += 1;
        }
        Ok(bytes)
    }

    pub fn read_range(
        &self,
        block_slab_id: u64,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, BlockStoreError> {
        // On-demand lazy recovery: drive the shared-store read-through for slab-report /
        // streaming reads too, so a not-yet-fetched checkpoint slab is pulled + cached on demand.
        self.ensure_slab_present(block_slab_id)?;
        let mut tally = BlockStoreReadTally::start();
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        let read = LocalSlabBackend::new(&inner.root).read_range_at_most(
            block_slab_id,
            offset,
            size,
            &mut tally,
        )?;
        Ok(read.charge(&mut inner.stats))
    }

    pub fn read_logical_range(
        &self,
        block_slab_id: u64,
        offset: u64,
        size: u64,
    ) -> Result<Vec<u8>, BlockStoreError> {
        // On-demand lazy recovery: drive the shared-store read-through for slab-report /
        // streaming reads too, so a not-yet-fetched checkpoint slab is pulled + cached on demand.
        self.ensure_slab_present(block_slab_id)?;
        let mut tally = BlockStoreReadTally::start();
        let mut inner = self.inner.lock().expect("block store lock poisoned");
        // `bytes_read` is charged the WHOLE slab, which is what this read pulls off disk to
        // answer a logical range. It used to be charged the length of the slice handed back --
        // the same number `logical_bytes_read` gets, which is the field that means that.
        let read = LocalSlabBackend::new(&inner.root).read_all(block_slab_id, &mut tally)?;
        let slab = read.charge(&mut inner.stats);
        let range = logical_range_from_slab(&slab, block_slab_id, offset, size)?;
        let bytes = range.bytes;
        inner.stats.logical_bytes_read += bytes.len() as u64;
        inner.stats.compressed_records_read += range.compressed_records_read;
        Ok(bytes)
    }

    /// A whole slab, by id.
    ///
    /// COUNTED, which it was not. This read the slab file straight off disk and charged the store
    /// nothing, so the dump manifest, the cluster snapshot and the two shared-storage paths that
    /// use it were invisible in `BlockStoreStats::reads` and `bytes_read` -- the other three read
    /// entry points beside it all charged. Going through the backend makes that impossible to
    /// repeat: `CountedSlabRead` does not give up its bytes without a `&mut BlockStoreStats`.
    ///
    /// The store lock is still NOT held across the file read -- the root is cloned for the read
    /// and the lock is taken again only to charge -- because a whole-slab read under the lock
    /// would block every writer for the length of a slab.
