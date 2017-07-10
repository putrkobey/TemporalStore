// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A timestamped series map that groups its objects by routing bucket and names each object's
//! identity ONCE.
//!
//! NOTHING USES THIS YET. It is the first of three steps: the container and its wire adapter land
//! alone so the design can be reviewed without a call-site conversion riding on it, then the shared
//! timestamped-series helpers are generalised, then one map is converted. A half-finished conversion
//! of that size would leave the tree worse than not starting.
//!
//! WHAT IT IS, AND WHY THIS SHAPE. Today a series map is `HashMap<key, BTreeMap<at, address>>`: one
//! whole B-tree per object key, however few points that key holds, and a one-entry B-tree charges 280
//! bytes of node to carry 16 bytes of value. This groups every object that routes to the same bucket
//! into ONE allocation: an `objects` array sorted by key, saying where each object's rows begin and
//! how many there are, and one contiguous fixed-width `rows` run holding them all in that order.
//!
//! Measured against what ships, RESIDENT bytes per row, at six occupancies. The first column pair
//! is #2075's original measurement; the second is a re-measurement on current main, pinned by
//! `index_bytes_per_key::what_the_grouped_series_shape_costs_against_the_one_that_ships`:
//!
//! | points a key | #2075: B-tree | #2075: this | now: B-tree | now: this | ratio |
//! |---|---|---|---|---|---|
//! | 1 | 366.0 | 74.9 | 364.0 | 73.6 | 4.95x |
//! | 2 | 182.9 | 50.9 | 182.0 | 60.5 | 3.01x |
//! | 4 | 91.3 | 38.9 | 91.0 | 47.9 | 1.90x |
//! | 10 | 38.5 | 31.7 | 33.3 | **35.1** | **0.95x** |
//! | 100 | 29.6 | 25.2 | 51.1 | 25.3 | 2.02x |
//! | 1,000 | 26.6 | 24.1 | 49.1 | 24.2 | 2.03x |
//!
//! THE SPARSE SAVING AND THE FLOOR BOTH HOLD, which is what matters: 73.6 against 74.9 at one point
//! a key, on a shipped shape at 364.0 against 366.0, and 24.2 against 24.1 at a thousand. A row
//! here is a `u64` and a 16-byte address -- 24 bytes of data -- so 24.2 at density is 1.008x the
//! data itself: the container overhead is a fraction of a byte a row and 24 is the floor. Going
//! below it would mean shrinking the ROW, not the container.
//!
//! BUT "BETTER AT EVERY OCCUPANCY WITH NO CROSSOVER" WAS WRONG, and it is corrected here rather
//! than quietly dropped, because it is the sentence a conversion would be justified by. At TEN
//! points a key the shipped shape holds 33.3 bytes a row and this one 35.1: it loses by 5%. Ten
//! entries fit ONE nearly-full `BTreeMap` node, which is the best a B-tree ever does.
//!
//! That same fact explains why the shipped column is NOT monotonic -- 33.3 at ten, rising to 51.1 at
//! a hundred -- and why #2075's figures for it at 100 and 1,000 do not reproduce: ascending inserts
//! leave a larger map's nodes about half full, so its per-row cost climbs again past the
//! single-node point, and whether a measurement catches that depends on the insertion order its
//! fixture used.
//!
//! SO THE SHAPE IS WORTH TAKING FOR MAPS WHOSE KEYS HOLD FEW POINTS, which is the case it was built
//! for and where it is 2-5x better. It is not a free win everywhere: a map whose keys cluster
//! around ten points would be made slightly worse. Check the occupancy of the map being converted
//! -- `series_occupancy` in the test module reports it -- rather than taking the sparse figure as
//! the whole story.
//!
//! WHAT IS DELIBERATELY NOT COPIED. The rows are FIXED WIDTH and kept sorted, so a lookup is two
//! binary searches. A variable-length payload searched by walking it is refuted twice over -- on the
//! read path at 3.0-17.8x on misses, and from the footprint side -- and the reason is structural:
//! variable-length packing is both what makes such a buffer compact and what stops it being
//! binary-searchable. The two-level SHAPE is what is being taken; the walk is not.
//!
//! WHY A PER-OBJECT START AND LENGTH DOES NOT GIVE THE SAVING BACK. An offset table costs per ENTRY
//! only when entries are not the same width. These are, so finding a row inside an object's run is
//! arithmetic and the only thing needing to be named is where each OBJECT's run starts: eight bytes
//! against a whole run. #2075 measured that rather than arguing it.
//!
//! THE RANGE IS NOT KNOWN WHEN THE BYTES ARE DECODED, which is why the bucketing is a method rather
//! than a constructor. `ShardState::routing_range` answers the whole keyspace until
//! `install_shard_state` stamps it, and on that range every key gets a bucket of its own by construction -- precisely the regime
//! where the amortisation vanishes. So a decoded container is CORRECT but uncompacted, and
//! [`GroupedSeriesIndex::rebucket`] is what compacts it once the range is known. Correct in every
//! state, compact once told the range; there is no invalid state to get wrong.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::block_store::ElementEntry;

/// Where one object's rows sit inside its bucket's run.
///
/// The key is here and NOWHERE ELSE: not in a map key, not in the rows. That is the whole point --
/// a row is 24 bytes of data with no identity in it, and identity is paid once an object.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ObjectRun {
    key: Arc<str>,
    /// First row of this object, as an index into its bucket's `rows`.
    start: u32,
    /// How many rows this object owns.
    len: u32,
}

/// One bucket: its objects in key order, and the single run their rows share.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct BucketRun {
    /// Sorted by key, so an object is found by binary search.
    objects: Vec<ObjectRun>,
    /// Grouped by object in `objects` order, and sorted by timestamp inside each group.
    rows: Vec<(u64, ElementEntry)>,
}

/// What an insert did, so a caller and a guard can both see it without a global counter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct InsertOutcome {
    /// Whether a row for that timestamp was already there and was overwritten.
    pub(super) replaced: bool,
    /// Rows shifted to make room. Zero when the row landed past the end of its bucket, which is
    /// what an ascending timestamp does. Reported rather than inferred, because the write cost of
    /// this shape is the question #2080 and #2081 exist to answer and a guard should read it
    /// directly.
    pub(super) rows_moved: usize,
}

/// A timestamped series map grouped by routing bucket.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct GroupedSeriesIndex {
    buckets: BTreeMap<u32, BucketRun>,
    /// The range `buckets` is keyed on. `(0, u32::MAX)` until told otherwise, which is what a
    /// decoded container has and what `ShardState::routing_range` answers before install.
    start_routing_bucket: u32,
    end_routing_bucket: u32,
}

impl GroupedSeriesIndex {
    /// An empty index on the whole keyspace, which is what an unstamped state routes on.
    pub(super) fn new() -> Self {
        GroupedSeriesIndex {
            buckets: BTreeMap::new(),
            start_routing_bucket: 0,
            end_routing_bucket: u32::MAX,
        }
    }

    /// The range this index is currently grouped on.
    pub(super) fn routing_range(&self) -> (u32, u32) {
        (self.start_routing_bucket, self.end_routing_bucket)
    }

    fn bucket_of(&self, key: &str) -> u32 {
        crate::engine::hashing::block_routing_bucket(
            key,
            self.start_routing_bucket,
            self.end_routing_bucket,
        )
    }

    /// How many OBJECTS the index holds. Named `object_count` rather than `len` on purpose: this
    /// container has two plausible lengths and a bare `len` would be read as whichever the caller
    /// happened to mean. The sibling is [`Self::point_count`].
    pub(super) fn object_count(&self) -> usize {
        self.buckets.values().map(|bucket| bucket.objects.len()).sum()
    }

    /// How many POINTS the index holds, across every object.
    pub(super) fn point_count(&self) -> usize {
        self.buckets.values().map(|bucket| bucket.rows.len()).sum()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.buckets.values().all(|bucket| bucket.objects.is_empty())
    }

    /// Where `key`'s run is: its bucket, and its position in that bucket's `objects`.
    fn locate(&self, key: &str) -> Option<(u32, usize)> {
        let bucket_id = self.bucket_of(key);
        let bucket = self.buckets.get(&bucket_id)?;
        let at = bucket
            .objects
            .binary_search_by(|object| object.key.as_ref().cmp(key))
            .ok()?;
        Some((bucket_id, at))
    }

    pub(super) fn contains_key(&self, key: &str) -> bool {
        self.locate(key).is_some()
    }

    /// One object's rows, as a contiguous slice. The slice IS the storage -- no copy, no
    /// materialised map.
    fn run(&self, key: &str) -> Option<&[(u64, ElementEntry)]> {
        let (bucket_id, at) = self.locate(key)?;
        let bucket = self.buckets.get(&bucket_id)?;
        let object = &bucket.objects[at];
        Some(&bucket.rows[object.start as usize..(object.start + object.len) as usize])
    }

    /// How many points `key` holds, or zero if it holds none.
    pub(super) fn series_len(&self, key: &str) -> usize {
        self.run(key).map(|run| run.len()).unwrap_or(0)
    }

    /// One point, by its timestamp. TWO BINARY SEARCHES AND NO WALK: the objects array for the key,
    /// then the object's own run for the timestamp.
    pub(super) fn get_point(&self, key: &str, at: u64) -> Option<&ElementEntry> {
        let run = self.run(key)?;
        let index = run.binary_search_by(|(existing, _)| existing.cmp(&at)).ok()?;
        Some(&run[index].1)
    }

    /// Every point of one object, in timestamp order.
    pub(super) fn points(&self, key: &str) -> impl Iterator<Item = (u64, &ElementEntry)> + '_ {
        self.run(key)
            .unwrap_or(&[])
            .iter()
            .map(|(at, address)| (*at, address))
    }

    /// One object's points within an inclusive-exclusive timestamp window, in order.
    ///
    /// Bounded by binary search at both ends rather than by filtering the run, so a narrow window
    /// over a long series does not read the series.
    pub(super) fn range(
        &self,
        key: &str,
        start_at: u64,
        end_at: u64,
    ) -> impl Iterator<Item = (u64, &ElementEntry)> + '_ {
        let run = self.run(key).unwrap_or(&[]);
        let from = run.partition_point(|(at, _)| *at < start_at);
        let to = run.partition_point(|(at, _)| *at < end_at);
        run[from..to].iter().map(|(at, address)| (*at, address))
    }

    /// Every object's key, in no particular order across buckets but sorted within each.
    pub(super) fn objects(&self) -> impl Iterator<Item = &str> + '_ {
        self.buckets
            .values()
            .flat_map(|bucket| bucket.objects.iter().map(|object| object.key.as_ref()))
    }

    /// Every object with its points, for the callers that walk the whole map.
    pub(super) fn iter(&self) -> impl Iterator<Item = (&str, &[(u64, ElementEntry)])> + '_ {
        self.buckets.values().flat_map(|bucket| {
            bucket.objects.iter().map(move |object| {
                (
                    object.key.as_ref(),
                    &bucket.rows[object.start as usize..(object.start + object.len) as usize],
                )
            })
        })
    }

    /// Insert or overwrite one point.
    ///
    /// A row lands inside its own object's run, in timestamp order. Everything after it in the
    /// bucket moves, and every later object's start moves with it -- which is why an ASCENDING
    /// timestamp is the cheap case: it lands at the end of its own run, and only objects sorting
    /// after it in the same bucket shift. #2080 measured that at 12.4 rows moved an insert at the
    /// sparsest occupancy falling to 0.0 at the densest; #2081 measured the opposite landing, a left
    /// push, at 499.5 and growing with the key's length, which is why a list map is not given this
    /// shape.
    pub(super) fn insert(&mut self, key: &str, at: u64, address: ElementEntry) -> InsertOutcome {
        let bucket_id = self.bucket_of(key);
        let bucket = self.buckets.entry(bucket_id).or_default();
        match bucket
            .objects
            .binary_search_by(|object| object.key.as_ref().cmp(key))
        {
            Ok(object_at) => {
                let (start, len) = {
                    let object = &bucket.objects[object_at];
                    (object.start as usize, object.len as usize)
                };
                let run = &bucket.rows[start..start + len];
                match run.binary_search_by(|(existing, _)| existing.cmp(&at)) {
                    Ok(inner) => {
                        // Same timestamp: overwrite in place. Nothing moves, and nothing about the
                        // objects array changes.
                        bucket.rows[start + inner].1 = address;
                        InsertOutcome { replaced: true, rows_moved: 0 }
                    }
                    Err(inner) => {
                        let index = start + inner;
                        let moved = bucket.rows.len() - index;
                        bucket.rows.insert(index, (at, address));
                        bucket.objects[object_at].len += 1;
                        for later in bucket.objects[object_at + 1..].iter_mut() {
                            later.start += 1;
                        }
                        InsertOutcome { replaced: false, rows_moved: moved }
                    }
                }
            }
            Err(object_at) => {
                // A new object. Its run begins where the next object's run begins, or at the end.
                let index = bucket
                    .objects
                    .get(object_at)
                    .map(|object| object.start as usize)
                    .unwrap_or(bucket.rows.len());
                let moved = bucket.rows.len() - index;
                bucket.rows.insert(index, (at, address));
                bucket.objects.insert(
                    object_at,
                    ObjectRun { key: Arc::from(key), start: index as u32, len: 1 },
                );
                for later in bucket.objects[object_at + 1..].iter_mut() {
                    later.start += 1;
                }
                InsertOutcome { replaced: false, rows_moved: moved }
            }
        }
    }

    /// Drop one point. Returns whether it was there.
    pub(super) fn remove_point(&mut self, key: &str, at: u64) -> bool {
        let Some((bucket_id, object_at)) = self.locate(key) else {
            return false;
        };
        let bucket = self.buckets.get_mut(&bucket_id).expect("located above");
        let (start, len) = {
            let object = &bucket.objects[object_at];
            (object.start as usize, object.len as usize)
        };
        let run = &bucket.rows[start..start + len];
        let Ok(inner) = run.binary_search_by(|(existing, _)| existing.cmp(&at)) else {
            return false;
        };
        bucket.rows.remove(start + inner);
        bucket.objects[object_at].len -= 1;
        if bucket.objects[object_at].len == 0 {
            bucket.objects.remove(object_at);
        }
        for later in bucket.objects[object_at..].iter_mut() {
            if later.start as usize > start {
                later.start -= 1;
            }
        }
        true
    }

    /// Drop a whole object and every point it holds. Returns whether it was there.
    pub(super) fn remove_object(&mut self, key: &str) -> bool {
        let Some((bucket_id, object_at)) = self.locate(key) else {
            return false;
        };
        let bucket = self.buckets.get_mut(&bucket_id).expect("located above");
        let object = bucket.objects.remove(object_at);
        let start = object.start as usize;
        let len = object.len as usize;
        bucket.rows.drain(start..start + len);
        for later in bucket.objects[object_at..].iter_mut() {
            later.start -= object.len;
        }
        if bucket.objects.is_empty() {
            // A bucket that empties is dropped rather than kept as an empty entry, for the reason
            // the dirty index records about its own sets: an empty container held forever is a node
            // paid for nothing.
            self.buckets.remove(&bucket_id);
        }
        true
    }

    /// Regroup every object onto `start..=end`.
    ///
    /// THIS IS WHAT RIDES INSTALL. A decoded container is grouped on `(0, u32::MAX)`, where every key
    /// has a bucket to itself and the amortisation this shape exists for is absent. The range becomes
    /// known in `install_shard_state`, and this is the one O(n) pass that moves the objects onto it.
    /// Calling it with the range already in force is a no-op beyond the walk.
    pub(super) fn rebucket(&mut self, start_routing_bucket: u32, end_routing_bucket: u32) {
        if (start_routing_bucket, end_routing_bucket) == self.routing_range()
            && !self.buckets.is_empty()
        {
            return;
        }
        let drained = std::mem::take(&mut self.buckets);
        self.start_routing_bucket = start_routing_bucket;
        self.end_routing_bucket = end_routing_bucket;
        // Collect every object once, then rebuild bucket by bucket so each bucket's run is built in
        // one piece rather than grown by insert -- the same reason a decoded B-tree is repacked
        // rather than refilled.
        let mut by_bucket: BTreeMap<u32, Vec<(Arc<str>, Vec<(u64, ElementEntry)>)>> =
            BTreeMap::new();
        for (_, bucket) in drained {
            let BucketRun { objects, rows } = bucket;
            for object in objects {
                let start = object.start as usize;
                let len = object.len as usize;
                let points = rows[start..start + len].to_vec();
                let bucket_id = self.bucket_of(object.key.as_ref());
                by_bucket.entry(bucket_id).or_default().push((object.key, points));
            }
