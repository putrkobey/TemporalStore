// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Hierarchical server locations.
//!
//! A server's `location` is a plain `String` that the metaserver only ever
//! compares for exact equality. Real deployments write a path into it —
//! `us-east/dc1/az1/rack7` — and the metaserver reads that as one opaque token,
//! which costs correctness in two places.
//!
//! **Replica spread happens at the wrong granularity.** [`build_shards`] refuses
//! to place two replicas of a shard in the same `location`, comparing whole
//! strings. `us-east/dc1/az1/rack1` and `us-east/dc1/az1/rack2` are different
//! strings, so both are accepted — and the shard ends up with two replicas in
//! one availability unit. Losing that unit loses both. The check looks like it
//! is spreading across failure domains while it is only spreading across the
//! deepest one.
//!
//! **A table can only be pinned to exactly one location.** A `preferred_location`
//! is matched by string equality, so "keep this table in dc1" is not
//! expressible: you can name one rack, or nothing.
//!
//! [`Location`] parses the string into its `/`-separated levels, coarsest first,
//! and supplies the two comparisons those cases need:
//!
//! * [`Location::belongs_to`] — prefix containment. A preference naming fewer
//!   levels matches every location beneath it, so `dc1` covers every rack in it.
//! * [`Location::shared_prefix_len`] — how much of a failure domain two
//!   locations have in common, which is what lets replica placement prefer the
//!   *widest* separation available instead of merely a different leaf.
//!
//! A location with no separator has a single level, so a deployment using flat
//! tags (`zone-a`, `zone-b`) behaves exactly as it does today: different strings
//! differ at level 0, and neither is a prefix of the other.
//!
//! This generalises the reference's fixed four-field location
//! (region / datacenter / availability unit / tag) to a path of any depth. The
//! reference compares its last field exactly and the earlier ones only when the
//! pattern sets them; a variable-depth path expresses the same intent as plain
//! prefix matching, and does not force every deployment onto exactly four
//! levels.

use super::*;

/// A parsed server location: `/`-separated levels, coarsest first.
///
/// Empty segments are dropped, so leading, trailing and doubled separators are
/// tolerated — an operator writing `/dc1/az1/` means the same thing as
/// `dc1/az1`.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Location {
    levels: Vec<String>,
}

impl Location {
    /// Parse a location string into its levels.
    pub fn parse(raw: &str) -> Self {
        Self {
            levels: raw
                .split('/')
                .map(str::trim)
                .filter(|level| !level.is_empty())
                .map(str::to_string)
                .collect(),
        }
    }

    /// True when no level was declared at all.
    pub fn is_empty(&self) -> bool {
        self.levels.is_empty()
    }

    /// How many levels this location names.
    pub fn depth(&self) -> usize {
        self.levels.len()
    }

    pub fn levels(&self) -> &[String] {
        &self.levels
    }

    /// The `depth`-level prefix of this location, or the whole thing when it is
    /// shallower. Used to name the failure domain a server sits in at a given
    /// granularity.
    pub fn ancestor(&self, depth: usize) -> Location {
        Location {
            levels: self.levels.iter().take(depth).cloned().collect(),
        }
    }

    /// True when `self` sits at or beneath `pattern`.
    ///
    /// An empty pattern matches everything ("anywhere"), and a pattern naming
    /// fewer levels than `self` matches every location beneath it — so a table
    /// pinned to `dc1` accepts a server in `dc1/az2/rack9`. A pattern deeper
    /// than `self` never matches: `dc1` does not belong to `dc1/az1`.
    pub fn belongs_to(&self, pattern: &Location) -> bool {
        if pattern.levels.len() > self.levels.len() {
            return false;
        }
        pattern
            .levels
            .iter()
            .zip(self.levels.iter())
