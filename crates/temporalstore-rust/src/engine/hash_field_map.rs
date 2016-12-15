// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! THE FIELD MAP OF ONE HASH, SIZED TO WHAT IT HOLDS.
//!
//! `ShardState` declares seventeen nested model maps whose inner container is an ORDERED map and,
//! until this type, exactly one -- `hashes` -- whose inner container was a HASHED table. This is
//! that one, and it is a sorted vector rather than either.
//!
//! WHY NOT A TABLE, WHICH IS WHAT IT WAS. A hashed table rounds its bucket count up to a power of
//! two and carries the rounding as resident slack, so its cost is a step function of occupancy and
//! its smallest step is already large. Measured on this crate's counting allocator, at ONE field --
//! which is what the product write path produces, see below -- the table took 272 chunk bytes to
//! hold 48 bytes of payload. A vector of one entry took 64.
//!
//! WHY NOT AN ORDERED MAP, WHICH IS WHAT THE OTHER SEVENTEEN USE AND WHAT CONSISTENCY WOULD SAY.
//! Because consistency is not the measurement, and the measurement refutes it: a `BTreeMap` node is
//! a fixed-width block sized for eleven entries whether it holds eleven or one, and at one field it
//! took 560 chunk bytes -- MORE THAN TWICE the table it would have replaced, and 8.75x the vector.
//! It is the worst of the three shapes at every occupancy below eight. That candidate was measured
//! and lost; `engine::tests::model_map_container_cost` carries the table.
//!
//! WHY THE OCCUPANCY IS ONE, AND WHY THAT IS A PROPERTY OF THE WRITE PATH AND NOT OF A FIXTURE.
//! `write_context_node` in `execute_on_shard.rs` is the one producer of a context-node page, and it
//! files that page under the single constant field `CONTEXT_NODE_FIELD` (`"meta"`). Seven readers
//! spell the same constant back. So every context node in a store is a hash of exactly one field,
//! and measured at 4,000 and at 40,000 nodes the fields-per-hash histogram was p50 = p90 = p99 =
//! MAX = 1 with a single distinct occupancy, 100% of keys, at both sizes. The only producer of a
//! wider hash is the Redis-compatible `HashSet`/`HashMultiSet` surface.
//!
//! THE SHAPE IS THEREFORE BIMODAL IN A REAL STORE that uses both, and a bimodal histogram is a
//! warning rather than a licence -- the mixed corpus measured a mean of 1.98 fields per hash over a
//! population containing NO hash holding two, which is the same trap a mean of 1.98 pages per
//! bucket set in this engine before. So the verdict was required to hold at BOTH arms, and it does:
//! the vector is cheaper than the table at every occupancy measured from 1 to 256, by 76.5% at one
//! field and by 51.4% at sixty-four.
//!
//! WHAT IT COSTS, STATED RATHER THAN HIDDEN. An insert is O(n): a binary search for the slot and a
//! memmove of the tail. At one field that is one comparison and no move. At the wide arm it is a
//! shift of a 40-byte entry per displaced field, no allocation, and it replaces a hash of the field
//! name. A lookup is a binary search, which at one field is ONE comparison and ZERO hashes against
//! the table's one comparison AND one hash -- so at the occupancy the product path writes, the
//! lookup is strictly cheaper too, counted rather than timed.
//!
//! ORDER. The entries are kept sorted by field name, so iteration is ordered where the table's was
//! arbitrary. Nothing depended on the arbitrary order -- `HashGetAll` and `HashLen` do not read
//! this map at all, they resolve through `bucket_index_component_block_addresses`, which sorts its
//! own result -- and making the order defined can only remove a source of run-to-run difference.
//!
//! THE WIRE FORMAT IS UNCHANGED, BY CONSTRUCTION AND BY TEST, AND THAT IS WHY `hashes` COULD BECOME
//! DURABLE WITHOUT A FORMAT STAMP. This type deserializes from and serializes to exactly the MAP
//! shape the `HashMap` used, through `serde(from/into)`, so no snapshot, manifest or index-log
//! encoding moves a byte. `hashes` is written now rather than skipped; because the shape it writes
//! is the shape the field always had, an index written by an older binary still decodes here -- the
//! field is absent, `#[serde(default)]` supplies an empty map, and the reconcile's merge fills it
//! from the bucket index exactly as it did before -- and an index written here still decodes in an
//! older binary, which ignores the field and rebuilds it from the index as it always did. So
//! `SHARD_INDEX_FORMAT_VERSION` does not move: there is no encoding a reader could misread.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::block_store::ElementEntry;

/// The field name as this map holds it: a SHARED handle, not an owned copy.
///
/// Measured in `what_the_one_constant_field_name_costs_at_the_shape_the_product_writes`: the
/// product write path files every context node under the single constant `CONTEXT_NODE_FIELD`,
/// so an owned name is that constant copied once per object -- 32.0 bytes and one allocation
/// per field at the narrow arm, 40.0 bytes and one allocation at the wide one. The handles come
/// from `bucket_index.kind_pool`, which already interned exactly these names for the bucket
/// index's component position.
