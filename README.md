# TemporalStore

**A distributed, time-aware serving engine, written in Rust.**
One durable store for the read paths that sit in front of a model or a request: ranked context,
long sequences, aggregated features, frequency caps, and control state.
Apache-2.0 · self-hostable · no vector database · [temporalstore.ai](https://temporalstore.ai)

📊 **Benchmarks & docs — [temporalstore.ai/benchmarks.html](https://temporalstore.ai/benchmarks.html)**

Keys are time-ordered, writes are append-structured, and every workload below is answered from one
temporal index rather than from several stores that each keep their own idea of time. It runs
locally with one Docker command and scales out to a replicated, shared-storage cluster.

**Every figure on this page is measured, and carries its denominator and its conditions** — store
size, routing range, payload size, node count. Each one names what reproduces it, and all of them
were re-measured on `main` for this document except the one that says it was not. Where a limit is
real it is stated plainly, and [What this store is bad at](#what-this-store-is-bad-at) is a section
rather than a footnote.

---

## Is this the store for your problem?

| You are building | This store | Read on |
|---|---|---|
| **Event sequences per entity that grow without bound** — clickstreams, session histories, interaction logs — read as a recent window | Keeps one sorted array per key, so a window read is a binary search plus the points in the window and does **not** grow with the tail. Retains by **count** (10,000 points a series by default), trimmed oldest-first on every write. No per-point age policy. | [§1](#1-long-sequences-per-entity) |
| **Many millions of distinct entities, each with few values** | **39.4 B of resident index per key** at 20,000 keys in one shard, falling from 86.3 B/key at 2,000 as per-bucket rows amortise. A 64 MiB per-shard resident bound is reached at roughly 1.7 M keys. | [§2](#2-high-cardinality-features) |
| **Rollups over a window** — counts, sums, min/max/avg | Exact, recomputed from the points on each read; an exact incremental ladder at 1 s / 1 m / 1 h / 1 d is available for sum-family counters. **No decayed scores, no top-k, no percentiles.** | [§3](#3-aggregated-features) |
| **Plain online feature serving** — point reads at serving latency | Addresses from a DRAM-resident index, bytes from a memory cache tier, disk only on a miss. Reads are cheap and local; writes pay a consensus round trip. Measured 21x apart at p50 on one box. | [§4](#4-online-feature-serving) |
| **Context for an LLM application** under a token budget | Retrieve, rank, and pack to `max_prompt_tokens`; the prompt is charged first and blocks are taken greedily in ranked order, with what did not fit recorded. Ranking is lexical plan score plus cosine over per-node vectors. **No ANN index** — scoring is linear in the candidate set the caller names. | [§5](#5-context-for-llm-applications) |
| **Frequency caps, quotas, pacing, blobs, stream ingestion** | Single-key atomic counters with windowed sums, a multipart blob path with range reads, and batch/stream ingestion with a dead-letter export. | [§6](#6-what-else-the-engine-actually-does) |

Underneath all six: an append-structured block store, no GC pauses, crash-safe reload from its own
persistence, and a format stamp that refuses an image it cannot read rather than guessing.

---

## 1. Long sequences per entity

A series is a **sorted owned array per key**, not a per-key tree: a one-point series costs one
allocation, and both ends of a window are located by binary search, so a read costs
`O(log n)` plus the points inside the window. The cost of a long tail falls on **memory and
retention**, not on the read.

**Windowing.** Every series read names a window. `FeatureQuery` / `SequenceQuery` take
`start_ms`, `end_ms` and a `count`; `SequenceBatchQuery` carries several windows in one request;
`FeatureQueryFiltered` adds `AND`-ed predicates over four fixed row fields.

**Retention is by count, and it is enforced on the write path.** `feature_max_size` defaults to
**10,000 points a series**, trimmed oldest-first on every append, and it is also the default *and
the maximum* read bound — a `count` above it is clamped, so a long window cannot return more than
the retained length. One append is hard-capped at 100,000 points. Whole-key TTL
(`CommonExpire` / `CommonTtl`) is evaluated lazily on read and by a background sweep bounded at
128 in-memory buckets and 8 on-store buckets a round, so expiry never becomes an unbounded scan.

**What the tail costs.** The term that grows with the shard is the resident index. Measured on the
shipped routing range `0..1023`, one shard, single-page records:

| keys in the shard | resident index | per key |
|---:|---:|---:|
| 2,000 | 172,672 B | 86.3 B |
| 8,000 | 404,992 B | 50.6 B |
| 20,000 | 788,224 B | 39.4 B |

*`data_node::tests::evict_live::what_a_default_eviction_threshold_has_to_clear`.*

**Limits.** Retention is by count or by whole-key TTL — there is **no per-point age policy** and no
per-point deadline. Row filters are evaluated *after* decoding each candidate row, so a wide
filtered window pays for every row in it. The only destructive series verbs are a window
overwrite (`FeatureReplace`) and a whole-key delete (`FeatureDelete`).

## 2. High-cardinality features

This is the case where per-key overhead decides whether the design is affordable, so the figure is
given rather than an adjective.

**Resident index per key: 39.4 B at 20,000 keys in one shard** (shipped routing range `0..1023`,
single-page string records). It *falls* with key count — 86.3 B/key at 2,000, 50.6 at 8,000 —
because the per-bucket rows are shared by every key that lands in the bucket. Quoting a per-key
figure without its key count therefore means nothing here.

**The resident bound.** `index_resident_limit_bytes` ships at **64 MiB a shard**. At 39.4 B/key
that bound is reached at roughly **1.7 M keys in one shard**, which is the scale it is a bound at.

**What the routing range buys.** A block's bucket is `start + hash(object_key) % (end - start + 1)`,
so the range sets the modulus and therefore how many blocks share a bucket. On the shipped
`0..1023`, per **routed record**, incompressible values, one shard — two allocator columns, because
the rounding is exactly what a change of container shape moves:

| routed records | `0..1023` asked / charged | whole keyspace asked / charged | blocks a bucket holds (p50 / max) |
|---:|---:|---:|---:|
| 4,000 | 94.8 / 107.0 B/rec | 187.3 / 197.1 B/rec | 4 / 8 |
| 40,000 | **52.8 / 54.0 B/rec** | 186.5 / 198.1 B/rec | 39 / 50 |

*`engine::tests::routing_range_default::both_byte_columns_and_the_allocations_a_record_at_every_candidate_range`,
854 s. "Asked" is what the allocator was asked for; "charged" reads `malloc_usable_size`. The
whole-keyspace column is the arm where every key lands alone in its own bucket by construction.*

**At a matched denominator** — same corpus, same 256 B values, one shard, weighing the whole
resident shard state rather than the index alone — the shipped range costs **316.3 B/key at 8,000
keys (marginal 274.5 B/key)** against **472.6 B/key at 2,000 keys (marginal 468.0 B/key)** on the
whole keyspace. *(`where_the_resident_index_bytes_go_on_both_routing_ranges`.)*

**The per-key figure is a sawtooth, and that is a property of the measurement.** The hash table
behind a per-key map is a step function of the key count, so dividing by keys gives a sawtooth that
doubles the moment the table does: the string table measures **46.9 B/key at 1,792 keys and 93.3
B/key at 1,800** — 1.99x for eight more keys — while the whole resident index steps 414.1 → 460.7
B/key (1.113x), diluted by the per-bucket rows that do not move.
*(`the_per_key_index_figure_is_a_sawtooth`.)*

**Limits.** A store records the routing range it was built on and **refuses** to load under a
different one, and refuses an unstamped store rather than guessing; the range is a decision taken
before the first ingest, not afterwards. Only new stores take the shipped default.

## 3. Aggregated features

`FeatureAggQuery` takes one aggregator over a window: `sum`, `avg`, `min`, `max`, `first`, `last`,
`count`. Values are `i64` throughout, so `avg` is an integer division and there are no float
aggregates. The same shapes exist for control-state counters, where `"at most N in the last H
hours"` is a single round trip.

**By default an aggregate is recomputed from the points in the window on every read** — the window
is ranged, each point decoded, and the aggregator folded over the result. Exact, and linear in the
window.

**Kept current without recomputing: the rollup ladder.** For sum-family counters an optional index
maintains exact per-bucket sums at **1 s → 1 m → 1 h → 1 d**, folded forward on each counter write,
so a wide window reads a few coarse cells plus the sub-second edges instead of every raw bucket. A
fully covered window is **bit-identical** to the raw scan; partial edges fall back to the
authoritative series; the index is derived, never serialized, rebuilt whenever its recorded length
disagrees with the series, and dropped on delete, expiry or reconcile.

**Batching amortises only the fixed part of a record, and the fixed part is small.** An index-log
delta record measures 63.0 B for one item, 55.1 B/item at 8 and 54.3 B/item at 32, with a marginal
**54.00 B/item over 1..8 and 54.08 B/item over 8..32** — three sizes, because two points fit any
line. About 9 B a record is shareable; the rest travels with the item.
*(`index_log::tests::what_an_index_item_costs_per_append_at_three_batch_sizes`.)*

**Limits, and they matter.** The ladder is **off by default**, and on the feature path it serves
only a literal `sum` with no `count`. There is **no decayed score** anywhere in the engine — no
time-decay function and no decay aggregator; recency enters retrieval as a tiebreak, not as a
curve. There is no top-k, percentile, histogram or distinct aggregator on the feature surface, no
multi-aggregate request, and an **unknown aggregator name returns the point count rather than an
error**. On the feature path the ladder's first read after an append re-derives its numeric view
from the whole series, so an alternating append-and-aggregate workload pays a full-series decode
per aggregate.

## 4. Online feature serving

**The tiers.** A point read resolves its address in the **DRAM-resident index** — in-process, no
I/O, no cache call — then fetches bytes from the **memory cache tier**, falling through to a local
**block slab** (or to the log, for a block still resident there) only on a miss. The resident index
holds addresses, never values. A **disk tier** exists but is only switched on where it spans a real
distance: with shared storage the durable copy is on another machine, so a local disk tier closes
milliseconds; with consensus replication the durable copy is this node's own disk, where the tier
would be a third copy of bytes the operating system already serves from the same device, so it is
off. The backend is resolved before the engine is built, and publishes the reason it chose what it
chose. The budget is `TS_CACHE_MEMORY_BYTES` (16 MiB default), and it is the only one: the
resident index named above takes no cache call, so there is no second cache to bound.

**Blocks are packed**, so one window read of adjacent instants typically decodes one block, and a
multi-point read memoizes each block for the duration of the query.

**The read/write asymmetry, measured.** A three-node cluster, consensus write path against replica
reads served from local state, 504 proposals and 44 sampled reads in a 2,018 ms run, one process,
one box (release build, WSL Ubuntu 22.04):

| | p50 | p95 | p99 | max |
|---|---:|---:|---:|---:|
| write, through consensus (504 samples) | **2,652 µs** | 8,144 µs | 12,004 µs | 20,200 µs |
| read, from a replica's local state (44 samples) | **126 µs** | 537 µs | 1,025 µs | 1,291 µs |

**21x apart at p50**, and the reason is structural rather than tunable: a write is replicated to and
committed on a majority before it is acknowledged, while a read takes a lock and applies the command
against the state the node already holds. The same run reported `replication_healthy`, a maximum
replica lag of **0**, a full error budget, **one failover and two membership changes exercised
