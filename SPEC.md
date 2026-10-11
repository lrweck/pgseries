# PgSeries — "TimescaleDB lite", works everywhere

**Hypertables, continuous aggregates, columnar compression,
retention.** The familiar time-series surface, on any Postgres 17+.
No C of our own, no `shared_preload_libraries`, no restart.
Apache-2.0.

Single-file install: `\i pgseries.sql`, one transaction. Anywhere
Postgres runs — any provider, any tier, your laptop. No vendor
approval, no support ticket, no parameter group to edit.

If you've used TimescaleDB you know the shape: `create_series_table`,
`add_continuous_aggregate`, `add_compression_policy`, `time_bucket`,
`first`/`last`, `show_chunks`, `drop_chunks`. Same surface, same
mental model. Where pure SQL can't match the C extension (bit-level
codecs, custom planner nodes), PgSeries is honest about it: target
**2–4× compression instead of 10–20×**, BRIN + partition pruning +
`LATERAL` instead of planner hooks. Most of the value, on the
Postgres you can actually deploy on.

PgSeries is the time-series sibling of
[PgQue](https://github.com/NikolayS/PgQue) — same install model,
same Apache-2.0 license, same "anti-extension" posture. PgQue's PgQ
engine is **embedded** for the snapshot/tick semantics that
continuous aggregates and the job scheduler need (see
*Architecture › Embedded PgQue*).

## Goal

PgSeries delivers TimescaleDB-shape time-series ergonomics on any
Postgres 17+ — managed, self-hosted, or laptop — using only pure SQL
and PL/pgSQL. The single-file install gives users hypertables,
continuous aggregates, columnar compression, and retention policies
under the same names and call shapes TimescaleDB users already know,
trading the ratio of C-extension columnstores for the reach of
"runs anywhere Postgres runs".

The full feature surface ships in **baby steps**: v0.1 is just
managed time partitioning; each subsequent release adds one
self-contained capability on top of the previous. The
**10¹⁰-row scale target with 2–4× compression** lands at v0.6 — see
*Release stages* below for the full plan.

## Related work

PgSeries isn't the first attempt to bring time-series ergonomics to
managed Postgres without TimescaleDB's TSL. The closest in spirit
is **[pg_timeseries](https://github.com/ChuckHend/pg_timeseries)**
(PostgreSQL license; originally Tembo, community-maintained after
Tembo's closure) — a real C extension that wraps **Citus columnar**
for compression. If your provider allows installing it,
pg_timeseries will compress harder than PgSeries can; Citus columnar
is a real columnstore, parallel-arrays-in-TOAST is not.

PgSeries trades raw ratio for **reach**. Zero extensions of our own
means it runs on:

- locked-down RDS parameter groups that ban third-party extensions,
- providers that haven't whitelisted pg_timeseries / Citus columnar,
- internal-policy environments that allow only PG built-ins,
- ad-hoc PG clusters where the operator can't restart for
  `shared_preload_libraries`.

The honest pitch: pg_timeseries when you can install it, PgSeries
when you can't (or when the operational simplicity of "it's just
SQL" is itself the win). Both projects exist because the gap they
target is real: TimescaleDB's TSL puts compression and incremental
caggs out of reach for a lot of teams, and PG core doesn't ship a
proper columnstore yet.

## Scale target

**The headline 10¹⁰-row, 2–4×-compressed target lands at v0.6** —
the release that ships columnar compression. Every number below is
derived against that endpoint; see *Release stages* for what each
intermediate version supports (v0.1 alone tops out around the
heap-storage budget the user is willing to pay for, since
compression isn't there yet).

**Per-row footprint matters.** PG's tuple header is 24 B before
any data; add MAXALIGN padding, page-header overhead, free space,
and one secondary B-tree index and a "narrow" row of `(timestamp,
bigint, double)` ≈ 48–60 B + index, while a realistic metric row
with a text tag and 4 doubles ≈ 100–200 B + index. The numbers
below assume **~150 B/row** (data + indexes, append-only) —
typical for the **narrow time-series tables of just a few
columns** that PgSeries targets.

> **Caveat.** All numbers below assume **narrow schemas** (a
> handful of columns) and an **append-only** ingest pattern. They
> **do not include bloat** from sustained `UPDATE`/`DELETE`
> activity, long-running transactions pinning the xmin horizon, or
> autovacuum falling behind. A workload with 30–50% bloat (not
> uncommon under heavy update / hot-row patterns) doubles raw
> footprint. Wider schemas (10+ columns, `jsonb` tags) or extra
> secondary indexes can multiply storage by 2–5×. Treat the
> "Range, schema-dependent" row as a soft band, not a ceiling.

Working back from 10¹⁰ rows, with default 1-day chunks and 1-year
retention:

| | |
|---|---|
| Chunks | **365** (one per day) |
| Rows per chunk | ~**2.7 × 10⁷** (steady state) |
| Per-row footprint | ~**150 B** typical (narrow, few columns, append-only — no bloat budget) |
| Steady-state ingest (avg) | ~**317 rows/sec** at 1-year retention |
| Scale-benchmark ingest (avg) | ~**3 800 rows/sec** at 30-day fill |
| **Peak ingest, single writer** | **≥10 K rows/sec** (narrow row, `synchronous_commit = off` recommended) |
| **Peak ingest, parallel writers** | **≥100 K rows/sec** aggregate on multi-vCPU + local NVMe; **target ≥250 K** with `COPY`/binary protocol |
| Raw heap + indexes | ~**1.5 TiB** at 150 B/row |
| Range, schema-dependent | **~0.7–3 TiB** raw (narrow → wide; pre-bloat) |
| Compressed (2–4×) | ~**400–800 GiB** typical |
| Per-chunk segmentby cap | ~**10⁴** (compression-ratio constraint) |

**Ingest is a first-class concern.** Steady-state averages above
hide bursty real-world ingest. Targets (all on the reference
machine: PG 18, local NVMe, ~128 GiB RAM, modern many-core x86):

- **Single writer**, narrow row, `synchronous_commit = off`,
  prepared statement, batched (~100 rows/INSERT):
  **≥10 K rows/sec**. With single-row INSERTs and sync commit,
  expect ~3–5 K/sec — that's PG's per-connection WAL-fsync
  ceiling, not PgSeries overhead.
- **Parallel writers**, batched INSERT, async commit:
  **≥100 K rows/sec aggregate at 32–64 connections** (≥3–5 K
  per connection × N). 8 connections gets you ~30–50 K
  realistically; you need 32+ to crack 100 K. 16+ vCPU on the
  reference machine.
- **Many connections** (100+) with single-row prepared INSERTs:
  ~**150–300 K rows/sec**. Useful as the "naive ORM at scale"
  baseline.
- **`COPY` / binary protocol**, 4+ streams, batch loads:
  **≥250 K rows/sec aggregate**, **target ≥1 M** at 16+
  streams. This is the "pre-aggregated batch loader" path that
  most TimescaleDB benchmarks lean on.

The per-connection ceiling is the WAL fsync rate of the
underlying NVMe + PG's group commit. PgSeries doesn't change it.
What PgSeries promises is that the **partitioning layer adds
zero detectable overhead** on the hot path — a control benchmark
against a hand-rolled partitioned table without PgSeries must be
within 5 % on every line above.

Compression (v0.6) runs out-of-band against cold chunks and must
not steal ingest throughput from the hot path. Cagg invalidation
triggers (v0.3+) use AFTER STATEMENT with transition tables — one
trigger fire per statement, not per row, so `COPY` of N rows
costs O(1) trigger invocations.

A user running narrow rows + minimal indexes + clean append-only
ingest lands at the bottom of the storage range; a user with a
wider schema, jsonb tags, or update-heavy traffic that bloats the
heap lands at the top — or beyond it.

10⁸–10⁹ rows is the easy case the same code handles without tuning.
Past 10¹⁰ on a single instance, two things break first: cagg refresh
on dense backfills (tunable), and PL/pgSQL compression throughput
per chunk (degrades gracefully to slower, never wrong).

The BRIN + range-partitioning pattern PgSeries leans on for both
ingestion and skipping is **well-established prior art**: operators
routinely run ~10⁹ rows/day on plain partitioned heaps with BRIN
indexes and no compression at all. PgSeries layers the columnar
sibling and view-layer predicate rewrite on top of that proven base
— it doesn't bet on anything PG itself hasn't already shown to
scale.

**Reference machine for the scale benchmark**: single self-hosted
PG 18 instance with **local NVMe** storage and **~128 GiB RAM**.
Local NVMe is deliberate — the benchmark measures what the
architecture can do on real hardware, not what cloud-attached block
storage allows. Smoke tests on managed providers cover the
deployment path separately.

## Release stages

Baby steps. Each version is a tagged release with its own bundle
and acceptance set; each adds one capability. *Architecture*
below describes the v1.0 endpoint with `[v0.x]` tags.

| | Theme | What ships |
|---|---|---|
| **v0.1** | Time partitioning | `create_series_table`, `precreate_chunks`, `move_default`, `show_chunks` / `drop_chunks` (manual), chunk-lease state machine, three roles, **`pgseries.tune()` GUC helper**, BRIN on hot partition's time column by default. External scheduler required. |
| **v0.2** | Retention + scheduler + space partitioning | Embed PgQue. `system_jobs` runner with per-job advisory lock; `add_retention_policy`; `alter_job` / `pause` / `resume` / `run_now`. **`add_dimension(table, col, type, n)` ships here** (pulled from v1.0 — hash sub-partitioning is TimescaleDB's biggest insert-speed lever). **`pgseries-parallel-copy` loader script** (chunk-affinity sharded `COPY` for ≥1 M rows/s aggregate). v0.1 functions become PgQue jobs. |
| **v0.3** | Caggs (materialized) | `add_continuous_aggregate`, `time_bucket()`, `refresh_continuous_aggregate(start, end)`. AFTER STATEMENT triggers + PgQue tick + data-time log. Backdated DML correct. |
| **v0.4** | Real-time aggregation | Partial-form storage; cagg view = `finalize_agg(combine_agg(...))` over materialized + on-the-fly past watermark. v0.3 caggs auto-rebuild. Select-list validation. |
| **v0.5** | Hierarchical + gapfill + first/last | cagg-on-cagg (fixed + calendar buckets), `time_bucket_gapfill()` (single-group-key), `first()` / `last()` with combinefuncs. |
| **v0.6** | Columnar compression | `add_compression_policy`, `_compressed` siblings, FOR + dict + null bitmap + lz4, BRIN `minmax_multi_ops`, LATERAL read view, pre-CHECK'd ATTACH swap, read-only triggers + REVOKE. **Headline 10¹⁰-row scale benchmark blocks here.** |
| **v0.7** | Late-write staging | `add_recompression_policy`; chunk heap reused as staging; modes `staging` (default) / `error`. `ON CONFLICT` on hot path. No `auto_decompress`. |
| **v0.8** | Reorder + observability + hll | `add_reorder_policy` (`CLUSTER`), `hll`, `add_index` (auto-applies to new chunks), full `pgseries_information` surface. |
| **v1.0** | API stability + GA | All v0.x APIs frozen; deprecations dropped; full doc set; the "this is safe to depend on" mark. No new headline feature — all the staged capabilities have already shipped. |

**Beyond v1.0**: DuckDB/Arrow acceleration where extensions are
allowed; pure-SQL t-digest; multi-group gapfill with `locf`/
`interpolate`; remaining hyperfunctions (`counter_agg`,
`time_weight`, ASOF, full `percentile_agg`).

## Architecture

Each piece below describes the **v1.0 endpoint**, with a `[v0.x]`
tag noting the release that introduces it. Detailed design lives
in *Detailed design* further down; this section is the map. See
*Release stages* above for the staged delivery plan.

### 1. Time partitioning — pre-create ahead, default-partition stashes misses [v0.1]

Series tables are native PG range-partitioned tables on the time
column. A PgQue-driven pre-creator job creates partitions ahead of
the write frontier — typically a week ahead at default settings —
so the hot path is just PG's own row-routing, with no PgSeries
code in it at all.

**Misses go to `DEFAULT`, not into a trigger that does DDL.** An
earlier draft of this spec had a `BEFORE INSERT` trigger on the
default partition that created the missing partition mid-statement
and re-routed the row. That doesn't work on real PG: ATTACH
PARTITION conflicts with the parent's running INSERT lock; tuple
routing is resolved before the trigger fires; for a multi-row
`COPY` only the first row could possibly succeed inside the
statement's snapshot. The trigger pattern is gone.

The replacement is dead simple. Misrouted rows land in
`<table>_default` — that's what `DEFAULT` partitions are for. They
remain visible to readers immediately (the parent table's planner
includes the default partition in any scan that doesn't prune it).
A **mover job** running on the PgQue scheduler tick (default 100 ms
cadence) does the work outside the writer's transaction:

1. `select count(*) from <table>_default group by time-bucket` to
   find the ranges that need partitions.
2. For each range, claim a `chunk_lease` row, create the
   partition, attach it.
3. `insert into <table> select * from <table>_default where
   time >= L and time < U; delete ... returning ...` — moves rows
   from default into the freshly attached partition.

Steady-state latency from miss to placement: typically ≤200 ms
(one tick + the move). Readers see the row from commit-time on,
just routed differently after the move. Pre-creation lag is
detectable as `<table>_default` non-empty for >5 s.

State coordination across pre-creation, compression, mover, and
retention goes through `pgseries.chunk_lease` — a row per chunk
range with `state in ('pending', 'ready', 'compressing',
'compressed', 'leased_for_drop')`. Every operation that mutates
chunk state takes `for update skip locked` on the lease row and
transitions through the documented state machine. Retention's
`drop` transition holds the lease *inside* the same transaction
that does `DROP TABLE`, closing the window where compression and
retention could race.

### 2. Continuous aggregates — embedded PgQue tick + data-time invalidation log [v0.3 materialized · v0.4 partial-form · v0.5 hierarchical]

A cagg has two jobs: (a) keep a materialized rollup correct under
a stream of writes (including backdated UPDATE/DELETE), and (b)
serve queries that need the current truth, not just the last
materialized state.

**(a) Materialization correctness.** PgSeries keeps two pieces of
state, both written by triggers:

1. A **PgQue queue** `cagg_invalidation_<cagg>` carrying invalidation
   events. The queue's tick is the watermark — refresh consumes a
   batch only when the batch is closed, so in-flight writes are
   never skipped. PgQ at Skype has been doing this since 2007; PgQue brings the same engine to managed Postgres in pure SQL.
2. A **data-time invalidation log** `pgseries.cagg_invalidation`
   keyed by `(cagg_id, time_bucket)` with `dirty bool`. The log is
   written by the same triggers that produce PgQue events; it's the
   per-bucket "needs recompute" set.

Triggers fire on **`INSERT` or `UPDATE` or `DELETE` (statement-level,
with transition tables)**, plus a separate **`BEFORE TRUNCATE`
statement trigger** (PG requires TRUNCATE triggers be statement-
level and separate from row-DML triggers). For each statement, the
trigger:

- computes the affected time range from `OLD UNION ALL NEW`
  transition tables (covering INSERT, UPDATE-old, UPDATE-new, and
  DELETE),
- emits one PgQue event with `(min_time, max_time)`,
- marks dirty buckets in `pgseries.cagg_invalidation`.

Backdated DML works correctly because the trigger sees the
statement's *data-time* range, not its transaction time. An
UPDATE to a 2022 row produces a 2022 invalidation; the next
refresh recomputes the 2022 bucket.

**Direct-to-leaf-partition INSERTs bypass the parent's statement
triggers.** v0.3 (when caggs ship) closes this loophole with
`revoke insert, update, delete on <every_chunk> from
pgseries_writer`; writes must go through the parent. Foreign-key
cascades that don't fire statement triggers are documented as a
deferred-indefinitely problem (rare in time-series schemas).

**(b) Real-time correctness — partial-form storage, not finalized.**
The materialized rollup stores **partial aggregate state**, not
finalized values. This is the TimescaleDB "finalization form" and
it's the only shape that handles `avg`, `stddev`, `percentile_*`,
and other non-decomposable aggregates correctly across the
materialized/real-time boundary.

The cagg view does:

```sql
select bucket, finalize_agg(combined_state) as result
from (
  select bucket, partial_state as state
  from <cagg>_partials
  union all
  select time_bucket(interval, time) as bucket,
         agg_state(value) as state
  from <source>
  where time >= materialization_watermark
  group by 1
) merged
group by bucket;
```

The outer combine + finalize merges partials from both halves
correctly. Buckets that straddle the watermark are handled by the
combine step; no double-count, no missed rows.

v0.1 rejects in cagg select lists (the `add_continuous_aggregate`
parser checks):

- aggregates without combinefuncs (`array_agg` with `order by`,
  `string_agg` with `order by`, `DISTINCT` aggregates without
  partial form),
- `HAVING`, `GROUPING SETS`/`ROLLUP`/`CUBE`,
- window functions in the cagg select list.

For non-rejected aggregates the view just works. Finalized-only
storage is opt-in via `add_continuous_aggregate(... materialized_only
=> true)` for users who want lower storage cost and accept that
the cagg lags behind by `refresh_interval` with no real-time half.

**Hierarchical caggs (cagg-on-cagg).** A parent cagg can roll up a
child cagg's partials when the parent's bucket boundaries align
with the child's. v0.1 supports both fixed-width buckets
(microsecond … week) and calendar-width buckets (`month`, `year`,
`quarter`) as parent or child, as long as the child's bucket width
divides the parent's. Daily child + monthly parent works. Weekly
child + monthly parent does not (week boundaries don't align with
month boundaries) and is rejected at `add_continuous_aggregate`
time. Earlier drafts had this constraint inverted; the parent
(downstream) bucket is the multiple, not the child.

**Retention vs cagg refresh window.** A `drop_chunks` job will
refuse to drop a chunk whose time range overlaps any registered
cagg's `refresh_lag` window — the user must either widen the
retention or shrink the refresh window before retention proceeds.
The check is part of the `chunk_lease` state-machine transition.

### 3. Compression — columnar siblings, BRIN-skipping, no planner hooks [v0.6]

PgSeries gives compressed read paths and policy-driven roll-up of
cold chunks without being a real columnstore. PG core doesn't ship
one, and pure SQL can't build one — only a C extension (Citus
columnar, Hydra columnar, pg_timeseries which wraps Citus) can give
you true column-major storage with vectorized scans. The pragmatic
workaround used here is parallel typed arrays in TOAST, BRIN-driven
segment skipping, and a view-layer rewrite. It hits 2–4× ratio on
metric-shape data and partition-pruning-class scan latency on cold
data, on any PG. That's the whole pitch — and it's enough for
analytics.

**Two siblings per chunk, not three.** When a chunk gets
compressed, its heap stays around — empty under the steady state
and reused as the staging area for late writes (§4). The compressed
sibling holds the cold data. So at any moment a chunk has exactly
two physical relations:

- `<chunk>` — the heap. Holds the chunk's writes when it's hot
  (uncompressed mode), and any late writes when it's cold
  (staging mode). For cold chunks the heap is typically empty.
- `<chunk>_compressed` — the columnar sibling. Holds compressed
  segments. Empty when the chunk is hot, populated when it's cold.

The read view `union all`s exactly these two relations. Two
branches, not three. Most queries hit one or the other (hot or
cold); the empty branch contributes nothing.

Each compressed row holds N source rows, laid out columnar:

- **segmentby columns** (e.g. `device_id`) — scalar on the
  compressed row. Predicates on segmentby skip whole compressed
  rows without unnesting. **Cardinality trap**: if segmentby
  cardinality approaches segments-per-chunk, segments hold
  ~1 row each, FOR overhead dominates, and ratio collapses to
  ~1.2×. Keep below ~10⁴ per chunk for the 2–4× target;
  `add_compression_policy()` warns at 50 000.
- **orderby columns** (typically `time`) — parallel typed arrays,
  sorted within each segment. `orderby DESC` is supported per
  column at compression time so last-point queries can stop after
  one segment.
- **value columns** — parallel typed arrays.

The pipeline that builds a compressed row from source rows:

1. group by segmentby
2. sort by orderby
3. encode integers/timestamps with frame-of-reference (`min(col)` +
   `int2[]`/`int4[]`/`int8[]` offsets — falls back wider when the
   range overflows)
4. encode low-cardinality text with a per-chunk dictionary
   (`text[]` dict + `int2[]` indices)
5. encode nulls as a per-column bitmap (`bytea`)
6. optional lossy float requantization (opt-in per column)
7. set per-column `compression lz4` explicitly (the cluster
   default may still be `pglz` on managed PG); store as
   `storage extended` so the values land in TOAST and lz4 runs.
8. tune segment row count so the resulting tuple is above the
   TOAST threshold and per-array values land out-of-line where
   compression actually fires

Side stats columns on every compressed row (`seg_min_ts`,
`seg_max_ts`, `seg_row_count`, per-column null counts, segmentby
scalars) plus a BRIN `minmax_multi_ops` index on `(seg_min_ts,
seg_max_ts)` and on segmentby columns give us data skipping
without a custom planner. `minmax_multi_ops` (PG 14+) is required
because the heap's row order on the compressed sibling is
segmentby-then-orderby — the time column's range *interleaves*
across heap rows, not increases monotonically — so per-range
min/max widens to the entire chunk under plain `minmax_ops`.
`minmax_multi_ops` stores multiple disjoint intervals per range
and skips correctly.

Reads go through a view that union-alls heap + compressed.
Predicate pushdown on the compressed branch uses `LATERAL` after
a filtered subquery, never `unnest` in the outer select — the
planner can't push quals through `unnest`. Plan shape is verified
in CI: a pgTAP test asserts partition pruning + BRIN index scan +
zero `Function Scan` on `unpack_segment` for skipped segments.

**Honest ratio.** Target **2–4×** against heap + TOAST on the
mixed-type metric workload TimescaleDB users actually run (tags +
gauges + counters). Per-type breakdown (target):

| Column type | Expected ratio |
|---|---|
| Integer (FOR + lz4) | 5–8× |
| Timestamp (FOR + lz4) | 6–10× |
| Low-cardinality text (dict + lz4) | 10–20× |
| Float64 (lossy requant + FOR + lz4) | 3–5× |
| Float64 (no requant) | 1.5–2.5× |

Real columnstores (Citus columnar, pg_timeseries) will beat these
numbers on installs that allow them. Bit-level codecs (Gorilla,
delta-delta, simple-8b) are out of reach in PL/pgSQL.

**Compression swap.** The swap pre-installs a `CHECK` constraint on
the compressed sibling matching the partition bounds, then does
`detach + attach` in one transaction:

```sql
alter table <chunk_compressed>
  add constraint match_bounds check (time >= L and time < U) not valid;
alter table <chunk_compressed> validate constraint match_bounds;
-- ^ scans the (small) compressed sibling once, off-line of parent

set local lock_timeout = '1s';
begin;
alter table <table> detach partition <chunk>;
-- heap kept; will be reused as the staging sibling
alter table <table> attach partition <chunk_compressed>
  for values from (L) to (U);
-- ATTACH skips its validation scan because match_bounds proves the rows fit
commit;
```

`DETACH PARTITION CONCURRENTLY` is not used (its two-phase
semantics expose mid-swap windows). The swap takes
`AccessExclusive` on the parent for the brief moment; pooled
clients with prepared statements will replan on first use after
commit.

**Post-swap BRIN.** `autosummarize=on` is asynchronous (driven by
autovacuum), so the first range queries after a compression swap
do not benefit from BRIN skipping. The compression job calls
`brin_summarize_new_values()` synchronously after the swap so the
compressed chunk's first read already skips correctly.

**Read-only enforcement** on compressed siblings is **two
triggers**: a `BEFORE INSERT OR UPDATE OR DELETE` row-level trigger
that raises, and a separate `BEFORE TRUNCATE` statement-level
trigger that raises. Plus explicit `revoke` on every compressed
sibling. `CHECK` constraints constrain row values, not writes,
and cannot enforce read-onlyness.

### 4. Late writes — heap stays as the staging sibling [v0.7]

When a chunk is compressed, its heap doesn't go away — it sits
empty and acts as the staging area for any future writes against
the chunk's range. So the same heap that held the data when the
chunk was hot is reused for late writes when the chunk is cold.

This collapses the previous spec's three-mode design (`error` /
`staging` / `auto_decompress`) to two:

- **`staging` (default)**: late writes go to the chunk's heap.
  They're visible to readers immediately (the read view from §3
  union-alls heap + compressed). On the next recompression-policy
  tick, the recompression job merges the heap rows into the
  compressed segments and `truncate`s the heap.
- **`error` (opt-in)**: a `BEFORE INSERT OR UPDATE OR DELETE`
  trigger on the compressed sibling raises; the application
  is expected not to write to compressed ranges. For users who
  want strict immutability after compression.

The `auto_decompress` mode from the previous spec is gone.
Decompressing whole segments on every late write is heavyweight,
the staging-heap approach makes it unnecessary, and the
per-segment advisory-lock complexity (with its 96-bit keyspace
collision concern) goes away with it.

`on conflict` — the unique-index requirement means it always
includes the partition key (`time`), so the constraint is
checkable without reading the compressed sibling. v0.1 *allows*
`on conflict` on writes that target the heap (hot chunks, or late
writes to a cold chunk's heap staging), and *errors* when the
conflict resolution would have to consult compressed segments
(no per-row primary key in compressed form). Earlier drafts
blanket-rejected `on conflict` everywhere, including hot chunks
where TimescaleDB allows it; that was wrong.

### 5. Embedded PgQue — three uses, one engine [v0.2 vendored; cagg + recompression queues land later]

PgQue's pgque-core ships vendored inside the PgSeries install bundle
at `pgseries/_pgque/`. The single-file install
(`\i pgseries.sql`) loads PgQue first, then PgSeries on top. The
schema is `pgseries_pgque` so it doesn't collide with a user's
existing PgQue install in the same database. PgQue is pinned to a
specific tagged release; PgSeries ships its own bundle.

Three places use it:

1. **Cagg invalidation queue** — described in §2 above. The
   batch boundary is the watermark; the data-time invalidation
   log holds the per-bucket dirty set.
2. **Job runner** — `pgseries.system_jobs` is a PgQue queue.
   Every scheduled policy (retention, compression,
   recompression, refresh, reorder, chunk pre-creation, mover)
   emits a "due" event on its schedule and a consumer worker
   processes the next batch. Each job body takes a per-job
   transaction-level advisory lock
   (`pg_advisory_xact_lock(hashtextextended('pgseries.job:'||job_id, 0))`)
   to prevent overlapping runs of the same job — PgQue guarantees
   at-least-once delivery, not at-most-once execution.
3. **Mover signaling** — the default-partition mover (§1) and the
   recompression merge consume PgQue events keyed on chunk_lease
   transitions.

Why vendor instead of depend: PgSeries has to give a single-file,
single-transaction install on any provider. Asking users to
install pgque first turns one transaction into two and one
namespace into a coordination problem. Vendoring is what PgQue does
internally with its three-table layout already; we're applying the
same trick one layer up.

### 6. Space partitioning — pulled forward to v0.2 [catalog v0.1 · API v0.2]

Every real time-series schema we've seen has a non-temporal axis —
`tenant_id`, `region`, `customer_id`, `device_id` — that needs to
participate in pruning and parallel write distribution. **It's
also the single biggest insert-speed lever.** TimescaleDB's
parallel-copy benchmarks hit 1 M rows/sec because hash sub-
partitions remove the index/lock hotspot on the current time-range
chunk. Without it, a single time-range chunk is the write
bottleneck and per-conn throughput plateaus quickly.

The catalog reserves the shape from v0.1; `add_dimension` ships at
**v0.2**. `pgseries.dimension` holds one row per dimension on
every series table, with `dimension_type in ('time', 'space_hash',
'space_range')` and `number_partitions` for space dimensions. v0.1
only writes `dimension_type = 'time'` rows; v0.2's
`pgseries.add_dimension(table, column, type, n)` flips the catalog
to multi-dimensional and re-partitions in place (or at table
create time, no migration needed).

`pgseries.chunk` carries dimension-coordinate columns
(`time_lower`, `time_upper`, `space_partition_index`) so chunk
identity is a tuple, not a single time range. v0.1 chunks all have
`space_partition_index = 0`; v0.2+ chunks fan out across N hash
sub-partitions per time range. Code paths that select chunks
(compression policy, retention, cagg refresh, `drop_chunks`,
`show_chunks`) accept a chunk *set*, not a range, so each
later-version feature picks up multi-dim without rework.

### 7. Job scheduling [v0.2]

There is no PgSeries-specific scheduler. PgQue's ticker model from
§5 is the scheduler. Three deployment shapes work out of the box:

- `pg_cron` calling `pgque.ticker_loop()` (PgQue's default).
- `pg_timetable` calling the same.
- An external driver (system cron, systemd timer, an app worker)
  calling `select pgque.ticker()` at any cadence ≥ 1 s.

Job bodies (compress chunk, refresh cagg, drop chunks, …) are
executed by a consumer of `pgseries.system_jobs`. Retry,
exponential backoff, and DLQ semantics are PgQue's, not ours.
Per-job advisory locks (above) prevent overlapping runs.
`pgseries.alter_job` / `pause_job` / `resume_job` /
`run_job_now` are thin wrappers over PgQue's queue operations.
Schedules are **fixed-cadence** by default (target time advances
in fixed steps independent of run latency); `floating_schedule =>
true` opts into next-run = last-finish + interval.

## Detailed design

The subsections below expand on the architecture above in the same
order: series tables, retention, continuous aggregates, compression
pipeline, BRIN data skipping, the read view, the compression swap,
write paths on compressed chunks, time helpers, aggregates, the
operational surface, migration, and roles. Anything load-bearing
ends up with an acceptance criterion below.

### Series tables

Native PG range partitioning by time. A `DEFAULT` partition exists
on every series table. The pre-creator runs as a PgQue job
(`pgseries.system_jobs` event type `precreate_chunks`) and creates
N partitions ahead of the write frontier (default `pre_create_n =
7` for daily chunks). The hot path involves no PgSeries code and
no triggers on any leaf partition or on the parent.

The `mover` job (event type `move_default`) runs on the same
PgQue tick. It scans `<table>_default` for the time-bucket of the
oldest row, claims the corresponding `chunk_lease`, creates the
partition, attaches it, and moves the rows out of default in a
single `with moved as (delete from <table>_default where time >= L
and time < U returning *) insert into <table> select * from moved`.

`SECURITY DEFINER` ownership: pre-creator and mover functions are
owned by `pgseries_admin`, so created chunks inherit
`pgseries_admin` ownership uniformly. Writers get table-level
grants via `pgseries_writer` membership on the parent. No writer
ever owns a chunk.

`chunk_lease` state machine:

```
pending  → ready              (pre-creator: partition created, attached)
ready    → compressing        (compression policy claims)
compressing → compressed      (compression swap committed)
compressed  → compressing     (recompression: merge staging back in)
ready    → leased_for_drop    (retention claims, holds across DROP)
compressed → leased_for_drop  (retention claims, holds across DROP)
```

Every transition is a `for update skip locked` claim on the lease
row inside the same transaction that does the corresponding DDL.
No transition leaves the lease in an intermediate state visible
to a concurrent worker.

Catalog: `pgseries.series_table`, `pgseries.chunk`,
`pgseries.chunk_lease`, `pgseries.dimension`.

### Retention

A scheduled `drop_chunks` job consumes from `pgseries.system_jobs`
and drops chunks with `time_upper < now() - retention_period`,
*subject to the cagg-refresh-window constraint above*. The job
checks every registered cagg's effective refresh window
(`max(now() - refresh_lag - end_offset)`) and refuses to drop a
chunk whose range overlaps any of them, leaving the chunk for the
next retention tick after the cagg has caught up.

### Continuous aggregates — implementation details

Triggers (one set per source table):

```sql
create trigger <src>_cagg_inv_iud
  after insert or update or delete on <src>
  referencing old table as old new table as new
  for each statement execute function pgseries.cagg_emit_invalidation();

create trigger <src>_cagg_inv_truncate
  before truncate on <src>
  for each statement execute function pgseries.cagg_emit_truncate();
```

`cagg_emit_invalidation()` reads `OLD UNION ALL NEW` transition
tables, computes `(min(time), max(time))`, emits one event per
registered cagg into its PgQue queue, and `insert ... on conflict do
update set dirty = true` into `pgseries.cagg_invalidation` for every
bucket the range overlaps.

Refresh consumer:

```
loop:
  batch := pgque.next_batch(queue)
  for each event in batch:
    union the event's (min_time, max_time) into refresh_range
  bucket_set := dirty buckets in pgseries.cagg_invalidation
                that overlap refresh_range
  for bucket b in bucket_set:
    insert/update <cagg>_partials for b from <src>
    set dirty = false in cagg_invalidation
  pgque.finish_batch(batch)
```

The watermark is `pg_xact_commit_timestamp(pgque.batch_end_xmin)
- safety_margin` for the real-time half (§2). Both halves use
PgQue's xid8 batch boundary as the source of truth; the timestamp is
only used to constrain the source-side scan.

### Compression — pipeline details

Per-column rules in pipeline order:

| Stage | Operation |
|---|---|
| group | `group by` segmentby columns |
| sort | `order by` orderby columns within each segment |
| FOR (int) | `min` + `int2[]`/`int4[]`/`int8[]` offsets, widest needed |
| FOR (timestamp) | same, on `epoch_microseconds` |
| dict (text) | `text[]` dictionary per chunk, `int2[]` indices on rows |
| nulls | `bytea` bitmap, one bit per source row per nullable column |
| float (lossy, opt-in) | quantize to N significant digits before FOR |
| compression | `alter column … set compression lz4` per array column (cluster default may be pglz) |
| storage | `alter column … set storage extended` so arrays go out-of-line |
| segment row count | tuned so the per-row tuple exceeds TOAST threshold |

`add_compression_policy()` warns when a chunk's distinct
segmentby count exceeds `segmentby_warn_threshold` (default
50 000); the practical cap for the 2–4× target is ~10⁴.

### Read view

```sql
create view <table> as
  select * from <table>_uncompressed_branch  -- the chunk heap
  union all
  select c.unpacked.*
  from (
    select compressed_row
    from <table>_compressed_branch
    where seg_max_ts >= $a and seg_min_ts <= $b
      -- segmentby predicates pushed here too
  ) f,
  lateral pgseries.unpack_segment(f.compressed_row) as c(unpacked);
```

Two branches: the chunk heap (acts as both hot storage and cold-
chunk staging area; usually empty for cold chunks) and the
compressed sibling. The filtered subquery is the load-bearing
shape: it lets quals land on the BRIN/partition scan before the
lateral unnest fires.

CI runs a pgTAP plan-shape test on a representative range query
and asserts: parent partition pruning, BRIN index choice on the
compressed branch, and zero `Function Scan` on `unpack_segment`
for partitions whose `(seg_min_ts, seg_max_ts)` doesn't overlap
the query.

`pg_class.reltuples` on each compressed sibling is updated
explicitly by `analyze` after recompression; without that, the
planner mis-estimates the union and may pick the wrong join order.
Documented as a v0.1 acceptance criterion.

### Write path on compressed chunks

Per `add_compression_policy(... write_mode => …)`: `staging`
(default) or `error`. See §4.

### Time helpers

`time_bucket()` and `time_bucket_gapfill()` in pure SQL. Fixed-
width buckets (microsecond … week) and calendar-width buckets
(`month`, `quarter`, `year`) are both supported. Cagg-on-cagg
alignment is enforced at `add_continuous_aggregate` time. Gapfill
is single-group-key in v0.1; multi-group + `locf`/`interpolate`
deferred.

### Aggregates

`first(value, time)` and `last(value, time)` as pure-SQL custom
aggregates with composite `(value, time)` state and combinefuncs
(required for partial-form cagg storage). `parallel safe` after
combinefunc verification. Last-point queries on compressed data
benefit from `orderby desc` so unpack stops after one segment.

### Operational surface

`pgseries_information` schema:

- `jobs`, `job_stats`, `job_errors` — system_jobs queue state
- `chunks`, `chunks_detailed_size`, `compression_settings`,
  `compressed_chunk_stats`, `hypertable_size` — capacity-planning
  surface (TimescaleDB users will recognize the names)
- `continuous_aggregates`, `cagg_lag` — per-cagg
  `(materialization_watermark, latest_invalidation_time,
  lag_seconds, dirty_buckets, out_of_window_writes)`
- `default_partition_lag` — non-zero rows in any series table's
  default partition for >5 s show up here

Job control: `alter_job`, `pause_job`, `resume_job`, `run_job_now`.
Continuous-aggregate control: `add_continuous_aggregate` (with
`materialized_only`, `with_data`, refresh-window args),
`refresh_continuous_aggregate(cagg, window_start, window_end)` for
manual backfill.

Index propagation: `pgseries.add_index(table, definition)` creates
the index on every existing chunk and registers it for new chunks
created by the pre-creator. Plain `CREATE INDEX ON <table>` on the
parent works too (PG cascades to leaves) but doesn't auto-apply
to chunks created after the fact; documented.

### Migration

- `pgseries.adopt_partitioned_table(regclass)` — adopt an existing
  range-partitioned table without data movement.
- From a hypertable: per-chunk parallel `pg_dump` is the preferred
  path. Hypertable chunks are individually `pg_dump`-able. The
  migration script (`scripts/migrate-from-hypertable.sh`) dumps
  chunk *data* via `--data-only --table=<chunk>` and rebuilds the
  schema (constraints, indexes, partial-index where clauses,
  generated columns, default expressions, tablespace, reloptions)
  from the source hypertable's catalog. Compressed-chunk internal
  tables and dimension slices are walked via the TimescaleDB
  catalog views and re-expressed as PgSeries chunk_lease rows.
- Fallback: `COPY` into a fresh series table (loses per-chunk
  metadata).
- From `pg_timeseries`: tables are plain partitioned PG tables;
  `pgseries.adopt_partitioned_table()` works in place once the
  Citus columnar columns are decompressed back to heap.

### Roles

`pgseries_reader`, `pgseries_writer`, `pgseries_admin`. All
`SECURITY DEFINER` functions pin
`set search_path = pgseries, pg_catalog`. Functions that create
chunks, compress, decompress, or merge staging are owned by
`pgseries_admin` so chunk ownership is uniform.

## Feature mapping (for TimescaleDB users)

A reference for readers familiar with TimescaleDB's surface, so
you can tell at a glance what maps to what. This is not a parity
goal — the real commitments are in *Architecture* and
*Non-goals*.

### Supported (with the version each lands in)

| TimescaleDB | PgSeries | Ships in |
|---|---|---|
| Hypertables | Series tables (native PG range partitioning + DEFAULT-partition stash + mover) | **v0.1** |
| Chunks | Same — one PG partition per chunk | **v0.1** |
| `show_chunks` / `drop_chunks` | Same names; manual in v0.1, scheduled in v0.2 | **v0.1** |
| Roles | `pgseries_reader`, `pgseries_writer`, `pgseries_admin` | **v0.1** |
| Retention policies | `add_retention_policy` (refuses chunks overlapping cagg refresh windows from v0.3 onward) | **v0.2** |
| Job scheduler | PgQue ticker (`pg_cron`, `pg_timetable`, or external driver); per-job advisory lock | **v0.2** |
| Job control | `alter_job`, `pause_job`, `resume_job`, `run_job_now`, retry-with-backoff, DLQ | **v0.2** |
| Continuous aggregates | `add_continuous_aggregate` over an embedded PgQue queue + data-time invalidation log | **v0.3** |
| `time_bucket()` | Pure SQL, full-width + calendar parity | **v0.3** |
| `refresh_continuous_aggregate(cagg, start, end)` | Same name | **v0.3** |
| Manual cagg backfill (`with_data => false` + later refresh) | Same | **v0.3** |
| Real-time aggregation | Cagg view returns `finalize_agg(combine_agg(…))` over union of materialized partials + on-the-fly partials past the watermark | **v0.4** |
| Hierarchical caggs (cagg-on-cagg) | Fixed-width and calendar-width buckets; child width must divide parent's | **v0.5** |
| `time_bucket_gapfill()` | Pure SQL, single-group-key | **v0.5** |
| `first()` / `last()` | Pure-SQL custom aggregates with combinefuncs (work in partial-form caggs) | **v0.5** |
| Compression policies | `add_compression_policy` | **v0.6** |
| Columnar compression | Sibling `_compressed` table; segmentby + orderby; FOR + dict + null bitmap + lz4. Target 2–4× | **v0.6** |
| Decompression API | `decompress_chunk` | **v0.6** |
| Chunk exclusion / data skipping | Partition pruning + BRIN `minmax_multi_ops` + view-layer pushdown via `LATERAL` | **v0.6** |
| Recompression | `add_recompression_policy` — separate scheduled policy that merges staging back into compressed segments | **v0.7** |
| `ts_insert_blocker` | `error`-mode write trigger (default is staging-via-heap, no blocker) | **v0.7** |
| Reorder / clustering policies | `CLUSTER` on chunks via PgQue schedule | **v0.8** |
| `timescaledb_information` views | `pgseries_information.{jobs, job_stats, job_errors, chunks, chunks_detailed_size, hypertable_size, compression_settings, compressed_chunk_stats, continuous_aggregates, cagg_lag, default_partition_lag}` (subset shipped earlier; full surface by v0.8) | **v0.8** |
| `pgseries.add_index(table, definition)` | Like `CREATE INDEX ON <hypertable>` but auto-applies to future chunks | **v0.8** |
| `add_dimension` (space partitioning) | Catalog-ready since v0.1; API ships at v0.2 | **v0.2** |

### Partial or lossy

| TimescaleDB | PgSeries |
|---|---|
| Hyperfunctions (`percentile_agg`, `histogram`, `counter_agg`, `time_weight`, ASOF, `locf`, `interpolate`) | Use PG built-ins (`percentile_cont`, `width_bucket`) and `DISTINCT ON` instead. Hand-rolled PL/pgSQL versions are too slow to ship. |
| Approximation sketches | `hll` ships in v0.8 (broadly available on managed PG); `tdigest` not on the managed-PG happy path; pure-SQL t-digest is post-v1.0 conditional. |
| Multi-group `time_bucket_gapfill` | Single-group-key only in v0.1. |
| Lossless float compression | Gorilla unavailable in PL/pgSQL; v0.1 ships only opt-in lossy requantization. Floats compress 1.5–2.5× without it. |
| Cagg select-list shapes | Aggregates without combinefuncs, `HAVING`, `GROUPING SETS`/`ROLLUP`/`CUBE`, and window functions are rejected by `add_continuous_aggregate()`. Wrap the cagg view in a regular view to apply them at query time. |
| `auto_decompress` write mode | Removed. Default is staging-via-heap; recompression policy merges back. The TimescaleDB direction since 2.11. |
| Compression ratio | Target **2–4×** on mixed metric workloads (vs Timescale's 8–15× with Gorilla; vs pg_timeseries' Citus-columnar numbers). |
| Direct-to-leaf-partition writes | Revoked at v0.1 to keep statement triggers honest. Writes must go through the parent. |

### Out of scope (never, or beyond v1.0)

| TimescaleDB | PgSeries |
|---|---|
| Bit-level codecs (Gorilla, delta-delta, simple-8b) | Never in pure SQL. Per-value PL/pgSQL overhead inverts the economics. |
| Custom planner/executor nodes (vectorized scan, chunk-exclusion hooks, constraint-aware parallel append) | Never without C. Replaced (imperfectly) by partition pruning + BRIN + `LATERAL`. |
| True columnar storage | Real columnstores (Citus columnar via pg_timeseries, Hydra) require an extension. PgSeries' parallel-arrays-in-TOAST is the pragmatic workaround. |
| Distributed hypertables | Deprecated upstream as of TimescaleDB 2.14. |
| Tiered storage to object storage | Requires custom storage hooks. |
| Promscale-style ingest caggs | Requires the bypassed insert path. |
| Hypertable → series_table in-place converter | A per-chunk parallel `pg_dump` migration script is the only path. |
| FK cascades into source tables | Statement triggers don't see FK-cascaded changes. Rare in time-series; deferred indefinitely. |
| Multi-group `time_bucket_gapfill` with `locf`/`interpolate` | Beyond v1.0. |
| Full hyperfunction parity (`counter_agg`, `time_weight`, ASOF, `percentile_agg`) | Beyond v1.0; `hll` ships in v0.8 as a starting point. |

## Non-goals

PgSeries declines to compete with C-extension columnstores on raw
ratio or scan speed, with TimescaleDB on its own deepest features,
and with itself on going past 10¹⁰ rows in a single series table
on a single instance without the `add_dimension` escape hatch
shipping in v1.0. Specific lines:

- Match TimescaleDB-class **8–15×** compression ratios on metric
  workloads. Target 2–4× (at v0.6+). Bit-level codecs are not
  feasible in PL/pgSQL.
- Beat `pg_timeseries` / Citus columnar on raw compression ratio
  or vectorized-scan latency. Pure SQL can't; that's not where
  PgSeries competes.
- Match TimescaleDB scan latency on compressed data. Target:
  queries filter through BRIN + partition pruning before
  unnesting; that's enough for analytics, not for sub-millisecond
  point lookups.
- Scale a single series table past 10¹⁰ rows without
  `add_dimension` (v1.0).
- Ship every capability in one big release. The staged plan in
  *Release stages* is the design choice; users opt in to the
  capability set they need at the version they need it.
- Support PG 16 or older. PgSeries leans on PG 17 partitioning
  ergonomics (`MERGE`, `MERGE/SPLIT PARTITION` opportunistically)
  for simpler code.
- Allow direct-to-leaf-partition writes in v0.1 (statement triggers
  wouldn't see them).

## Acceptance criteria

Each version is its own release-blocker set. A version doesn't
ship until every item below for that version (and every prior
version) is green on PG 17 and PG 18.

### v0.1 — Time partitioning
- **Install.** `\i pgseries.sql` installs cleanly on stock PG 17
  and PG 18 in a single transaction. Plain SQL only at this
  point — no PgQue, no embedded extensions.
- **Create + adopt.** `pgseries.create_series_table()` produces a
  range-partitioned table with the right structure;
  `pgseries.adopt_partitioned_table()` succeeds on a hand-rolled
  range-partitioned table without data movement.
- **Pre-creation.** `pgseries.precreate_chunks(table, n)` creates
  N chunks ahead of the write frontier; idempotent on re-run.
- **Default-partition catch.** Pre-creation deliberately disabled;
  inserts arrive against ranges with no partition; calling
  `pgseries.move_default(table)` creates the missing partitions
  and moves rows out of `…_default` with zero rows left behind.
  Multi-row `COPY` and `INSERT … SELECT` both round-trip correctly
  (the test the previous design would have failed).
- **Chunk lease state machine.** Direct invariant tests cover
  every documented transition; no half-states reachable.
- **BRIN on hot heap by default.** `create_series_table()`
  installs a BRIN index on the time column of every new chunk.
  Verified that range queries hit BRIN, not an unwanted B-tree.
- **`pgseries.tune()` print mode.** Returns the recommended GUC
  bundle (`wal_compression`, `max_wal_size`,
  `checkpoint_timeout`, `wal_buffers`,
  `effective_io_concurrency`, per-table
  `synchronous_commit=off`) as a result set. Apply mode is
  v0.2.
- **Ingest benchmark, time-only partitioning** (sub-partitions
  ship at v0.2 — without them the per-time-range chunk is the
  hot spot). On the reference machine (PG 18, local NVMe,
  ~128 GiB RAM, 16+ vCPU):
  - ≥**10 K rows/sec** single writer (narrow row, prepared,
    batched ~100 rows/INSERT, `synchronous_commit = off`).
  - ≥**100 K rows/sec aggregate** with **32–64 parallel
    writers** (batched INSERT, async commit).
  - ≥**250 K rows/sec aggregate** via `COPY` from 4+ streams.

  Pre-creation kept ≥7 days ahead throughout the run; nothing in
  `…_default` at end. PgSeries is **not** allowed to be the
  bottleneck — a control run against a hand-rolled partitioned
  table without PgSeries must be within 5 % on the same hardware.
- **Roles.** `pgseries_writer` cannot read another tenant's
  `chunk_lease` rows; `pgseries_reader` cannot mutate any catalog;
  `pgseries_admin` covers both.

### v0.2 — Retention + scheduler + space partitioning
- **Bundle install.** `\i pgseries.sql` now ships pgque-core in a
  `pgseries_pgque` schema, single transaction.
- **Smoke test.** RDS, Aurora, Cloud SQL, Supabase, Neon, Crunchy
  Bridge with PG 17+; 10⁶ rows ingested; retention drops the
  expected chunks. Verified under `pg_cron` and under PgQue's
  ticker pattern from an external driver.
- **Job overlap prevention.** Two ticker drivers running
  concurrently never run the same job body twice in parallel
  (per-job advisory lock test).
- **Chunk lease vs retention.** Retention against an actively
  compressing chunk waits or skips; never deadlocks. (Compression
  doesn't ship yet, but the lease contract is exercised with a
  simulated `compressing` state.)
- **`add_dimension` upgrade.** A v0.1-created series table accepts
  `pgseries.add_dimension(table, 'tenant_id', 'space_hash', 16)`
  and re-partitions in place without rewriting source rows. New
  chunks fan out across 16 hash sub-partitions per time range.
- **Multi-dim chunk identity.** `show_chunks`, `drop_chunks`,
  retention, and the (still-simulated) compression / cagg paths
  all work on the new tuple-keyed chunk set without API breakage.
- **Parallel-COPY ingest benchmark (the new headline for v0.2).**
  On the reference machine with 16 hash sub-partitions and the
  shipped `pgseries-parallel-copy` script:
  - **≥500 K rows/sec** at 8 streams,
  - **target ≥1 M rows/sec** at 16+ streams.
  Single-conn batched INSERT also re-tested: ≥10 K/sec stays;
  parallel batched INSERT at 32–64 conns now hits **≥250 K
  rows/sec** (vs the v0.1 ~100 K ceiling without sub-partitions).
  Control vs hand-rolled `PARTITION BY RANGE … PARTITION BY HASH`
  must come within 5 %.
- **`pgseries.tune()` recommendations.** Print-only mode shows
  `wal_compression=on`, `max_wal_size=16 GB`,
  `checkpoint_timeout=15 min`, `checkpoint_completion_target=0.9`,
  `wal_buffers=64 MB`, `effective_io_concurrency=200`, plus the
  per-table `synchronous_commit=off` recommendation. Apply mode
  (`apply => true`) issues `ALTER SYSTEM` on installs that allow
  it.

### v0.3 — Continuous aggregates (materialized)
- **Cagg correctness — straight path.** A scheduled `avg` cagg
  matches a manual `select avg(...) group by time_bucket(...)`
  against the same source after refresh.
- **Backdated-DML test.** An UPDATE to a 2022-bucketed row
  re-materializes the 2022 bucket on the next refresh tick (the
  regression check the previous spec missed).
- **Out-of-window writes.** Writes outside `refresh_lag` appear
  in `pgseries_information.cagg_lag`; users can see what's stale.
- **Trigger / DDL safety.** AFTER STATEMENT triggers fire under
  `COPY` and bulk `INSERT … SELECT`. Direct-to-leaf-partition
  INSERTs are revoked at this version.

### v0.4 — Real-time aggregation
- **Aggregate correctness across watermark.** `avg`, `stddev`,
  `percentile_cont` on a stream straddling the materialization
  watermark match a direct source query within numeric tolerance.
- **Cagg select-list rejection.** Aggregates without combinefuncs,
  `HAVING`, `GROUPING SETS`/`ROLLUP`/`CUBE`, and window functions
  are rejected at `add_continuous_aggregate()` time with a clear
  error pointing at the wrap-it-in-a-regular-view workaround.
- **v0.3 → v0.4 upgrade.** Existing materialized caggs are
  rebuilt to partial form on first refresh after upgrade; opt-out
  via `materialized_only => true`.

### v0.5 — Hierarchical caggs + helpers
- **Cagg-on-cagg.** Daily-child + monthly-parent works; weekly-
  child + monthly-parent is rejected at create time with a clear
  alignment-error message.
- **Calendar-bucket parents.** Monthly and yearly parents work as
  long as the child width divides the parent's calendar boundary.
- **Gapfill.** `time_bucket_gapfill()` returns rows for empty
  buckets in single-group-key queries; multi-group documented as
  v0.6+ deferred.
- **first/last under combine.** `first(value, time)` and
  `last(value, time)` work correctly in partial-form caggs
  (combinefunc verified by property-based test).

### v0.6 — Columnar compression (the headline)
- **Plan-shape regression test.** pgTAP asserts partition pruning
  + BRIN `minmax_multi_ops` segment skipping + zero
  `Function Scan` on `unpack_segment` for skipped partitions.
- **Stats hygiene.** After a compression swap,
  `pg_class.reltuples` on the compressed sibling matches
  `seg_row_count` sum within 1% before any user query.
- **Compression swap.** Pre-installed `CHECK` constraint lets
  `ATTACH PARTITION` skip its validation scan; `lock_timeout`
  retry test under contention; no orphaned partitions, no
  half-detached state.
- **Read-only enforcement.** Two triggers (row-level + statement-
  level for TRUNCATE) raise on writes to compressed siblings;
  `revoke` denies before the trigger fires.
- **Compression benchmark.** NYC taxi + devops-metrics: ratio and
  scan latency on PgSeries vs uncompressed PG vs `pg_timeseries`
  (Citus columnar). Per-column-type breakdown.
- **Ingest doesn't degrade.** Repeat the v0.1 ingest benchmark
  (≥10 K rows/sec single writer, ≥100 K aggregate parallel,
  ≥250 K via `COPY`) with an `add_compression_policy` running in
  the background against cold chunks. The compression worker
  takes its own segment-by-segment cost; **the hot ingest path
  must stay within 5 % of the v0.1 number**. If it doesn't,
  compression scheduling needs tuning before release.
- **Scale benchmark (the headline).** Single self-hosted PG 18,
  **10¹⁰ rows** over 30 days. Reference hardware: local NVMe,
  ~128 GiB RAM, modern many-core x86. Reported numbers: ingest
  p50/p95, cagg refresh p95, compression throughput per chunk,
  on-disk size, p95 query latency.

### v0.7 — Late-write staging
- **Compressed write path.**
  - `error` mode raises with the documented message.
  - `staging` (default) round-trips row count and aggregate
    values through the recompression merge.
- **`ON CONFLICT`** works on the heap branch; errors only when
  conflict resolution would need compressed segments.

### v0.8 — Reorder + observability
- **`add_index` propagation.** New chunks created after
  `pgseries.add_index()` automatically get the index.
- **Info-view parity.** A user with TimescaleDB muscle memory can
  find `chunks_detailed_size`, `hypertable_size`,
  `compression_settings`, `compressed_chunk_stats`, `cagg_lag`,
  `default_partition_lag`, `jobs`, `job_stats`, `job_errors` at
  the documented names and produce the same capacity-planning
  reports.
- **`hll`** integrates inside a partial-form cagg without
  combinefunc errors.

### v1.0 — API stability + GA
- **Frozen APIs.** Every public function from v0.1 through v0.8
  is type-locked (signature + return shape + side effects). The
  v0.x deprecation shims are removed.
- **No breaking changes since v0.8.** Confirmed by a contract-
  test suite that runs the v0.8 example workload against v1.0
  unchanged.
- **Doc set complete.** Every public function has a man-style
  reference page; the migration guide covers v0.x → v1.0 in one
  page; the perf playbook includes the `pgseries.tune()` output
  + `pgseries-parallel-copy` recipe + tuning trade-offs.

### Always (every release)
- Red/green TDD for `pgseries-api`; `pgseries-core` covered by
  regression tests against PG 17 and PG 18.
- `samospec brief --ai pgseries` regenerates without inventions
  flagged by the verifier on the version's published SPEC.md.

## Open questions

Decisions still in flight, grouped by the version that needs them
resolved. Each is tracked separately so a release isn't blocked
on a question that doesn't apply to it.

### Open for v0.1
- Table abstraction name: `series_table` (default) vs `hypertable`
  (trademark check) vs other.
- DEFAULT-partition mover lag SLA. Target ≤200 ms p95 from insert
  to placement assumes a tight scheduler tick, which v0.1 doesn't
  bundle. Should v0.1 document a lower SLA (e.g. ≤2 s p95 with
  default `pg_cron` 1-minute cadence) and let v0.2 raise it?
- Default chunk size: time-based (1 day), row-based (~3 × 10⁷), or
  adaptive? At the 10¹⁰-row target (v0.6) the two converge on
  ~1 day; v0.1 picks the default.

### Open for v0.2
- Embedded-PgQue schema collision: is `pgseries_pgque` distinctive
  enough, or do we want a per-database prefix?
- `pgseries_information` view names: mirror existing time-series
  conventions exactly (familiarity) or differentiate?

### Open for v0.4
- `materialized_only` cagg storage: when (if ever) is it the
  better default? v0.3 default is materialized-only; v0.4 flips
  to partial-form. Document the storage-cost trade-off.
- Cagg select-list shapes: which of `HAVING` / `ROLLUP` / window
  functions can be lifted in v0.5+ with a specified recombination
  rule?

### Open for v0.6
- Segment row count default. 1 000 to start; revisit after
  benchmarks.
- Lossy float requantization: opt-in per column at table creation,
  or per compression policy?

### Open for v0.2
- `add_dimension` API shape: hash partitions only, or hash +
  range? List partitioning is rare in time-series and probably
  out of scope.
- `pgseries.tune()` apply mode: which GUCs to refuse on managed
  PG (some are gated even for `pgseries_admin`)?
- `pgseries-parallel-copy` — bundled as a Python script, a Bash
  wrapper around `psql \\copy`, or an embedded SQL function
  using `dblink` for parallelism?

### Open for v1.0
- Which deprecation shims to drop (the deprecation list itself
  builds up across v0.x).

### Beyond v1.0
- DuckDB / Apache-Arrow as opt-in acceleration on installs that
  allow extensions. The pure-SQL pipeline is the compatibility
  floor; a fast-path reader could consume the same compressed
  siblings through DuckDB for vectorized scans. The compression
  layout (parallel typed arrays + BRIN sidecars) is intentionally
  chosen so an Arrow-shaped reader can map onto it later.