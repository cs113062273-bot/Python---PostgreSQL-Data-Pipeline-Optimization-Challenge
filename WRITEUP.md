# Faster, recoverable Expand: parallel recipe evaluation, bulk index/FK builds and server-side jobs

**Repository:** https://github.com/cs113062273-bot/Python---PostgreSQL-Data-Pipeline-Optimization-Challenge
**Submission commit:** listed in the Kaggle Writeup (the final commit of `main`)

Fork of the official repository (base `5dfde9d`). The GUI, SSH architecture, recipes, validations,
counters, manifest and version registration are unchanged in behaviour; the output tables are
row-for-row identical to the original, identity values included.

## 1. Bottlenecks in the original pipeline

Baseline at 1M rows (MEASURED PROCESSING TOTAL 107.5 s; SQL stage 104.4 s):

| Step | Time |
|---|---:|
| table3 / table4 (DISTINCT over the source) | 2.8 s / 2.3 s |
| table1 (1M rows) | 33.5 s |
| table5 / table6 (1M rows each) | 9.8 s / 11.4 s |
| table2 (2.32M rows) | 43.0 s |
| Preflight (32 remote commands) | 2.9 s |

1. **Single-core execution.** `INSERT … SELECT` never uses parallel query in PostgreSQL, so every
   recipe — 91 JSON extractions per source row, casts, `GREATEST`, 14 lateral positions for table2 —
   runs on one core. table2 alone is ~24 CPU-seconds of expression evaluation at 1M.
2. **Per-row foreign-key triggers.** Seven FKs fire for every inserted row (7.3M trigger queries at 1M).
   `table1.id_column_3 → raw_data` also takes a `FOR KEY SHARE` lock per row, which dirties pages of
   the 2 GB source table (random I/O and WAL).
3. **Per-row index maintenance.** Nine unique btree indexes on the destinations are updated row by row;
   for table2 this is more than half of the insert time.
4. **JSON extraction copies.** `raw -> 'values' ->> 'k'` materialises the whole `values` object
   (~1.5 KB, 64 keys) before reading one key.
5. **WAL volume.** 3.1 GB of WAL at 1M, for a 2.6 GB database.
6. **Preflight round trips.** 32 timed remote commands, 25+ of them separate `psql` processes.
7. **No recovery.** The batch is streamed through the SSH channel; if it drops, the work is rolled back.
   Version registration is a second transaction in another database, so a failure between the two
   leaves committed data without versions.

## 2. Optimizations

Everything still runs in the original single `REPEATABLE READ` transaction with the same locks,
row counters, manifest and validations.

**Parallel recipe evaluation.** Each recipe's `SELECT` is evaluated by
`CREATE TEMP TABLE … AS SELECT … ORDER BY …`, which PostgreSQL does run with parallel workers;
the destination is then filled from that table with the original `INSERT` column list (counters
are updated from its row count as before). The sort key reproduces the order in which the original
statement produced its rows, so identity values go to the same rows:

- row recipes: source row position (`ctid`, exposed through the existing scoped source subquery),
  then the output columns (orders the 14 table2 positions within a source row);
- `SELECT DISTINCT`: the output columns, but only when `EXPLAIN` of the original statement inside
  the transaction shows Sort + Unique in select-list order (at 100k the planner hashes instead, and
  the statement then runs unchanged).

Statements whose order cannot be described this way — aggregates, window functions, `GROUP BY`,
`ORDER BY`/`LIMIT`, set operations, whole-row or `*` references to the source, a source used twice —
keep the original single-statement execution. Workers per query = online CPUs − 1, capped by the
server's `max_parallel_workers`; no memory setting is changed.

**Bulk foreign-key validation.** After the destination locks, the validated FKs of the destinations
are dropped and re-created from `pg_get_constraintdef` right before `COMMIT`; each is validated with
one join instead of one trigger query per row. A violation aborts the transaction, as before.

**Bulk index build.** The destinations' btree indexes (PK, UNIQUE, partial unique) are dropped after
the FKs and rebuilt from `pg_get_constraintdef` / `pg_get_indexdef` (with comments) before the FKs are
restored. A duplicate makes the rebuild fail and aborts the transaction, like a failing row insert.
Indexes still needed by a non-deferred FK, partition, clustered, replica-identity and
non-default-tablespace indexes are left alone; only tables the role owns are touched.

**Planner statistics.** A destination read by a later recipe is `ANALYZE`d after it is filled, so
those recipes plan their joins on real row counts (without it the planner hashed the 1M-row source
instead of table1 and spilled 256 batches).

**JSON path collapse.** `col -> 'a' ->> 'b'` on json/jsonb source columns becomes
`(col #>> ARRAY['a','b'])`; chains with numeric-looking keys or tighter-binding neighbours are left
alone. The recipe stored in version history is the original text.

**One-batch preflight.** The same checks run in the same order on outputs fetched by one `psql`
process (plus one for missing counters). Timed remote commands: 32 → 7.

**Tried and dropped:** larger `work_mem` / `maintenance_work_mem` (no gain), a larger identity
sequence cache (−0.3 s on table2, not worth changing sequence options), skipping the sort
(−0.6 s on table2 but identity values would no longer match).

## 3. Results

Same machine (16 cores), PostgreSQL 17.11 defaults, organizer generator, median of three alternating
runs on fresh clones, the report's own measurement model (SQL clocked in PostgreSQL, remote
commands clocked on the server).

| | Original | This version | Change |
|---|---:|---:|---:|
| **1M rows — MEASURED PROCESSING TOTAL** | **107.5 s** | **23.8 s** | **−77.8 %** |
| 1M rows — SQL stage | 104.4 s | 23.0 s | |
| **100k rows — MEASURED PROCESSING TOTAL** | **12.46 s** | **4.38 s** | **−64.9 %** |
| 100k rows — SQL stage | 9.64 s | 3.48 s | |

Per step at 1M (seconds, evaluate + insert):

| Step | Original | This version |
|---|---:|---:|
| table3 / table4 | 2.8 / 2.3 | 0.7 / 0.8 |
| table1 | 33.5 | 2.0 + 1.4 |
| table5 | 9.8 | 0.9 + 1.0 |
| table6 | 11.4 | 0.9 + 1.1 |
| table2 | 43.0 | 4.9 + 3.1 |
| Rebuild indexes | — | 3.3 |
| Re-create and validate foreign keys | — | 1.7 |

Correctness: an output fingerprint (all six tables ordered by identity, row counters, constraint
definitions and validation state, index definitions, sequence positions) is identical for the original
and this version at 100k and 1M rows. Unit tests cover the JSON rewrite, the staging rules and their
fallbacks, the FK/index deferral SQL, the batch structure and the preflight batching.

## 4. Resource usage

| 1M rows | Original | This version |
|---|---:|---:|
| WAL written | 3086 MB | 730 MB |
| Peak temporary disk (temp tables + spill files) | 43 MB | 425 MB |
| Peak private memory, all PostgreSQL processes | 59 MB | 114 MB |
| Database size after the run | 2618.5 MB | 2618.6 MB |

At 100k: WAL 300 → 74 MB, peak temporary disk 0 → 39 MB, peak
memory 36 → 62 MB.

Temporary disk is the evaluated rows of one recipe at a time (each stage table is dropped right after
its insert; the largest is table2) plus the workers' sort spill files; it grows linearly with the
source. Memory grows with the number of parallel workers (each bounded by the unchanged `work_mem`),
not with the data size. No staging in permanent tables, no settings changed outside the transaction.
Extra persistent storage is one journal row per expansion (a few KB).

## 5. Failure recovery

- **Server-side job.** The batch is uploaded to `~/postgres_data_manager_storage/expansion_jobs/<job>/`
  and started with `nohup psql`; the upload is checked for completeness before anything starts.
  The SSH session only waits for the job, so losing it does not affect the transaction. The job is
  identifiable in `pg_stat_activity` (`application_name = pgdm_expand:<job>`).
- **Journal in the same transaction.** The data transaction also inserts
  `public.pgdm_expansion_journal(job_id, request_fingerprint, expansion)` holding everything version
  registration needs. Data and journal commit atomically.
- **Resume.** Every Expand first runs a recovery check: running jobs on that database are awaited;
  committed jobs without versions are registered from the journal. If the interrupted job was the same
  request, the GUI reports it as recovered instead of loading the data twice.
- **Idempotent registration.** The job id is stored in each version's dependency payload and checked
  under the existing advisory locks, so registering a job twice returns the existing versions.
- **Atomic data phase.** Any failure before `COMMIT` (backend killed, PostgreSQL crash, cancel) rolls
  back everything, including the dropped indexes and FKs and the stage tables.

Tested at 100k (each case followed by a normal Expand): client killed mid-job, crash between data
commit and versioning, backend terminated mid-transaction, PostgreSQL immediate shutdown
mid-transaction, cancel during the job, double registration.

## 6. Limitations and trade-offs

- The speedup depends on CPU cores: on a 4-core server each parallel step gets 3 workers plus the
  leader instead of 7 + 1. Inserting, index builds and FK validation stay single-threaded.
- Identity values match the original because the original plans emit rows in source order and the
  generated `raw_data` is stored in `raw_id` order (so the raw_id-ordered merge joins of table5 and
  table2 produce the same order). On a source whose physical order differs from `raw_id`, the
  relational content is the same but identity numbers can be assigned in a different order.
- Dropping FKs and indexes needs `ACCESS EXCLUSIVE` on the destinations and the referenced source,
  so readers of `raw_data` wait while an expansion runs. Deferral requires owning the tables
  (the generator's role does); otherwise those constraints keep per-row checking.
- The data phase stays all-or-nothing: a PostgreSQL crash mid-transaction loses that run. Committed
  chunks would allow partial progress but would expose half-expanded destinations.
- Measured on a Windows lab with PostgreSQL and bash on the same machine, not on the organizers'
  Linux server.

## 7. Reproduction

Standard challenge setup (Linux PostgreSQL + SSH, generator, six SQL files in the official order),
then `pip install -r requirements.txt` and `python main.py` from the repository root.
`tools/run_expansion.py` runs the same service calls without the GUI against any checkout (this one
or the original), and `tools/output_fingerprint.sql` compares outputs.
