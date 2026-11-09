# Faster, recoverable Expand: bulk FK validation, single-pass JSON paths and server-side jobs

**Repository:** https://github.com/cs113062273-bot/Python---PostgreSQL-Data-Pipeline-Optimization-Challenge
**Submission commit:** listed in the Kaggle Writeup (the final commit of `main`)

Fork of the official repository (base `5dfde9d`). The GUI, SSH architecture, recipes, validations,
counters, manifest and version registration are unchanged in behaviour; the output tables are
row-for-row identical to the original, identity values included.

## 1. Bottlenecks in the original pipeline

Baseline at 1M rows (MEASURED PROCESSING TOTAL 106.2 s; SQL stage 103.2 s):

| Step | Time |
|---|---:|
| table3 / table4 (DISTINCT over the source) | 2.8 s / 2.3 s |
| table1 (1M rows) | 33.5 s |
| table5 / table6 (1M rows each) | 9.8 s / 11.4 s |
| table2 (2.32M rows) | 43.0 s |
| Preflight (32 remote commands) | 2.9 s |

1. **Per-row foreign-key triggers.** Seven FKs on the destinations fire for every inserted row (7.3M trigger queries at 1M).
   The worst is `table1.id_column_3 → raw_data`: each check probes `raw_data_pkey` and takes a
   `FOR KEY SHARE` lock, which writes the tuple header of a page in the 2 GB source table — random I/O
   plus WAL. Keeping only that one FK costs +21 s on table1 at 1M.
2. **JSON extraction copies.** `raw -> 'values' ->> 'k'` first materialises the whole `values` object
   (~1.5 KB, 64 keys) and then reads one key. The recipes contain 91 such chains (56 in table2 alone),
   all evaluated for every source row.
3. **WAL volume.** The row locks above make the expansion write far more WAL than the rows themselves
   (3.1 GB at 1M, for a 2.6 GB database).
4. **Preflight round trips.** 32 timed remote commands per run, 25+ of them separate `psql` processes
   (start-up, connection, authentication) for one small catalog query each.
5. **No recovery.** The batch is streamed through the SSH channel; if it drops, psql dies and the work is
   rolled back. Version registration is a second transaction in another database, so a failure between
   the two leaves committed data without versions.

## 2. Optimizations

All of them keep the original single `REPEATABLE READ` transaction, locks, row counters, manifest,
and — importantly — the same query plans, so rows are inserted in the same order and receive the
same identity values.

**JSON path collapse (SQL generation, Python).** Before execution, every
`col -> 'a' ->> 'b'` chain on a json/jsonb source column is rewritten to `(col #>> ARRAY['a','b'])`.
For json/jsonb the two forms return the same value whenever no key looks like an array index; chains
with numeric-looking keys, non-plain string literals or neighbouring operators that bind tighter
(`::`, `AT TIME ZONE`, `COLLATE`, arithmetic) are left untouched. A small position-aware lexer does
the rewrite, so comments and literals are never modified. The recipe stored in version history is the
original text.

**Bulk FK validation (PostgreSQL).** Inside the transaction, after the destination locks, the validated
FKs of the destination tables are read from `pg_constraint`, dropped, and re-created with
`pg_get_constraintdef` (and their comments) right before `COMMIT`. `ALTER TABLE … ADD CONSTRAINT`
validates each FK with one join instead of one trigger query per row. A violation raises and the whole
transaction rolls back, exactly like a failing row insert in the original. Only constraints the role can
re-create (owner, `REFERENCES` privilege, not inherited) are deferred.

**One-batch preflight (Python).** The preflight still calls the same checks in the same order, but
their exact `psql -c` commands are answered from a per-thread cache filled by one `psql` process
(output format switched per query with `\pset`, results split by `\echo` markers). Missing row counters
are initialized in one more process with the original reconcile SQL. Any error drops the cache and the
checks run individually. Timed remote commands: 32 → 7.

**Rejected after measurement:** dropping and rebuilding secondary unique indexes (no gain),
`ANALYZE` after each destination and a larger `work_mem` (they change join plans — and therefore the
order in which identity values are assigned — without a reliable speedup), and evaluating the table2
lateral lookups lazily (−30 % on table2, but a different plan and different identity values).

## 3. Results

Same machine, PostgreSQL 17.11 defaults, organizer generator, median of three alternating runs on
fresh clones. The report's own measurement model is used (SQL clocked in PostgreSQL, remote commands
clocked on the server).

| | Original | This version | Change |
|---|---:|---:|---:|
| **1M rows — MEASURED PROCESSING TOTAL** | **106.2 s** | **63.2 s** | **−40.5 %** |
| 1M rows — SQL stage | 103.2 s | 62.3 s | −39.6 % |
| 1M rows — preflight | 2.9 s | 0.5 s | |
| **100k rows — MEASURED PROCESSING TOTAL** | **12.64 s** | **6.97 s** | **−44.9 %** |
| 100k rows — SQL stage | 9.33 s | 6.17 s | −33.9 % |

Per step at 1M: table1 33.5 → 12.5 s, table5 9.8 → 5.3 s, table6 11.4 → 5.4 s, table2 43.0 → 31.6 s,
table3/table4 unchanged (2.8/2.3 s), plus 1.5 s for re-creating and validating the foreign keys.
table1 gains most because its FK to `raw_data` was the expensive one. The preflight times here include
Windows process start-up; on Linux each saved `psql` start is smaller but the count goes from 32 to 7.

Correctness: an output fingerprint (all six tables ordered by identity, row counters, constraint
definitions and validation state, indexes, sequence positions) is identical for the original and this
version at 100k and 1M rows. 21 unit tests cover the rewrite rules, the batch structure and the
preflight batching.

## 4. Resource usage

| 1M rows | Original | This version |
|---|---:|---:|
| WAL written | 3086 MB | 1200 MB (−61 %) |
| Temporary files (`pg_stat_database.temp_bytes`) | 74.7 MB | 74.7 MB |
| Peak private memory of a PostgreSQL backend | 35 MB | 53 MB |
| Database size after the run | 2618.5 MB | 2618.6 MB |

At 100k: WAL 300 → 120 MB, no temporary files in either version, peak backend private memory
16 → 13 MB. The temporary files at 1M come from a multi-batch hash join that both versions plan
identically. No memory setting is changed, so every sort and hash stays within the server's
`work_mem` / `maintenance_work_mem`.

No staging tables, no temporary tables beyond the original's timing/manifest tables, no settings
changes. Extra persistent storage is one journal row per expansion (the request and manifest, a few
KB). The uploaded batch file on the server (tens of KB) is deleted when the job finishes.

## 5. Failure recovery

- **Server-side job.** The batch is uploaded to `~/postgres_data_manager_storage/expansion_jobs/<job>/`
  and started with `nohup psql`; the upload is checked for completeness before anything starts.
  The SSH session only waits for the job, so losing it does not affect the transaction. The job is
  identifiable in `pg_stat_activity` (`application_name = pgdm_expand:<job>`).
- **Journal in the same transaction.** The data transaction also inserts
  `public.pgdm_expansion_journal(job_id, request_fingerprint, expansion)` holding everything version
  registration needs. Data and journal are committed atomically.
- **Resume.** Every Expand first runs a recovery check (merged into the existing metadata round trip):
  running jobs on that database are awaited; committed jobs without versions are registered from the
  journal. If the interrupted job was the same request, the GUI reports it as recovered instead of loading
  the data twice.
- **Idempotent registration.** The job id is stored in each version's dependency payload and checked under
  the existing advisory locks, so registering a job twice returns the existing versions.
- **Atomic data phase.** Any failure before `COMMIT` (backend killed, PostgreSQL crash, cancel) rolls back
  everything, including the dropped FKs; no intermediate artifacts remain.

Tested at 100k (each case followed by a normal Expand): client killed mid-job, crash between data commit
and versioning, backend terminated mid-transaction, PostgreSQL immediate shutdown mid-transaction,
cancel during the job, double registration.

## 6. Limitations and trade-offs

- Dropping FKs needs `ACCESS EXCLUSIVE` on the destinations and the referenced source, so readers of
  `raw_data` wait while an expansion runs.
- Deferral needs ownership of the destination tables (the generator's role has it); otherwise those FKs
  keep per-row checks.
- The data phase stays all-or-nothing: a PostgreSQL crash mid-transaction loses that run. Committed
  chunks would allow partial progress but would expose half-expanded destinations.
- Expression work required by the recipes themselves (casts, `GREATEST`, 14 lateral positions) is
  unchanged; table2 is now about half of the total.
- Measured on a Windows lab with PostgreSQL and bash on the same machine, not on the organizers' Linux
  server.

## 7. Reproduction

Standard challenge setup (Linux PostgreSQL + SSH, generator, six SQL files in the official order), then
`pip install -r requirements.txt` and `python main.py` from the repository root. `tools/run_expansion.py`
runs the same service calls without the GUI against any checkout (this one or the original), and
`tools/output_fingerprint.sql` compares outputs.
