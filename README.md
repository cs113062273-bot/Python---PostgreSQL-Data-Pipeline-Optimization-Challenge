# PostgreSQL Data Manager — faster, recoverable Expand

Submission for the Kaggle **Python & PostgreSQL Data Pipeline Optimization Challenge**.
This repository is a fork of [igorsiecz/kaggle_postgre_challenge](https://github.com/igorsiecz/kaggle_postgre_challenge)
(base commit `5dfde9d967425982229b92d6ffe98efcd74612e9`). Only the Expand pipeline changed;
the application is installed and used exactly as before (Windows GUI → SSH → Linux PostgreSQL).

`git diff da993ac` shows every change against the untouched organizer files.

## What changed

**Execution** — still one `REPEATABLE READ` transaction with the same locks, counters, manifest and
validations. The six destination tables come out row-for-row identical to the original, identity
values included.

1. **Parallel recipe evaluation and bulk load.** PostgreSQL never parallelizes `INSERT … SELECT`,
   so the original ran every recipe on one core and inserted row by row. Each recipe's `SELECT` now
   runs as a parallel ordered query whose rows are written with psql `\copy … TO` a binary file in
   the job directory and loaded with `\copy destination (columns) FROM` it (COPY writes pages in
   batches). The sort key is the order the original statement produced rows in (source row
   position, then output columns; for `SELECT DISTINCT` the output columns, used only when `EXPLAIN`
   of the original shows Sort + Unique), so every row gets the same identity value. COPY is used
   only when each output already has the destination column's type; otherwise the rows go through a
   parallel `CREATE TEMP TABLE … AS` and `INSERT … SELECT`, or, for untyped literals and for recipes
   with aggregates, window functions, `GROUP BY`, `ORDER BY`/`LIMIT`, set operations or whole-row
   source references, the original statement runs unchanged
   ([services/expansion_engine.py](services/expansion_engine.py)).
2. **Bulk index build.** The destinations' btree indexes are dropped inside the transaction and rebuilt
   from their catalog definitions before `COMMIT`, instead of being maintained row by row.
   A duplicate fails the rebuild and aborts the transaction, as a failing insert did.
3. **Bulk foreign-key validation.** Validated FKs are dropped and re-created from
   `pg_get_constraintdef` before `COMMIT`; PostgreSQL validates each with one join instead of a
   trigger query (and a `FOR KEY SHARE` lock on `raw_data`) per row.
4. **Planner statistics.** A destination that later recipes read is `ANALYZE`d once it is filled.
5. **JSON path collapse.** `raw -> 'values' ->> 'k'` (copies the ~1.5 KB `values` object first) becomes
   `raw #>> ARRAY['values','k']`.
6. **One-batch preflight.** The source, function and destination checks run unchanged on outputs
   fetched by one `psql` process instead of 25+. Timed remote commands per expansion: 32 → 7.

**Recovery**

7. **Server-side job.** The SQL batch is uploaded to `~/postgres_data_manager_storage/expansion_jobs/<job>`
   and started with `nohup psql`. A dropped SSH connection, a closed DB Manager or a Windows crash
   no longer aborts the transaction. The job is visible in `pg_stat_activity` as `pgdm_expand:<job>`.
8. **Journal.** The same transaction that writes the data inserts a row into
   `public.pgdm_expansion_journal` (data database) containing everything version registration needs.
   Data and journal commit together or not at all.
9. **Resume on the next Expand.** Before running, Expand waits for any job still running on that
   database and registers the versions of committed-but-unregistered jobs. If the interrupted job was
   the same request, it is not executed again. Registration is idempotent per job id.
10. **Cancel** terminates the job's backend; the uncommitted transaction rolls back completely,
    including the dropped indexes and foreign keys.

## Results

Local lab: Windows 11, 16 cores, PostgreSQL 17.11 with default settings, organizer generator,
six official recipes. "Remote" commands run through a local bash, so the measurement model is the
application's own: SQL clocked inside PostgreSQL, remote commands clocked on the server.
Each row is the median of three runs on fresh clones of the same generated database.

| | Original | This version | Change |
|---|---:|---:|---:|
| **1M rows — MEASURED PROCESSING TOTAL** | **107.5 s** | **22.9 s** | **−78.7 %** |
| 1M rows — SQL stage | 104.4 s | 22.1 s | |
| **100k rows — MEASURED PROCESSING TOTAL** | **12.46 s** | **4.33 s** | **−65.2 %** |
| 100k rows — SQL stage | 9.64 s | 3.34 s | |
| WAL written, 1M | 3086 MB | 473 MB | |
| Peak temporary disk, 1M (spill files, temp tables, job directory) | 43 MB | 205 MB | |
| Peak private memory of all PostgreSQL processes, 1M | 59 MB | 106 MB | |

Per step at 1M (seconds; this version: evaluate + load):

| Step | Original | This version |
|---|---:|---:|
| table3 / table4 | 2.8 / 2.3 | 0.7 / 0.8 |
| table1 | 33.5 | 2.3 + 1.0 |
| table5 | 9.8 | 1.0 + 0.6 |
| table6 | 11.4 | 1.1 + 0.5 |
| table2 | 43.0 | 5.7 + 2.2 |
| Rebuild indexes | — | 3.4 |
| Re-create and validate foreign keys | — | 1.8 |

Output check: [tools/output_fingerprint.sql](tools/output_fingerprint.sql) gives identical results for
the original and this version at 100k and 1M rows (all six tables with identity values, row counters,
constraint definitions and validation state, indexes, sequence positions).

Failure tests (100k, each followed by a normal Expand):

| Interruption | Result |
|---|---|
| Client killed while the job runs | Job commits on the server; next Expand registers its 6 versions without reloading |
| Crash after data commit, before version registration | Next Expand registers the 6 versions from the journal |
| Expansion backend terminated mid-transaction | Nothing committed; all FKs and indexes present, counters unchanged |
| PostgreSQL immediate shutdown mid-transaction | Same as above after restart |
| User cancel during the job | Same as above |
| Version registration repeated for one job | Returns the existing versions, no duplicates |

## Reproduce

Set up the Linux server, role and dataset exactly as in the official instructions
([GENERATE_DATABASE_README.md](GENERATE_DATABASE_README.md)), then on Windows:

```powershell
git clone <this repository>
cd <repository>
python -m pip install -r requirements.txt
python main.py
```

Connect, open `kaggle_challenge` → `raw_data`, click **Expand**, configure the six SQL files in the
official order and run. The Expansion Performance Report shows `MEASURED PROCESSING TOTAL`.

Without the GUI, the same service calls can be run from a terminal:

```powershell
$env:PG_KAGGLE_CHALLENGE_PASS = "<postgres password>"
$env:SSH_PASSWORD = "<ssh password>"
python tools/run_expansion.py --ssh <linux host> --ssh-user <ssh user> --user <postgres role>
python tools/run_expansion.py --app <checkout of the original repository> --ssh ...   # baseline
```

Compare outputs after each run with `psql -d kaggle_challenge -XAt -f tools/output_fingerprint.sql`.
Unit tests: `python -m pytest tests`.

## Trade-offs and limits

- Speed depends on CPU cores: parallel steps use online CPUs − 1 workers (capped by the server's
  `max_parallel_workers`). Inserts, index builds and FK validation remain single-threaded.
- Temporary disk holds one recipe's binary COPY file at a time in the job directory (table2 is the
  largest) plus sort spill files, and grows linearly with the source.
- Identity values match because the original plans emit rows in source order and the generated
  `raw_data` is stored in `raw_id` order. On a source stored in another order the rows and their
  relationships are the same, but identity numbers may be assigned in a different order.
- While an expansion runs, the source table and the destinations are locked `ACCESS EXCLUSIVE`
  (needed to drop foreign keys). The role must own the destination tables to defer their foreign
  keys and indexes (true for the generator's role).
- The data phase is atomic: a PostgreSQL crash mid-transaction loses that run's work and Expand must be
  started again. Splitting it into committed chunks would break the all-or-nothing visibility of the six
  destinations, which the original guarantees.

## Changed files

| File | Change |
|---|---|
| [services/expansion_engine.py](services/expansion_engine.py) | New: JSON path rewrite, parallel evaluation and COPY load, FK and index defer/restore SQL |
| [services/postgres_service.py](services/postgres_service.py) | Batch construction, batched preflight, server-side job, journal, recovery, idempotent registration |
| [main.py](main.py) | Runs recovery before a new Expand |
| [tests/test_expansion_engine.py](tests/test_expansion_engine.py) | Rewrite, staging, FK/index SQL and preflight batch tests |
| [tests/test_expansion_timing.py](tests/test_expansion_timing.py) | Adapted to the job-based execution |
| [tools/](tools/) | Headless runner and output fingerprint |

---

# Application documentation (upstream)

PostgreSQL Data Manager is a desktop application for browsing and administering PostgreSQL databases through an SSH connection. It includes table previews, imports, version history, controlled destructive actions, data-dictionary tooling, dumps, restores, and role management.

## PostgreSQL setup

The application uses two different PostgreSQL objects:

- Control database: `postgres_data_manager` — the application creates it on the first connection when the PostgreSQL role has `CREATEDB`.
- Control schema: `app_control` — the application creates this automatically inside the control database, together with its internal tables.

For automatic setup, grant the PostgreSQL role permission to create databases once:

```sql
ALTER ROLE your_app_role CREATEDB;
```

If automatic creation is not allowed in your environment, run the following manually as a PostgreSQL administrator, replacing `your_app_role` with the PostgreSQL username entered in the connection window:

```sql
CREATE DATABASE postgres_data_manager OWNER your_app_role;
```

If the database already exists under another owner, grant the application role access instead:

```sql
GRANT CONNECT, CREATE ON DATABASE postgres_data_manager TO your_app_role;
```

The role must be able to connect to `postgres_data_manager` and create the `app_control` schema. The same `CREATEDB` permission also enables the optional database-creation button in the application.

The SSH server must provide the PostgreSQL command-line tools used by the application, including `psql`, `pg_dump`, and `pg_restore`.

## Configuration

Defaults can be overridden without changing the source code:

| Environment variable | Default | Purpose |
|---|---|---|
| `PDM_CONTROL_DB` | `postgres_data_manager` | Control database name |
| `PDM_CONTROL_SCHEMA` | `app_control` | Internal control schema |
| `PDM_LEGACY_CONTROL_SCHEMA` | `app_control_legacy` | Optional legacy schema inspected during migration |
| `PDM_REMOTE_STORAGE_DIR` | `postgres_data_manager_storage` | Storage directory under the SSH user's home |
| `PDM_LOCAL_STORAGE_DIR` | `PostgreSQL Data Manager` | Local application-data directory |
| `PDM_APP_TITLE` | `PostgreSQL Data Manager` | Window title |

Changing these variables does not rename existing PostgreSQL objects. Migrate or rename existing databases and schemas before pointing the application at new names.

## Local installation

Python 3.10 or newer is recommended.

```bash
python -m pip install -r requirements.txt
python main.py
```

On Windows, remembered credentials are encrypted for the current Windows user with DPAPI and stored under the configured local application-data directory.

## Synthetic challenge database

[`generate_database.py`](./generate_database.py) creates the deterministic `kaggle_challenge` database used to exercise the Manager with synthetic data. See [GENERATE_DATABASE_README.md](./GENERATE_DATABASE_README.md) for the Linux server setup, SSH tunnel command, resume behavior, generated metadata, and the six expansion recipes in [`SQL files`](./SQL%20files/).

## Expansion performance report

After a successful cross-table **Expand** operation, the application displays a detailed English-language performance report. It includes preparation and preflight work, destination locks, source-manifest scanning, every destination statement, affected-row counts, version registration, commits, cache invalidation, and interface refresh.

Local work is measured with per-thread CPU time. Remote command work is clocked on the Linux server, and expansion statements are clocked inside PostgreSQL. SSH connection and result-return transit are therefore excluded from the measured processing total. The report can be exported as a UTF-8 `.txt` file.

## Main dependencies

- CustomTkinter
- Paramiko
- pandas
- ReportLab
- openpyxl and xlrd for Excel imports

## License

Licensed under the [Apache License 2.0](./LICENSE).
