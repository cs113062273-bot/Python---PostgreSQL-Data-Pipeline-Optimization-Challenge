# PostgreSQL Data Manager — faster, recoverable Expand

Submission for the Kaggle **Python & PostgreSQL Data Pipeline Optimization Challenge**.
This repository is a fork of [igorsiecz/kaggle_postgre_challenge](https://github.com/igorsiecz/kaggle_postgre_challenge)
(base commit `5dfde9d967425982229b92d6ffe98efcd74612e9`). Only the Expand pipeline changed;
the application is installed and used exactly as before (Windows GUI → SSH → Linux PostgreSQL).

`git diff da993ac` shows every change against the untouched organizer files.

## What changed

**Execution** — still one `REPEATABLE READ` transaction with the same locks, counters, manifest and
the same query plans, so the six destination tables come out row-for-row identical to the original,
identity values included.

1. **JSON path collapse.** `raw -> 'values' ->> 'k'` copies the whole `values` object (~1.5 KB) before
   reading one key; the six recipes contain 91 such chains, each evaluated for every source row. Chains are rewritten to
   `raw #>> ARRAY['values','k']`, which walks the stored document once. Only chains on json/jsonb source
   columns with non-numeric string keys and unambiguous operator precedence are rewritten
   ([services/expansion_engine.py](services/expansion_engine.py)).
2. **Bulk foreign-key validation.** The original checks every inserted row with an RI trigger
   (an index probe plus a `FOR KEY SHARE` row lock that dirties a `raw_data` page). The destinations'
   validated foreign keys are now dropped inside the transaction and re-created with their original
   definitions right before `COMMIT`; PostgreSQL validates each one with a single join. A violation
   aborts the transaction, as before. Constraints the role cannot re-create keep their triggers.
3. **One-batch preflight.** The source, function and destination checks used to start 25+ separate
   `psql` processes over SSH. Their exact queries now run in one `psql` process (plus one for missing row
   counters); the checks themselves run unchanged on those outputs. Timed remote commands per expansion:
   32 → 7.

**Recovery**

4. **Server-side job.** The SQL batch is uploaded to `~/postgres_data_manager_storage/expansion_jobs/<job>`
   and started with `nohup psql`. A dropped SSH connection, a closed DB Manager or a Windows crash
   no longer aborts the transaction. The job is visible in `pg_stat_activity` as `pgdm_expand:<job>`.
5. **Journal.** The same transaction that writes the data inserts a row into
   `public.pgdm_expansion_journal` (data database) containing everything version registration needs.
   Data and journal commit together or not at all.
6. **Resume on the next Expand.** Before running, Expand waits for any job still running on that
   database and registers the versions of committed-but-unregistered jobs. If the interrupted job was
   the same request, it is not executed again. Registration is idempotent per job id, so a crash
   between the control-database commit and the journal update is also safe.
7. **Cancel** terminates the job's backend; an uncommitted transaction rolls back completely,
   including the dropped foreign keys.

## Results

Local lab: Windows 11, 16 cores, PostgreSQL 17.11 with default settings, organizer generator,
six official recipes. "Remote" commands run through a local bash, so the measurement model is the
application's own: SQL clocked inside PostgreSQL, remote commands clocked on the server.
Each row is the median of three alternating runs on fresh clones of the same generated database.

| | Original | This version | Change |
|---|---:|---:|---:|
| **1M rows — MEASURED PROCESSING TOTAL** | **106.2 s** | **63.2 s** | **−40.5 %** |
| 1M rows — SQL stage | 103.2 s | 62.3 s | −39.6 % |
| 1M rows — preflight | 2.9 s | 0.5 s | |
| **100k rows — MEASURED PROCESSING TOTAL** | **12.64 s** | **6.97 s** | **−44.9 %** |
| 100k rows — SQL stage | 9.33 s | 6.17 s | −33.9 % |
| WAL written, 1M | 3086 MB | 1200 MB | −61 % |
| Temporary files, 1M | 74.7 MB | 74.7 MB | 0 |
| Peak backend private memory, 1M | 35 MB | 53 MB | +18 MB |

Per step at 1M (seconds):

| Step | Original | This version |
|---|---:|---:|
| table3 / table4 | 2.8 / 2.3 | 2.9 / 2.4 |
| table1 | 33.5 | 12.5 |
| table5 / table6 | 9.8 / 11.4 | 5.3 / 5.4 |
| table2 | 43.0 | 31.6 |
| Re-create and validate foreign keys | — | 1.5 |

Output check: [tools/output_fingerprint.sql](tools/output_fingerprint.sql) gives identical results for
the original and this version at 100k and 1M rows (all six tables with identity values, row counters,
constraint definitions and validation state, indexes, sequence positions).

Failure tests (100k, each followed by a normal Expand):

| Interruption | Result |
|---|---|
| Client killed while the job runs | Job commits on the server; next Expand registers its 6 versions without reloading |
| Crash after data commit, before version registration | Next Expand registers the 6 versions from the journal |
| Expansion backend terminated mid-transaction | Nothing committed, all 6 FKs present, counters unchanged |
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

- While an expansion runs, the source table and the destinations are locked `ACCESS EXCLUSIVE`
  (needed to drop foreign keys). Other sessions reading `raw_data` wait until it commits.
- The role must own the destination tables to defer their foreign keys (true for the generator's role);
  otherwise those constraints keep per-row checking.
- The data phase is atomic: a PostgreSQL crash mid-transaction loses that run's work and Expand must be
  started again. Splitting it into committed chunks would break the all-or-nothing visibility of the six
  destinations, which the original guarantees.
- Per-row work that the recipes themselves require (casting, `GREATEST`, 14 lateral positions for table2)
  is unchanged; table2 remains the largest step.

## Changed files

| File | Change |
|---|---|
| [services/expansion_engine.py](services/expansion_engine.py) | New: JSON path rewrite, FK defer/restore SQL |
| [services/postgres_service.py](services/postgres_service.py) | Batch construction, batched preflight, server-side job, journal, recovery, idempotent registration |
| [main.py](main.py) | Runs recovery before a new Expand |
| [tests/test_expansion_engine.py](tests/test_expansion_engine.py) | Rewrite, FK SQL and preflight batch tests |
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
