# PostgreSQL expansion performance laboratory

The organizer application is preserved in `app/` at
`5dfde9d967425982229b92d6ffe98efcd74612e9`. Its generator and fixed seed are
unchanged. Run `python scripts/verify_upstream.py` to verify all 32 files.

This project contains measured SQL candidates, a six-table equivalence oracle,
strict malformed-input differential fuzzing, and an experimental durable chunk
runner with a recoverable control-database version saga. Research results are
local diagnostics; no official score, winning placement or submission is claimed.
See [experiments](docs/EXPERIMENTS.md), [recovery](docs/RECOVERY.md) and
[competition evidence](docs/COMPETITION_BRIEF.md).

## Install and launch

Python 3.10+ and PostgreSQL 17 are the tested API target. Actual tests used Python
3.11.9 / PostgreSQL 17.11 on Windows 11, i7-12700H, 16 GiB RAM and an NVMe SSD.
The EDB Windows binary has no JIT implementation. Linux/SSH is still required
for the official full workflow.

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements-lock.txt
.venv/Scripts/python.exe scripts/verify_upstream.py
.venv/Scripts/python.exe launch.py --mode baseline
```

`baseline` is the conservative default. `--mode path_table1` and `--mode
path_array` are explicit experiments, restricted to exact organizer recipes.
The original preflight, counters, versioning, UI and dump implementation remain
in use. Rejected record and payload experiments cannot be selected in this GUI
launcher. Chunk recovery is not enabled in the GUI.

For a project-local disposable PostgreSQL server (no system service or global
settings), run `scripts/bootstrap-local.ps1`. It downloads the EDB binary from
the provider linked by PostgreSQL and extracts only server/client components.
The local research cluster uses loopback-only trust authentication and the
dedicated `challenge` role; it contains no production credentials or data.

## Reproduce local SQL diagnostics

Set the connection through libpq environment or `CHALLENGE_DSN`; credentials
are never included in reports. `CHALLENGE_PSQL` selects a client executable.
For the project-local server:

```powershell
$env:PGHOST='127.0.0.1'
$env:PGPORT='55432'
$env:PGUSER='challenge'
$env:CHALLENGE_PSQL=(Resolve-Path .runtime/postgresql/pgsql/bin/psql.exe).Path
.venv/Scripts/python.exe app/generate_database.py --rows 100000 --host 127.0.0.1 --postgres-port 55432 --user challenge --psql $env:CHALLENGE_PSQL
.venv/Scripts/python.exe -m bench.prepare --database lab_100k
.venv/Scripts/python.exe -m bench.run --database lab_100k --repeats 3 --modes baseline path_table1 path_array lazy_keys --output reports/reproduced-100k.json
.venv/Scripts/python.exe -m bench.profile --database lab_100k
```

The generator refuses an existing target. To build another scale, preserve the
existing generated database under another name or use a separate disposable
cluster. Do not point replacement generation or diagnostics at a production DB.
`bench.prepare` clones to a new database and refuses replacement. Destructive
diagnostics require both a `lab_` name and a disposable marker. Benchmark SQL
timings exclude oracle/reference work and are not MEASURED PROCESSING TOTAL.
Exact SQL snapshots, stage times, settings, typed fingerprints and resource
limitations are recorded under `reports/`.

## Functional and recovery tests

```powershell
.venv/Scripts/python.exe -m pytest tests/test_recipes.py -q
$env:PYTHONPATH='app'
.venv/Scripts/python.exe -m unittest discover -s app/tests -v
Remove-Item Env:PYTHONPATH
.venv/Scripts/python.exe -m tests.semantics.fuzz --count 1500 --seed 3011 --modes path_table1 array_values path_array
.venv/Scripts/python.exe -m tests.recovery.run
.venv/Scripts/python.exe -m tests.recovery.tamper
```

Fuzz/recovery create fresh `lab_*` databases, deliberately mutate only those
fixtures, and preserve them for investigation. They are not official workloads.
Recovery tests seed initial clone lineage from an already-generated
`kaggle_challenge` control store. Reports name each fixture and job UUID.

For a full local organizer-service integration, create a fresh clone and run
`python -m bench.service_run --database lab_full_100k`. This executes real
preflight, managed counters and all six version/dependency registrations. It
substitutes local psql for SSH and labels its report accordingly.

For checkpoint measurement, create another fresh clone and run
`python -m bench.checkpoints --database lab_chunks_100k --chunk-rows 25000`.
The job UUID is saved before work; repeat the same command with `--job UUID`
and identical arguments to resume. Full source rehashing and version finalization
costs are included. Partial public outputs and the persistent fence require the
state contract in [RECOVERY.md](docs/RECOVERY.md).

`scripts/benchmark-linux.ps1` invokes the unchanged generator through SSH and
launches the real GUI for the official-path baseline. Enter the Linux/SSH
connection and six recipes in the GUI and export its report. This missing
infrastructure gate cannot be replaced by the local SQL numbers.

Use `scripts/clean-room.ps1 -Ref COMMIT -PythonExe PATH_TO_PYTHON` for a fresh
source archive, locked dependency install and offline regression checks.

## Attribution

Organizer code: Igor Moreira / `igorsiecz/kaggle_postgre_challenge`, Apache-2.0.
Public extraction controls and the version-result capture pattern were informed
by `Agnuxo1/QueryFoundry`, submitted commit
`32c283f32fe35e9b5bd0e538bd6631f305863fb3`, Apache-2.0. Its local results are
calibration targets, not an official ranking. See [research notes](docs/RESEARCH_NOTES.md).
