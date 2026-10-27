# PostgreSQL expansion performance laboratory

The organizer application is preserved in `app/` at
`5dfde9d967425982229b92d6ffe98efcd74612e9`. Its generator and fixed seed are
unchanged. Run `python scripts/verify_upstream.py` to verify all 32 files.

This project contains measured SQL candidates, a six-table equivalence oracle,
strict malformed-input differential fuzzing, and an experimental durable chunk
runner with a recoverable control-database version saga. Research results are
local diagnostics; no official score, winning placement or submission is claimed.
See [current status](docs/STATUS.md), [experiments](docs/EXPERIMENTS.md), [recovery](docs/RECOVERY.md) and
[competition evidence](docs/COMPETITION_BRIEF.md).

Latest fresh 1M recheck after a private-server recovery: **107.542s official
versus 23.898s guarded_copy, 77.78% less complete processing time**, one paired
run, both with exact six-table/counter/version/schema checks. The earlier
three-run cohort below reached 81.88%; that figure is not an every-run guarantee.
The current 85% target remains unachieved. See [current status](docs/STATUS.md).
The original six and two new benchmark databases have since passed 48 full
logical-table comparisons against a newly executed official original. Source,
counters, actual versions and durability/index metadata were also rechecked:
[reproduction commands and verification limits](docs/VERIFIED_RESULTS.md).

Historical 80% research: fresh 1M **complete local-service three-run medians** are
110.524s official versus 20.030s guarded_copy (**81.88% less time**). Every
paired run exceeds 80%, with actual preflight/counters/six versions, COPY I/O
and cleanup, rebuilding and full validation included. Real private-server
crash/retry, dump/restore and the concurrent-writer fallback also passed.
Source and caveats: [current status](docs/STATUS.md). COPY currently supports
only the local transport, the 300M shared stage is not yet bounded, and broader
submission qualification continues. These succeed the older release below.

The current user target is **95% less time** (5.526s at that official median),
which is **not achieved**. Four newer isolated experiments and their negative
results are recorded in [active 95% research](docs/ACTIVE_95.md); unpaired pilots
do not replace the repeated comparison or change the default implementation.

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

`baseline` remains the default. `--mode bulk_fks` is the validated atomic bulk
candidate: keep original recipes and all PK/UNIQUE/CHECK constraints, defer six
proven FK relationships within the data transaction, then rebuild and fully
validate before COMMIT. Its fresh 100k median is 34.30% below the official
original and 12.54% below the exact QueryFoundry control (same cohort).

`--mode guarded_prefix` is the fastest validated local candidate. In a fresh
100k three-run cohort it reduces SQL-stage time from the official 10.300s median
to 4.983s (51.62%), including proof/staging, rebuilding and full FK validation.
The exact QueryFoundry control is 8.700s in that cohort. This is a local SQL
diagnostic, not the official whole-workflow metric. See STATUS.md for scope.

It extracts recipe inputs as TEXT once, proves conservative numeric/calendar
safety plus empty destinations, and retains exact original fallback. In the
proven domain, table2 uses the original freshly computed maximum as a sound
position bound. All original casts, CHECK/NOT NULL protections and final
relationships remain. Added data functions use the unchanged immutable guard.
Native versions include compiler/batch SHA provenance; complete batch SQL is
archived under reports/sql. guarded_normalize is the preceding research mode.

Earlier unguarded bulk_payload / bulk_array / bulk_full / normalization modes
were rejected after malformed-input counterexamples. They remain only in the
laboratory CLI; their faster numbers are not eligible claims. The GUI exposes
baseline, bulk_fks and the two guarded modes; baseline remains the default.

The original preflight, managed counters, versions, UI and dump code remain in
use. `qf_lazy` calls the pinned public source directly and retains known malformed
input differences, so it is a control rather than the safe default. The separate
chunk recovery protocol is not combined with atomic bulk mode or enabled in the
GUI. jsonb_to_record remains a disposable-laboratory experiment.

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
.venv/Scripts/python.exe -m bench.prepare --database lab_template_100k
.venv/Scripts/python.exe -m bench.prepare --database lab_reference_100k
.venv/Scripts/python.exe -m bench.reference --database lab_reference_100k
.venv/Scripts/python.exe -m bench.fresh --template lab_template_100k --reference lab_reference_100k --repeats 3 --modes baseline qf_lazy guarded_prefix --output reports/reproduced-100k.json
.venv/Scripts/python.exe -m bench.profile --database lab_reference_100k --modes baseline qf_lazy hash_array64
```

The generator refuses an existing target. To build another scale, preserve the
existing generated database under another name or use a separate disposable
cluster. Do not point replacement generation or diagnostics at a production DB.
`bench.prepare` clones to a new database and refuses replacement. Every ranked
diagnostic run uses a new clone, with no destination warmup or TRUNCATE first:
TRUNCATE preserves column statistics and can make repeated runs incomparable.
`bench.run` remains a historical same-database diagnostic harness, not the
preferred controlled comparison. Destructive
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
.venv/Scripts/python.exe -m tests.recovery.bulk_atomic --mode guarded_prefix
```

Fuzz/recovery create fresh `lab_*` databases, deliberately mutate only those
fixtures, and preserve them for investigation. They are not official workloads.
Recovery tests seed initial clone lineage from an already-generated
`kaggle_challenge` control store. Reports name each fixture and job UUID.

For a full local organizer-service integration, create a fresh clone and run
`python -m bench.service_run --database lab_full_100k --mode bulk_fks --reference lab_reference_100k`. This executes real
preflight, managed counters and all six version/dependency registrations. It
substitutes local psql for SSH and labels its report accordingly.

For checkpoint measurement, create another fresh clone and run
`python -m bench.checkpoints --database lab_chunks_100k --chunk-rows 25000 --reference lab_reference_100k`.
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


The current fresh harness rotates modes by Latin-square order and uses
PostgreSQL FILE_COPY clones. `--clone-strategy WAL_LOG --order alternating`
reproduces the older clone/order method. Optional `--cpu-affinity 2` pins only
the measured local backend, identically for every mode; it is a lab control,
not part of the submitted architecture. No global OS/durability settings change.
Run `python -m tests.semantics.domain` for sufficient-domain boundaries and
`python -m tests.recovery.bulk_guards` for metadata fallback/refusal cases.


The fresh 1M SQL pilot is 104.529s official / 45.551s guarded_prefix (56.42%
less time). A separate full local original-service pair is 103.046s / 48.355s
wall (53.07% less time), including real preflight, counters and six versions.
Both 1M comparisons are single-run pilots. Read STATUS.md and reports/README.md
for authoritative cohorts and limits; do not mix SQL-only and full-service times.

For native guarded recovery and detailed unranked scale profiles:

```powershell
.venv/Scripts/python.exe -m tests.recovery.bulk_atomic --mode guarded_prefix
.venv/Scripts/python.exe -m tests.recovery.bulk_concurrency
.venv/Scripts/python.exe -m bench.release_profile --template lab_template_1m
```

For the 80% successor, prepare a 1M template/reference using the same pattern
above after untouched generation with `--rows 1000000` on a fresh cluster (or
preserve the existing generated DB first). With those lab databases available:

```powershell
.venv/Scripts/python.exe -m bench.native_fresh --template lab_template_1m --reference lab_1m --modes baseline guarded_copy --repeats 3 --output reports/native-reproduced-1m.json
.venv/Scripts/python.exe -m bench.native_resources --template lab_template_1m --reference lab_1m --mode guarded_copy --output reports/native-resources-reproduced-1m.json
.venv/Scripts/python.exe -m tests.recovery.bulk_concurrency --mode guarded_copy --output reports/native-concurrency-reproduced.json
```

Run timing and resource probes separately with no competing test jobs. Resource
sampling and its explicit stage-size observation are unranked diagnostics.
The server-crash test intentionally stops only the verified project-private
cluster and must run without another active job. See tests/recovery and the
gate-by-gate [audit](docs/RELEASE_80_AUDIT.md) before interpreting results.

The actual crash test stops only the verified private project cluster; run it
without other database work. The dump test preserves the prior database under
a new lab_ name and restores the original name with its control identity.
Their complete commands and results are in RECOVERY.md / reports.
