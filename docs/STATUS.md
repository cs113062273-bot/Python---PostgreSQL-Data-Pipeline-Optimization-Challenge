# Current status

No reliable performance win over QueryFoundry and no official score is claimed.
The generator, application guards, counters and version/dump code are preserved.
The default GUI launcher remains baseline; experiments require an explicit mode.

The preferred benchmark now creates a new database clone for every run, starting
with all six destinations empty and unanalyzed. Historical repeated-TRUNCATE
series retained learned statistics and showed large drift; their performance
inferences are superseded. Output-correctness evidence from those runs remains
useful. The fresh input and all SQL snapshots are reproducible from the reports.

Latest fresh 100k SQL diagnostics, n=3 per mode:

| Mode | Median seconds | Range | Reduction vs baseline |
|---|---:|---:|---:|
| Baseline | 10.859 | 9.506–11.502 | — |
| Exact QueryFoundry lazy source | 9.086 | 8.300–9.181 | 16.33% |
| hash_array64 | 8.999 | 8.472–10.181 | 17.13% |
| Explicit ANALYZE after dimensions/table1 | 9.899 | 9.282–11.176 | 8.84% |
| Explicit ANALYZE after dimensions only | 10.028 | 10.005–11.880 | 7.66% |

hash_array64 is only 0.96% below the public control median, well within observed
spread; do not call it a win. All six tables passed exact bidirectional EXCEPT
ALL once per mode, typed fingerprints/counts every run, and schema/FK checks.
Its 1,228-case differential suite found no mismatch; finite fuzzing is not proof
of universal semantic equivalence. Direct QueryFoundry lazy paths retain known
SQLSTATE and acceptance/rejection counterexamples.

Fresh local organizer-service runs (n=1, includes actual preflight, counters and
all six version/dependency registrations, substitutes local psql for SSH):
baseline 11.016s wall, QueryFoundry 11.007s, hash_array64 11.398s. There is no
end-to-end lead in this small pilot. GUI cache/refresh and Linux/SSH parity are
not covered. SQL-only and local-service times must never be mixed as one metric.

The 1M historical SQL series verified exact six-table outputs but used the old
same-database harness. Its timing comparison is not the final controlled scale
result. A fresh-clone 1M pilot uses the corrected method and is recorded
separately in `reports/fresh-1m-pilot.json` when complete.

Recovery protocol v2 uses durable chunks, a persistent source/destination fence,
actual-content source SHA-256, native typed fingerprints, counter reconciliation,
and an audited two-database version saga. Twelve final interruption tests passed.
Actual local PostgreSQL crash/WAL recovery and pg_dump/restore passed. A narrow
official CHECK deparse equivalence is normalized; other schema changes remain
rejected. Source-preserving-hash edits, destination edits, schema edits, version
dependency edits and row-counter edits were rejected. Protocol v1 receipts retain
their canonical fingerprint algorithm for compatible replay.

The 100k four-chunk prototype measured 20.028s in an exploratory local run,
including three source scans and finalization; it is still costlier than a single
data transaction. Early 70.804s results used the older fingerprint implementation
and different runtime conditions, so their ratio is not an isolated speedup.
Atomic publication, cancellation/undo and default GUI resume remain unfinished.

Remaining release gates: standardized Linux/SSH and actual GUI timing, fresh
scale confidence, GUI raw-dump/version replay, larger scales, public repository
and exact public submission commit. This repository is a working laboratory and
prototype, not a declared winning submission.
