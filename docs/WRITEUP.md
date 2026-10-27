# PostgreSQL pipeline optimization: local submission draft

This is a local draft, not a submitted Kaggle writeup or an official score.
The active competition is `python-postgre-sql-data-pipeline-optimization-challenge`.
The latest benchmark implementation is frozen at local commit `7b0f077`.
No public submission commit, publication or enrollment is claimed.

## Result and measurement scope

The complete local original-service fresh 1M cohort at `7b0f077` has three
runs per mode. Official times are 109.854989s, 110.523788s and 111.100602s;
`guarded_copy` times are 20.030262s, 19.728010s and 20.746827s. The medians are
**110.523788s versus 20.030262s: 81.876967% less processing wall time**.
Each paired run exceeds 80%. There is no CPU affinity override. Exact six-table
EXCEPT ALL comparison passed once per mode; every run checks typed fingerprints,
protected schema/sequence metadata, actual counters and six control versions.

Summing the native report's included timing items in this same cohort gives
medians 110.340067s versus 19.940298s, 81.928325% less accounted processing time.
That locally accounted total still substitutes Windows psql for Linux/SSH and
must not be advertised as an official judge measurement. The full wall metric
above is the primary local result; it includes additional unaccounted overhead.
A clean-room-install-overlapping first attempt was explicitly aborted before
accepting any timing. The accepted cohort began after that process was terminal.

The preceding separate single-run pilot is retained: official 113.008988s,
COPY 20.975695s (81.438914%) and CTAS 22.581307s (80.018132%). It is not pooled
with the repeated cohort.
A different frozen SQL-only cohort (`9b6e61c`, three runs per mode) has medians
107.190432s official and 20.111344s `lab_parallel_cache`, 81.237743% less time.
The SQL cohort pins each leader equally to CPU 2; workers retain server affinity.
These two metrics/cohorts are not pooled. QueryFoundry is a pinned public control,
not an official winner; its earlier same-cohort results remain in STATUS.md.

The native service timer includes original preparation/preflight, actual row
counters, transformation/proof/staging, COPY I/O and cleanup, key rebuilds,
complete FK validation, data commit and six control-version registrations.
Fresh FILE_COPY clones and post-run oracle verification are outside the timer.
Local Windows psql replaces Linux/SSH; this wall metric is not the official
MEASURED PROCESSING TOTAL or a prediction of the judge's score.

Environment: Python 3.11.9, PostgreSQL 17.11 EDB, Windows 11, i7-12700H,
16 GiB RAM, NVMe SSD. This binary has no JIT implementation. The untouched
organizer generator and fixed seed produce the source; all 32 imported organizer
files remain byte-identical. fsync, full_page_writes and synchronous_commit
remain ON. No unlogged durable state or hardcoded source answers are used.

## Bottlenecks and implementation

In the native pilot baseline, table1 takes 35.698s and table2 44.605s, together
about 71% of the 113.009s operation. The main costs are repeated JSONB extraction,
fixed-position expansion, sequential execution and row-at-a-time key/FK work.

The fast path builds one logged normalized relation, using native single-element
array/record barriers to extract fields as TEXT once per source row. The
projection and conservative safe-cast proof can execute in parallel. Session
planner settings are saved/restored; actual worker counts are verified in plans.
`jsonb_to_record` was researched but its STABLE volatility fails the unchanged
organizer recipe guard, so the production path does not exempt that function.
JSON_TABLE was measured and rejected for cost and observed null differences.

A global decision requires proven-safe input and empty protected destinations.
Unsupported values or metadata select exact original INSERT statements. Source
and destination locks are acquired before the repeatable-read data snapshot.
No generator distribution is used as a correctness assumption. The table2
position bound follows from the freshly computed original parent maximum, keeps
the original 14-position cap and retains declared/type/weight predicates.

`guarded_copy` computes transformed SELECTs with native parallel execution,
exports/imports binary files through actual psql `\copy` in a private client
working directory, and captures the actual COPY ROW_COUNT immediately. Those
receipts feed the original managed counter updates. Identity defaults still use
native sequences; a guarded cache change is restored before commit. The schema
layout gate checks receiving types, typmods, identity/collation and exact native
index/constraint shapes before binary transfer. Native NOT NULL/CHECK checks
remain active; eligible keys are rebuilt and all six FKs restored/validated
before data commit. A workspace-name collision selects original fallback and
preserves the unrelated namespace.

Generic psql binary redirection was rejected after an actual Windows CRLF
corruption probe. The implementation uses binary-aware \copy in both directions.
The CTAS alternative uses ordinary SQL transport with the same original final
INSERT/default identities; its separate costs are recorded, not inferred from
COPY performance.

## Correctness, versions and recovery

The native COPY suite covers 121 actual captured batches with zero observed
semantic differences, including malformed-input outcomes, actual counters and
rollback. Six changed-layout native cases verify exact-original fallback;
22 exception/backend-termination cases exercise actual COPY boundaries.
Finite tests are not a universal proof. Broader common-core and earlier release
suites are retained with their own mode/commit scopes.

A real immediate stop after uncommitted COPY and before FK restoration passed
WAL recovery: original schema, empty outputs, source SHA, sequence options and
counter rows were restored. Full native retry produced the exact six outputs
and six versions. Real pg_dump/pg_restore of that completed DB preserved its
name, source/output/schema/sequence/counter state and retained control history.
A concurrent writer committed while target locks were pending; the proof saw
it, selected original fallback and preserved the writer and counters.

The atomic COPY mode rolls back and retries the whole data transaction before
commit. Its post-data-commit control-version gap retains the original service
behavior. A separate durable chunk protocol implements committed-prefix resume,
source-content verification, write fences and an audited control-version saga;
it is not yet combined with the fast COPY mode. It is not distributed atomic
commit. Direct readers of that prototype must honor active job state.

A Windows child-process heap corruption caused automatic
recovery between tests. Its root cause remains unresolved; candidate dump
addresses alone do not explain it. CRASH_INVESTIGATION.md retains the evidence.
Subsequent passing crash tests do not erase or explain this incident.

## Resources, reproduction and remaining work

Separate native resource probes verified the same 1M source and all native
output/counter/version gates. COPY's logged stage is 177.51 MiB and its sampled
private-file peak 273.74 MiB. Observed cluster WAL is 659.86 MiB versus official
3087.58 MiB (78.63% lower); processing-window temp bytes are 229.10 versus
74.69 MiB. Summed sampled PostgreSQL RSS is 1750.86 versus 181.21 MiB: shared
pages are double-counted, so these are not unique physical-RAM peaks. The
candidate trades higher working sets and temp I/O for lower wall time and WAL.
See NATIVE_RESOURCES.md for CPU, sampling/window/statistics limits and actual
instrumented SQL provenance. These are single resource probes, separate from
ranked uninstrumented timing. The normalized relation spans the entire batch;
bounded 300M staging is not established. No 300M score is extrapolated.

README.md and RELEASE_80_AUDIT.md link commands, cohorts, gates and remaining
work. A fresh source archive/venv/locked dependency install at `7b0f077` passed
51 compiler tests, six original application tests and the 32-file upstream
integrity check. A 64-row real native service execution from that archive/venv also passed
exact outputs, actual counters, six versions, schema/sequence/workspace checks
and compiler identity matching the repeated 1M cohort. It is functional
reproduction, not a separate clean-room 1M timing cohort. Bounded/chunk integration and broader submission qualification remain open.
COPY currently requires the local private-workspace transport; ordinary SSH
COPY and the fastest default are not promoted. The user has no Linux/SSH host
and requested local validation first. Official GUI/SSH/dump replay and a public
exact submission commit remain unverified.

Attribution: organizer Igor Moreira / igorsiecz/kaggle_postgre_challenge and
Agnuxo1/QueryFoundry, Apache-2.0. The exact public control is retained under
vendor at submitted commit 32c283f32fe35e9b5bd0e538bd6631f305863fb3. Official
rules/timing sources and primary PostgreSQL research links are in
COMPETITION_BRIEF.md and RESEARCH_NOTES.md; rejected experiments are retained
in EXPERIMENTS.md and raw reports.
