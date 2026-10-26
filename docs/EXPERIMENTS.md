# Experiment ledger

All measurements below are local diagnostics, not an official Kaggle score.
Machine: Windows 11, i7-12700H (hybrid 14 cores / 20 logical processors),
16 GiB RAM, NVMe SSD. PostgreSQL 17.11 EDB, 128 MiB shared_buffers, 4 MiB
work_mem, fsync/full_page_writes/synchronous_commit on. JIT unavailable.
Exact SQL hashes, commits, settings, individual stages and resource limits are
in JSON reports. The original application/generator files remain unchanged.

## Controls: 100k, three alternating rounds

`reports/100k-controls.json`, SQL diagnostic stages only, original six outputs
checked by exact bidirectional EXCEPT ALL once per mode and two typed hashes
plus counts each run. Schema/index/constraint and relationship checks passed.

| Mode | Individual seconds | Median | Reduction |
|---|---|---:|---:|
| baseline | 27.523, 24.738, 27.499 | 27.499 | — |
| payload_once | 25.563, 25.187, 23.226 | 25.187 | 8.41% |
| lazy_keys (independent control) | 18.512, 21.467, 21.246 | 21.246 | 22.74% |
| record_once | 23.414, 23.628, 23.886 | 23.628 | 14.08% |
| path_once | 26.721, 22.424, 23.487 | 23.487 | 14.59% |

Baseline stage medians: table2 11.448s, table1 8.791s. These are the dominant
targets. lazy_keys table2 6.483s; record_once table1 5.717s. This independently
reproduced the approximate public control improvement in this initial series.
Some small functional tests overlapped this exploratory series; do not use it
as a final comparative performance claim. SQL timing excludes GUI/preflight,
managed counters, version/dump handling, SSH, oracle work and commit latency.

## Semantic rejection, seed 1507

`reports/semantics-1507.json`: 1,026 cases per mode, table2-only and full-pipeline,
four planner configurations. All are explicitly mutated functional fixtures.
One full-pipeline multi-error case caused baseline SQLSTATE 22P02 while
payload_once, fields_once and record_once succeeded. The mutation includes an
invalid dimension text_column_2 plus integer_column_1=true. A planner barrier
can move the table5 numeric filter behind a join that drops the bad row.
This is an acceptance/rejection difference, not just a message difference.

Decision: **REJECT promotion** of these three variants under the strict
equivalence policy. Keep their code/reports for controlled research. The
original application also refuses jsonb_to_record because it is STABLE. Only
the exact generated all-TEXT record projection is allowed in the disposable
laboratory to measure the hypothesis; application validation is unchanged.

## Isolated pilot and failed experiment

`reports/100k-new-pilot.json`, n=1: baseline 29.981s; array_values 25.091s;
record_table1 28.344s; record_table2 25.798s. No win is established from n=1.
All successful candidates passed six-table exact checks. The last each_once
attempt was rejected by the organizer parser, so the process stopped before
summary generation. Raw successful runs were retained and summarized later.
The parser failure was corrected by adding an explicit OFFSET 0 terminator to
the aggregate projection; a fresh each_once timing is still needed. It has no
promotion status.

## Conservative candidates: 100k, three alternating rounds

`reports/100k-final-candidates.json`, code 8130884. Path extraction changes only
operator evaluation; array_values uses immutable unnest with all 14 positions
and all 56 raw text fields. No early position skipping or typed record casts.
path_array combines table1 path extraction and table2 eager array expansion.

| Mode | Individual seconds | Median | Reduction |
|---|---|---:|---:|
| baseline | 30.517, 15.721, 15.015 | 15.721 | — |
| path_table1 | 23.474, 17.543, 17.083 | 17.543 | -11.59% |
| path_array | 15.792, 14.920, 14.885 | 14.920 | 5.09% |
| lazy_keys (independent control) | 13.944, 14.465, 14.571 | 14.465 | 7.99% |

Large global drift occurred: even unchanged baseline almost halved. Several
unrelated stages improved together. Cache, scheduling on a hybrid CPU, planner
statistics and host activity have not been separated. This conflicts with
the first series, so **no stable 20%+ win or default promotion is established**.
Do not cherry-pick the slower baseline. Path_table1 is not a standalone winner.

`reports/semantics-2027.json`: 826 cases each for array_values/path_once, no
differences. `reports/semantics-3011.json`: 3,028 cases each for path_table1,
array_values and path_array, no differences. Includes the known trap and
multiple planner settings. Finite fuzzing is evidence, not universal proof.
Decision: **INVESTIGATE** path_array at larger scale and on the official Linux
environment; baseline remains the default.

## Direct QueryFoundry source and next optimization

User steering on 2026-10-26: build directly on the public submitted source.
`vendor/queryfoundry_recipes.py` is byte-identical to submitted
`dbperf/recipes.py`; SHA-256
`f84d00103da768bc88ed1fb7cfd21cec340bbc218218f858a94fe1037114a31c`.
qf_payload/qf_lazy call it directly. qf_lazy_path1 replaces only its table1
recipe with our path candidate. From cea38c0 onward `lazy_keys` also calls the
original vendor implementation; historical independent-control reports retain
their exact SQL/recipe hashes and are not relabeled as original source.

`reports/semantics-queryfoundry-direct.json`: 628 cases per direct mode, seed
4051. qf_payload had no observed mismatch; qf_lazy and qf_lazy_path1 each had
three. Two multi-error cases changed SQLSTATE (including Infinity/NaN integer
conversion). Another accepted baseline input was rejected by lazy numeric
evaluation. Decision: **REJECT default promotion** of both direct lazy modes.
This demonstrates why inheriting the original fast path still requires an
eligibility oracle. Direct-source performance comparison remains research.

## Recovery

`reports/recovery.json`: 12 real database interruption boundaries passed,
including killed backend, killed client, before/after chunk commit, data/control
gap and lost control completion. Exact six-table outputs, no duplicate
versions, original counter reconciliation and unchanged committed chunk times.
`reports/recovery-tamper.json`: four tamper scenarios rejected, including actual
JSON edits retaining raw_hash. No full Linux/SSH or power-loss claim.
See RECOVERY.md for publication/cleanup restrictions and measured overhead scope.

## Scale and release gates

Official generator completed unchanged at 100k and 1M. 100k source retained
under lab_generated_100k before generating 1M. A 1M diagnostic pilot is queued
after the alternating 100k series. No 5M/10M/300M result or extrapolated winning
score is claimed. Full Windows GUI + Linux/SSH benchmark, real JIT, final
version/dump behavior and clean-room release verification remain explicit gates.


## Corrected fresh controls and transactional bulk candidates

The fresh-database-per-run method supersedes historical timing inferences above.
TRUNCATE keeps learned statistics; every new ranked run now starts from a new
clone with six reltuples=-1 destinations and no column statistics. References
are separate, with exact comparisons after timing.

- fresh-bulk-100k.json, n=3: official 10.456s, exact QF 7.854s, bulk_fks 6.870s
  medians. Reductions vs official: QF 24.88%, ours 34.30%; ours vs QF 12.54%.
- fresh-bulk-1m-pilot.json, n=1: official 103.975s, QF 91.322s, bulk_fks 64.920s.
  Ours reduces 37.56%. These are local SQL diagnostics, not official totals.
- fresh-bulk-index-100k-pilot.json, n=1: official 9.186s, QF 7.315s, bulk_fks
  6.590s, child unique bulk 6.361s, main+child unique bulk 6.495s, plus table1
  payload 6.088s. Index-only savings are modest, not a 50% result.
- fresh-bulk-combined-100k-pilot.json, n=1: official 9.436s, QF 7.317s,
  table1-payload+unique bulk 6.181s, table1/table2 payload 5.786s, table1 payload
  plus eager table2 arrays 5.621s (40.43% below official, 23.18% below QF).

All completed modes passed six-table bidirectional EXCEPT ALL, per-run typed
fingerprints/counts, exact restored index/constraint schema and relationships.
All index/FK building and validation stages are included. bulk_payload and
bulk_array each passed 1,228 differential cases (seed 10277), zero differences.
bulk_full passed 1,228 (seed 10383), zero differences. Atomic rollback tests
terminate backends and inject exceptions before/after key drops, inserts,
rebuilding and FK validation. Completed reports identify every boundary.

bulk_fks retains all PK/UNIQUE/CHECK constraints while six proven relationships
are rebuilt and fully validated within the same transaction. Unique bulk modes
also rebuild table1(raw_id) and/or table2(main_id,position), whose uniqueness
follows from protected recipes and native key metadata. bulk_full additionally
rebuilds native noncycling identity PK indexes on table1/2/5/6; NOT NULL stays
active and all keys are rebuilt before dependent recipes. bulk_full64 tests
transaction-local work_mem=64MB independently of a forced join strategy.

No direct lazy-key path is promoted. No generator, hardcoded distribution,
unlogged table, asynchronous commit or function-guard exemption is used.
The earlier no-win hash_array64 / NoMemo / ANALYZE experiments remain recorded.


## Semantic rejections and guarded normalization

The later seed 10501 invalidates bulk_payload, bulk_array and bulk_full: four
acceptance/rejection differences each over 528 cases. The apparent 40.43% pilot
is therefore not an effective eligible result. bulk_fks passed the same 528
cases without differences. Unguarded TEXT staging and work_mem variants also
have counterexamples. Known missing-dimension + bad unused numeric cases are
now permanent fuzz fixtures. The fuzzer exits nonzero on differences unless
explicit --allow-mismatches is used for a negative experiment.

The guarded_normalize compiler stages all original recipe inputs as TEXT, then
proves a conservative safe domain with immutable native operations. Numeric
bounds guarantee smallint and rounded int32 conversions; timestamp shape plus
Gregorian calendar arithmetic guarantees valid timestamps. Outside the domain,
original statements, indexes, FKs and settings execute unchanged. The gate
never rejects data. It derives no answers from the seed or generator metadata.
Inside the proven domain, TEXT fields are reused across all six recipes, proven
keys are bulk built, FKs are fully validated and sparse table2 positions use
only declared-number cache keys. All preparation and validation costs are timed.

Native tests: 32 boundaries, 6,000 calendar/style cases, immutable-function
metadata, 634 differential cases (262 fast, 372 fallback), 36 atomic interruption
boundaries, plus actual original-service counters/versions and valid-fallback
integration. Version recipes retain replayable canonical SQL and a compiler /
exact-batch SHA comment. Complete batch SQL is archived by local service/GUI.
One pilot exceeded the target but host drift was substantial; do not report a
50% achievement before the balanced repeated campaign and final checks.

Clone methodology uses explicit FILE_COPY to complete clone checkpoints before
measurement; see PostgreSQL 17 CREATE DATABASE documentation. Optional local
CPU affinity applies equally to official/control/candidate measured backends.
Latin-square order balances positions; backend CPU is recorded beside elapsed.


## Qualified local 50% target

The guarded_normalize balanced 100k campaign at commit 0e9f636 produced official
26.382s, QF 19.293s, ours 13.887s medians: 47.36% reduction. Its normalizer cost
was substantial. A component test found stage Memoize on/off medians 1.484s /
1.460s: too small to pursue. EXPLAIN showed table2 expanded before its parent
join, causing 232,386 parent index probes for 100,000 source rows.

The guarded_prefix compiler uses table1's original GREATEST as an upper bound;
this forces the parent join before expansion and avoids repeated parent probes.
Every qualifying positive p satisfies p <= max(declared). Unused positions are
skipped only within the proven error-free domain. The first three-run prefix
cohort reached 49.51%, not rounded up to 50%.

EXPLAIN confirmed the stage raw_id index was unused in all six recipes. Removing
it and 14 unused column-statistics samples, plus counted native ANALYZE of the
small fresh dimensions, produced the final commit 1a61b98 cohort:

| Mode | Individual seconds | Median | Reduction vs official |
|---|---|---:|---:|
| Official original | 10.280075, 10.300463, 10.364258 | 10.300463 | 0% |
| Exact QueryFoundry | 9.129256, 8.700098, 8.490309 | 8.700098 | 15.54% |
| guarded_prefix | 5.240706, 4.982953, 4.977160 | 4.982953 | 51.62% |

Final suite: 1,242 cases, zero differences (223 fast-domain, 1,019 fallback).
The combined proof explicitly includes empty destinations; a seeded/stale parent
therefore uses original fallback even in table2-only tests. All six exact outputs
and restored metadata passed. Source: fresh-guarded-prefix-v2-100k.json.
The full original service, actual private-cluster crash/WAL recovery and native
dump/restore were checked separately; these are correctness gates, not included
in the SQL-stage percentage. The 1M fresh pilot is recorded separately.


The fresh 1M pilot passed exact output/schema verification: official 104.528613s,
QF 91.191969s, guarded_prefix 45.551031s (56.42% / 50.05% less time respectively).
Complete local original-service wall pair: official 103.045745s, guarded_prefix
48.355471s (53.07% lower), with actual original preflight/counters/six versions.
These are separate metrics/cohorts, both n=1 at 1M.

Release fuzz seed 12473: 3,042 cases, zero differences, 587 fast-domain and 2,455
fallback cases. Thirty-six current interruption tests, seven metadata fallbacks,
32 boundary tests and 6,000 calendar/style cases passed. Clean source 1a61b98
passed 45 tests plus the six original app tests. A final transport-only hardening
acquires target locks before the first RR data snapshot; the new concurrent
writer/empty-proof/counter regression passes. Ranked SQL-driver locking and
transformation SQL were already in that order.

The unranked 1M profile records an explicit staged relation of 186,130,432 bytes.
1M sampled backend RSS was 182.2 MiB official / 231.3 MiB ours; WAL cluster delta
3085.6 MiB / 818.1 MiB. These are bounded diagnostic observations, not total peak
storage/memory or an official resource score.
