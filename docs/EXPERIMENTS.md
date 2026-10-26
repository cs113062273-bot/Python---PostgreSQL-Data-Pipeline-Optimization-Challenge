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
