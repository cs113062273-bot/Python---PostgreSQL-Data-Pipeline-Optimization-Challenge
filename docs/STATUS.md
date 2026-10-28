Active objective: **90% less complete processing time**, not achieved.
See [current 90% research](ACTIVE_90.md). Earlier 85% thresholds below are historical.

Latest single 1M pair: **105.120996s original / 11.423356s shared native
details, 89.133136% less complete service wall**. Exactly three binary files
confirm the new engine ran. Six exact outputs and native gates, independent
full EXCEPT ALL/source/catalog/counters/version audit, 17 native fault/retry
cases, pre/post-commit server crashes and dump/restore pass. Ordinary owner
permissions/code protection also pass. One local minimal-WAL AB pair; 90%,
repeatability, Linux/SSH and official eligibility remain unproven. Paired 90%
threshold is 10.512100s, another 0.911256s required. Older results follow.

Latest single local 1M pair: **103.818006s original / 11.689431s candidate,
88.740459% less complete service wall**. Both use the same durable minimal-WAL
configuration; the candidate resets only proven empty destination storage in
the data transaction. All six exact outputs and native gates, independent live
EXCEPT ALL/source/catalog/version audit, pre/post-commit server crash checks
and populated dump/restore pass. It remains below 90% by 1.307630s for this
pair. Repeatability and official Linux/SSH/eligibility remain unproven. The
isolated cluster is restored to replica afterward, and app defaults stay opt-in.
See reports/native-minimal-official-1m-pair.json. Replica-mode results follow.

New best single local 1M pair (2026-10-29): **106.639991s original /
13.570365s compact C, 87.274601% less full-service wall**. All six exact
outputs/counters/catalog/sequence/source/six-version checks pass. One AB pair,
not repeated or an official score; trusted C implementation eligibility and
Linux/SSH qualification remain unproven. Defaults remain unchanged. The 90%
threshold for this pair is 10.663999s, requiring another 2.906365s reduction.
See reports/native-c-compact-official-1m-pair.json; older results below follow.

Latest fresh 1M pair (2026-10-28): **113.174393s unchanged official versus
18.202447s parent_hint_copy, 83.916462% less complete processing wall time**.
This is one pair, passes all native output/integrity/version gates, and does
not achieve 85% or replace the best single inline pair (84.811364%). The
native MEASURED PROCESSING TOTAL values are separately 113.079168s versus
18.115297s (83.979987% less). No cross-cohort control is used.
Independent full EXCEPT ALL/source/version rechecking and catalog/durability
audits now pass the four live databases in this pair and the best inline pair
(parent-hint-inline-live-audit.json; parent-hint-inline-catalog-audit.json).
The three-pair calendar result below remains a separate historical repeated
comparison. See ACTIVE_85.md and reports/parent-hint-pair-analysis.json.

# Current status

Latest local result: **84.627281% less complete processing time**, official
272.559992s versus calendar_copy 41.899881s, three verified pairs. The first
complete pair plus two continuation pairs retain AB/BA/AB order with identical
processing hooks; post-timer verifier and observer-readiness repairs are
documented in calendar-table5-three-pairs-analysis.json. Failed attempts are
retained and excluded. All six output/counter/version/catalog/sequence gates
pass. The 85% target remains unachieved; at this control another 1.015882s must
be removed. The independently live audit of this new cohort is pending.

The active objective is **at least 85% less time than the official original**;
see [active research](ACTIVE_85.md). The earlier fresh 1M three-run comparison
measures **83.674682%**: medians 103.941286s official and 16.968745s refined_copy.
The target remains unachieved. Every run checks all six outputs, counters, six
versions and schema/sequence metadata; exact six-table comparison runs once
per mode. An independent full live audit of these six new databases is pending.
Earlier 77.78% and 81.88% cohorts remain separate, with all unfavorable results
retained. The eight-database independent audit below predates this new cohort.
Local complete-service three-run medians crossed the prior 80% threshold, but broader
submission qualification is incomplete.
Do not interpret these results as an official score or a completed submission.
The subsequent scaled-int cohort has a raw 90.68% reduction with a much slower
official control; it is not promoted as a win. Same-session kernels show that
algorithm is 13.23% slower than refined. The newer per-value reuse worker passes
121 native semantic cases and a complete exact 1M pilot (34.305058s), but still
needs a fresh paired official control. See ACTIVE_85.md for retained failures
and the separately labeled same-CPU measurement diagnostic.
The next candidate combines an equivalent Gregorian proof and table2/table5
parsing reuse. It passes 177 native semantic/boundary cases and 16 native faults
with exact retry. No new overall percentage is claimed before paired testing.
The eight retained benchmark databases have now passed independent post-recovery
source/full logical-row/counter/version checks. Read
[reproducible evidence and limits](VERIFIED_RESULTS.md); the new 77.78% result
and the unresolved private-server stop are both retained there.

New bounded-COPY research has passed a four-chunk/64-row restart experiment,
with at most 17 normalized rows, actual backend loss, file/row/source tamper
checks and six exact final outputs. This is not the managed counter/version/
fencing integration. A phased-psql 1M single pilot took 17.7371s, but has no
paired repeated official control; it does not update either paired cohort.
See [bounded research](BOUNDED_COPY_RESEARCH.md) and
[compatible source hashing](SOURCE_FINGERPRINT.md).

| Fresh 1M cohort | Official original | Candidate | Time reduction |
|---|---:|---:|---:|
| Latest complete local-service comparison, three-run medians | 103.941286s | refined_copy 16.968745s | **83.67%** |
| New complete local-service paired recheck after recovery, one run/mode | 107.541923s | guarded_copy 23.898150s | **77.78%** |
| Complete local original service, three-run medians | 110.523788s | guarded_copy 20.030262s | **81.88%** |
| SQL stages, three-run medians | 107.190432s | lab_parallel_cache 20.111344s | **81.24%** |
| Complete local original service, single-run pilot | 113.008988s | guarded_copy 20.975695s | **81.44%** |
| Same complete-service pilot | 113.008988s | guarded_ctas 22.581307s | **80.02%** |

The repeated complete-service cohort is frozen at `7b0f077`, with no CPU
affinity override. Official runs: 109.854989s, 110.523788s, 111.100602s.
COPY runs: 20.030262s, 19.728010s, 20.746827s. Every individual paired run
exceeds 80%; this is not a single best-run claim. Exact six-table comparisons
passed once per mode, typed fingerprints/schema/sequence/counters/six versions
passed every run. Fresh FILE_COPY databases, alternating starting modes,
unchanged original service preflight and all COPY/rebuild/validation/cleanup
costs included. An earlier start that overlapped the clean-room installation
was aborted before accepting a timing and retained as a rejected attempt.
The native report's included-item medians in this same cohort are 110.340067s
and 19.940298s (81.928325% less locally accounted processing time). That total
also remains a local substitute, not the official Linux/SSH measurement.

Sources: [repeated complete service](../reports/native-fresh-copy-1m-repeated-v2.json),
[repeated SQL cohort](../reports/fresh-parallel-cache-1m-repeated.json)
and [complete-service pilot](../reports/native-fresh-ctas-copy-1m-pilot.json).
The latter includes native preflight, actual counters, transformation, proof,
staging, COPY files, all rebuilding/validation, commit and six control versions.
The SQL cohort pins the leader to CPU 2 equally; the service pilot has no pin.
They are separate experiments and must not be pooled. Every mode has exact
six-table output checks plus restored schema/sequence metadata checks.

Native COPY additionally passed 121 actual-batch semantic differential cases,
six changed-schema binary-layout fallback cases and 22 actual-batch exception /
backend termination cases. Actual server immediate-stop/WAL-recovery/full
native retry, same-name dump/restore, and a concurrent commit during the target
lock wait have now passed for native COPY. Reports are
[server crash/retry](../reports/recovery-native-copy-server-crash.json),
[dump/restore](../reports/recovery-native-copy-dump.json) and
[concurrency](../reports/recovery-native-copy-concurrency.json).
A fresh archive/venv/locked-install regression passed for `7b0f077`
([receipt](../reports/clean-room-native-copy-7b0f077.json)); that is not a
clean-room DB timing run. Actual native COPY from that fresh archive/venv also
passed a 64-row functional DB execution, with exact outputs, actual counters,
six versions, restored schema/sequence/workspace and the same compiler SHA as
the repeated 1M cohort. It is not an additional clean-room 1M timing cohort.
Repeated complete-service timing is now complete.
A separate
unexpected Windows child-process heap corruption is recorded in
[the incident note](CRASH_INVESTIGATION.md); its root cause is not established.
The COPY transport is local-only, the GUI/default remains unchanged, and the
whole-batch shared stage still needs a bounded architecture for 300M rows.
Separate native resource probes record a 177.5 MiB normalized stage, 273.7 MiB
sampled COPY-file peak and 78.63% lower observed cluster WAL delta, with higher
summed backend RSS and temp I/O. Read [resource methods and tradeoffs](NATIVE_RESOURCES.md);
RSS sums include shared pages and are not physical-RAM measurements.
These candidates are separate from the earlier qualified release below.
See [parallel research and its remaining gates](PARALLEL_RESEARCH.md).

The local 100k SQL-stage target is achieved: guarded_prefix reduces the
three-run median by **51.62% versus the official original**, with all six
outputs and restored constraints/indexes verified. This is not an official
MEASURED PROCESSING TOTAL or Kaggle score. The user has no Linux/SSH environment;
qualification continues locally and the official environment remains a gate.

| Same fresh 100k cohort, n=3 | Median | Range | Time reduction vs official |
|---|---:|---:|---:|
| Official original (baseline) | 10.300463s | 10.280075-10.364258s | 0% |
| Exact submitted QueryFoundry lazy_keys | 8.700098s | 8.490309-9.129256s | 15.54% |
| Our guarded_prefix | 4.982953s | 4.977160-5.240706s | **51.62%** |

Ours uses 42.73% less time than QueryFoundry in the same cohort. The metric
includes TEXT staging, the conservative proof, statistics, all index rebuilding
and complete native FK validation. Clone, oracle, full service preflight,
counters, version registration, GUI/SSH, lock waits and commit latency are outside
this SQL diagnostic; commit latency is reported separately. One individual run
was below 50% reduction: the claim is specifically the three-run median.

Source: [fresh-guarded-prefix-v2-100k.json](../reports/fresh-guarded-prefix-v2-100k.json),
implementation commit `1a61b98`. Every run starts from a new empty, unanalyzed
clone. FILE_COPY completes clone checkpoints before timing; CPU 2 affinity is
identical for all measured local backends; Latin-square order balances positions.
All raw results, exact SQL and backend CPU/RSS/WAL limitations are retained.

## Correctness and integrity

- All six relational outputs: bidirectional EXCEPT ALL once per mode, typed
  fingerprints/counts and exact restored schema/index/constraint checks every run.
- Release guarded_prefix differential suite: **3,042 cases, zero differences**;
  587 proven-domain cases, 2,455 exact fallbacks. Prior v2 suite: 1,242 cases,
  zero differences. Earlier full-graph
  prefix suite: 621 cases, zero differences. The preceding guarded compiler
  also passed 3,034 cases. Finite fuzzing is not universal proof.
- Native proof tests: 32 numeric/date boundaries and 6,000 calendar/DateStyle
  cases. Added data functions pass the unchanged native immutable-function guard.
- Original-service pilots exercise actual counters, six atomic version /
  dependency registrations, fast and valid-fallback branches. Replayable canonical
  recipes persist compiler and complete batch SHA provenance in version history.
- Thirty-six transaction interruption boundaries passed for the guarded
  architecture. Actual private PostgreSQL immediate stop restored uncommitted
  schema/data/counters exactly; a complete retry through the original service
  produced the six exact outputs and six versions.
- Actual pg_dump/pg_restore of that completed run preserved all six outputs,
  validated constraints/indexes, actual source SHA-256, counters and versions.
  The data database name and retained control database identity were preserved.
- All 32 organizer files and the pinned QueryFoundry module remain byte-identical.
  fsync, full_page_writes and synchronous_commit remain enabled. No durable
  UNLOGGED data, generator changes, fixed-seed answers or STABLE-function bypass.

## Architecture

TEXT extraction never casts, filters or rejects. A conservative proof covers
all numeric conversions and valid timestamps. Empty protected destinations are
also required for the fast path. Outside either condition, original statements,
settings and constraints execute unchanged. Inside the proof, one TEXT stage is
reused by the six recipes, provably unique keys are built in bulk, and six FKs
are fully validated before commit. All NOT NULL and CHECK constraints remain.

The original table1 GREATEST is a sound upper bound for qualifying positive
positions: if declared[p] = p, then p <= max(declared). Prefix expansion uses
that freshly built parent value, caps only at the original 14-position limit,
and retains declared/type/weight predicates. No axis-count distribution or
precomputed answers are assumed. Small dimension statistics improve joins;
unneeded stage indexing/statistics were removed after EXPLAIN verification.

## Rejected results and remaining gates

The earlier apparent 40.43% bulk_array result is INVALID: new cases found
acceptance/rejection differences. bulk_payload, bulk_full and unguarded
normalization also have counterexamples. These remain negative research
artifacts and are excluded from GUI choices. Original bulk_fks passed those
new traps, but its 34.30% result is historical, superseded by the new compiler.
Historical repeated-TRUNCATE timings retained learned statistics and are not
used as final comparisons.

The fresh 1M SQL pilot completed (n=1): official 104.528613s, exact QF
91.191969s, ours 45.551031s: **56.42% less time than official**, 50.05% less
than QF. All six exact outputs and restored schema/constraints passed.
The separate complete local original-service pair (n=1), including preflight,
counters and all six versions, measured 103.045745s / 48.355471s wall:
**53.07% less time**. Its native timing-report totals are 102.976936s /
48.121935s. Local psql transport is not standardized Linux/SSH parity.
No larger-scale extrapolation is claimed. Full original
GUI + standardized Linux/SSH timing, GUI snapshot filesystem/version replay,
larger scales and a public submission commit remain release gates. Local native
CLI dump/recovery is narrower than the complete GUI workflow.

The independent chunk protocol v2 retains durable chunk receipts, actual-content
source SHA-256, fences, counters and an audited version saga. Its 12 interruption
and five tamper tests, actual restart and partial-job dump/restore passed. The
100k prototype costs 20.028s; it is not combined with the fast atomic compiler.
The atomic mode rolls back and retries pre-commit failures; the post-commit
control/version gap retains original-service behavior. No chunk reuse or full
recovery-score claim is made for the fast mode.


## Final hardening and resource evidence

After the timed implementation 1a61b98, production transport locking was moved
before the first RR timing-table write: both source and targets are stable before
its data snapshot. A concurrent writer commit during a target-lock wait is
therefore seen by the empty-target proof, uses original fallback, preserves its
row and reconciles all counters. The SQL diagnostic driver already locked in
that order, so its ranked transformation SQL is unchanged. The native concurrent
regression passes; compiler/batch provenance reflects the hardening revision.

The clean source archive at 1a61b98 passed all 45 regression tests, all six
upstream tests and all 32 source byte hashes. Final hardened archive 509e260 also passed the same 45 + six tests and byte
verification (reports/clean-room-final.txt). Native fast, valid-fallback and
concurrent-lock checks pass on that revision.

1M SQL diagnostics: sampled backend peak RSS official 182.2 MiB / ours
231.3 MiB (shared mappings included, not whole-server peaks); cluster WAL delta
3085.6 MiB / 818.1 MiB, about 73.49% lower. An unranked fresh 1M profile measured
the explicit temporary TEXT stage at 186,130,432 bytes total (177.5 MiB), dropped
at commit. Executor temp statistics do not include that explicit relation.
Detailed EXPLAIN/BUFFERS/WAL plans are retained under
reports/plans/guarded_prefix-1000000. No official resource score is claimed.
