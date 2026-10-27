# Authoritative release evidence

These are local diagnostics, not an official Kaggle score. Comparisons use only
matching cohorts and metrics. Keep historical raw reports, including failed
experiments; their presence is not an eligibility/performance claim.

Current 80% successor (`7b0f077`):

Latest paired recheck is **77.777829%**, not a guaranteed 81.88%:
`native-official-recheck-after-recovery-1m.json`, 107.541923s official versus
23.898150s guarded_copy, one fresh run per mode, exact six outputs/counters/
versions/schema checks. Keep this slower result alongside the historical n=3
cohort. `ranked-cohort-live-audit-v4.json` and
`ranked-cohort-catalog-supplement.json` independently recheck all eight retained
benchmark databases against the newly executed original, with 48 full logical
table comparisons. `verified-results-evidence.json` seals source/evidence hashes
and scope; `docs/VERIFIED_RESULTS.md` provides commands and limits. Older failed
checker receipts and the deliberately cancelled v3 are retained and explained.

The active target is now 95% less time, still unachieved. See
`docs/ACTIVE_95.md` and `target95-research-round.json` for four new isolated
directions. The typed stage and JSON_TABLE lost their component/service tests;
index memory/worker tuning had a small noisy paired component gain. Compact
axes passed 121 native semantic plus 48 numeric-boundary cases and a fresh
19.165993s complete-service pilot with exact six outputs/counters/versions.
The pilots are unpaired; none replaces the three-run 81.876967% result or
changes the default. All actual batch SQL and negative results are retained.

New bounded/native research (separate qualification, no new ranked percentage):
`recovery-source-fingerprint-final-v2.json` has 57 compatibility cases;
`recovery-streaming-source-final.json` and
`recovery-streaming-source-tamper-final.json` pass the existing 12 interruption
and five tamper cases after the compatible hash acceleration.
`recovery-psql-session-file-stdin-final.json` passes five transport cases and
`semantics-phased-native-copy.json` passes 121 native differential cases.
`local-service-phased-native-copy-1m.json` is an **unpaired single** 17.7371s
pilot, not a replacement for the three-run percentage below.
`shadow-publication-primitives.json` rejects table replacement on older-RR
visibility and validates the original-object alternative.
`bounded-native-copy-resume-final.json` passes nine fixture-only checks with
four committed chunks, 17-row stage cap, 24 receipts, actual backend loss,
valid binary corruption rollback, verifier tamper rejection and six exact
final outputs. Managed fencing/counters/versions are not integrated.
`chunk-projection-bounded997.json` checks a bounded slice within the 1M source;
the 1M fused-hash component variants lost and were not promoted. See
`docs/BOUNDED_COPY_RESEARCH.md` for limits and reproduction entry points.

- native-fresh-copy-1m-repeated-v2.json: fresh complete-service 1M, three runs
  per mode, official median 110.523788s / guarded_copy 20.030262s, **81.876967%
  less time**. No CPU pin; exact six-table comparison once per mode, every run
  fingerprints/schema/sequence/counters/six native versions. Child reports and
  text timing reports live in the same-stem directory. Native accounted-item
  medians are 110.340067s / 19.940298s (81.928325%), still a local substitute.
- native-fresh-copy-1m-overlap-rejected.json: first attempt aborted before any
  timing was accepted because clean-room installation/testing overlapped it.
- fresh-parallel-cache-1m-repeated.json: separate SQL-only 1M n=3, 81.237743%
  reduction; CPU 2 leader pin equally applied. Do not pool with service timings.
- semantics-native-copy-pilot.json / semantics-copy-layout.json: 121 native
  differential batches and six binary-layout fallback cases.
- recovery-native-copy-atomic.json / recovery-native-copy-server-crash.json:
  22 actual-batch exceptions/terminations plus actual immediate server stop,
  WAL recovery, exact rollback and full native retry with six versions.
- recovery-native-copy-dump-values.json / recovery-native-copy-concurrency.json:
  actual same-name dump/restore including physical identity rows and sequence
  state, plus a concurrent writer observed after the protected lock wait.
- clean-room-native-copy-7b0f077.json / clean-room-native-copy-service64.json:
  fresh archive/venv/locked installation, 51+6 tests, upstream byte verification
  and actual 64-row native execution. Functional reproduction, not another 1M
  timing cohort.
- native-copy-resources-1m-v2.json: separate sampled resource probe; the stage
  size is read inside its owning transaction and actual instrumented SQL SHA
  is archived/retained in versions. The preceding resource report corrects an
  invalid outside-transaction stage observation to unknown, not zero.
- native-unexpected-crash-dump.json: limited read-only metadata from an
  unexpected Windows child-process failure; see CRASH_INVESTIGATION.md.

Broader 300M bounded staging, combined checkpoint/COPY recovery, ordinary SSH
COPY transport and final default integration are not qualified. The records
below describe the earlier release and retain their original scope.

- fresh-guarded-prefix-v2-100k.json: fresh 100k, three runs/mode, balanced order,
  51.62% lower SQL-stage median than official. Exact six-table/schema verification.
- fresh-guarded-prefix-v2-1m-pilot.json: fresh 1M, one run/mode, 56.42% lower SQL
  stages than official. Exact six-table/schema verification.
- local-service-final-baseline-1m.json / local-service-final-prefix-1m.json:
  separate full original-service pair, 103.046s / 48.355s local wall, with original
  preflight, counters, atomic versions and compiler/batch provenance.
- semantics-guarded-prefix-release.json: 3,042 strict cases, zero differences;
  587 fast-domain, 2,455 exact fallbacks. safe-domain-boundaries.json: 32 boundaries
  and 6,000 calendar/DateStyle cases, immutable data-function metadata.
- recovery-guarded-prefix-release.json: 36 exceptions/backend terminations.
- recovery-guarded-server-crash.json / recovery-guarded-dump.json: real private
  cluster immediate stop, exact rollback, native retry/versioning and native dump /
  same-name restore. Original control identity retained. GUI filesystem flow is
  not tested by those commands.
- recovery-guarded-concurrency.json: concurrent target commit is visible after
  lock wait, exact original fallback and counter reconciliation.
- bulk-guard-fallbacks.json: seven metadata fallback/refusal checks.
- clean-room-1a61b98.txt: clean locked install, 45 tests, six original app tests,
  all 32 source byte hashes. Final hardening archive report is separate.
- plans/guarded_prefix-1000000/: unranked EXPLAIN/BUFFERS/WAL and stage size.
  Explicit stage 177.5 MiB; sampled backend RSS and cluster WAL are not whole-server
  peak resources. sql/: recipe, architecture and exact native batch snapshots.

The apparent 40.43% bulk_array pilot is rejected after semantic differences.
Unguarded payload/normalization/index modes retain negative cases. Early guarded
prototype syntax/duplicate-temp harness failures were fixed and retained as raw
research evidence, not included in release qualification. Older same-database
TRUNCATE benchmarks retained learned stats and are not final timing comparisons.

Data-stage timed implementation: 1a61b98. Later production lock hardening changes
only lock timing before the first RR snapshot; the ranked SQL driver already
used that order. Compiler/batch hashes in native version history identify the
exact executed implementation. No official Linux/SSH or 300M claim is made.

The final hardened source archive 509e260 passed the same 45 regression / six
original tests and source byte verification (clean-room-final.txt). Final native
fast/fallback and concurrent-lock checks also pass. See QUALIFICATION.json.
SQL snapshot hashes use UTF-8 with normalized LF; all archived SQL bytes are
verified against their filenames. Large failed prototype traces have byte-identical
gzip archives and uncompressed SHA records in archive/index.json; original local
files are retained outside Git. They are negative research evidence.
