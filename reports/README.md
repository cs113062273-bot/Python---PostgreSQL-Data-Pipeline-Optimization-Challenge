# Authoritative release evidence

These are local diagnostics, not an official Kaggle score. Comparisons use only
matching cohorts and metrics. Keep historical raw reports, including failed
experiments; their presence is not an eligibility/performance claim.

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
