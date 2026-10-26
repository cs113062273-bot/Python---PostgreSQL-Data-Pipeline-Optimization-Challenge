# Research notes

Sources consulted 2026-10-26. Repository clones are ignored under `.research/`.
QueryFoundry README, recipe implementation at submitted commit, SQL/GUI reports,
negative sparse experiments, planner fuzz report, receipts and recovery docs
were inspected. Its payload_once default preserves conservative relational
shape. lazy_keys remains experimental because multi-error SQLSTATE differences
were observed. Its durable receipt covers the whole data transaction, not
precommit chunks; its source manifest trusts stored raw_hash rather than actual
JSON. We independently reimplement the extraction controls, with attribution.

Generator: `AXIS_SHARES` assigns about 86.443896% to two axes and expected
2.322 axes per row. This distribution guides hypotheses only; algorithms must
handle all 14 axes and arbitrary valid data. Generated inputs stay unchanged.

- [JSON/JSONB functions](https://www.postgresql.org/docs/17/functions-json.html):
  record functions in FROM can reuse extracted columns. Extract as text first.
  Operators return NULL on incompatible shapes; record SRFs can throw. Our
  record_once input uses CASE on jsonb_typeof to preserve non-object behavior.
  Nested values and SQLSTATE still need differential testing.
- [LATERAL](https://www.postgresql.org/docs/17/queries-table-expressions.html):
  parameterized per-source projection supports the existing validator.
- [CTE materialization](https://www.postgresql.org/docs/17/queries-with.html):
  fences prevent duplication but can spill and block pushdown. Official recipe
  guards reject CTEs; do not silently weaken that guard. OFFSET 0 experiments
  provide narrow per-row barriers without a whole-source disk stage.
- [EXPLAIN](https://www.postgresql.org/docs/17/sql-explain.html): diagnostic
  ANALYZE executes DML. Run only in disposable databases, and never mix its
  instrumentation times with uninstrumented measurements. PG17 options include
  BUFFERS, WAL, SETTINGS, SUMMARY and TIMING OFF; MEMORY is deliberately omitted.
- [JIT](https://www.postgresql.org/docs/17/jit-decision.html): compilation can
  help long CPU queries but cost more than it saves for short ones. A setting
  alone is not proof of JIT: record pg_jit_available and plan evidence.
- [Bulk population](https://www.postgresql.org/docs/17/populate.html): COPY,
  index/FK rebuilds and ANALYZE must be measured end-to-end. Baseline already
  inserts server-side sets; generic COPY results are not applicable evidence.
- [Resources](https://www.postgresql.org/docs/17/runtime-config-resource.html):
  work_mem is per operation and can multiply; maintenance_work_mem concerns
  maintenance. Tune sessions and report settings and observed spills.
- [WAL](https://www.postgresql.org/docs/17/runtime-config-wal.html): asynchronous
  commit can lose acknowledged work; durable checkpoints require synchronous
  commit and logged tables. Do not hide WAL/checkpoint costs.
- [CREATE TABLE](https://www.postgresql.org/docs/17/sql-createtable.html):
  unlogged tables are truncated after crash; TEMP is session-local, not durable.
- [Parallel safety](https://www.postgresql.org/docs/17/parallel-safety.html):
  CTE/temp scans are restricted. More requested workers do not prove actual
  parallel execution; retain plans before introducing concurrency.

Additional public-submission search did not identify another sufficiently
documented implementation in this research pass. This is not an exhaustive
leaderboard survey. JSON_TABLE and bulk index rebuilds remain experiments
until the simpler measured candidates justify further complexity.
