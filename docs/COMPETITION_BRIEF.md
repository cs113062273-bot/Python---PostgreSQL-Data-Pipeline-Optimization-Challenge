# Competition contract and evidence

Active competition: [Python & PostgreSQL Data Pipeline Optimization Challenge](https://www.kaggle.com/competitions/python-postgre-sql-data-pipeline-optimization-challenge).
Checked 2026-10-26, Asia/Taipei. Direct page reads were empty; search-indexed
official overview content confirms performance 60, recovery 20, resources 20,
the eligibility gates, public repository + exact commit + submitted Writeup,
and organizer reruns on standardized hardware. Indexed content can lag the live
page. Deadline, detailed Rules and authenticated submission state remain unverified.
No enrollment, publishing or submission has been performed.

The official overview spells the repository `kaggle_postgresql_challenge` in
one location. The working authoritative source is
[igorsiecz/kaggle_postgre_challenge](https://github.com/igorsiecz/kaggle_postgre_challenge),
pinned at `5dfde9d967425982229b92d6ffe98efcd74612e9`.
`app/` is a byte-preserved import, including generator, UI, validators,
version/dump operations and six SQL recipes. `upstream.json` records hashes.

Read and retained: generator and schema; six recipes; expansion preparation,
function safety, protected scope, manifest, managed DML counters, destination
locks, timing and version registration; dump/version replay paths; all tests.
The dependency order is table3, table4, table1, table5, table6, table2.
Recipes prohibit WITH/CTE, RETURNING and ON CONFLICT. Functions must be native
immutable pg_catalog functions, without SECURITY DEFINER.

The application records local thread CPU, remote Linux command elapsed time,
and PostgreSQL clock_timestamp stage elapsed time. SSH connection and returned
result transit are excluded. `bench.run` is SQL-only diagnosis and must never
be called MEASURED PROCESSING TOTAL. Full eligibility still requires the real
Windows GUI + Linux PostgreSQL/SSH workflow and version/dump recovery checks.

Do not alter generator, fixed seed or official input; no hardcoded outputs.
Do not disable checks, fsync, full_page_writes or synchronous_commit, or use
unlogged durable data/checkpoints. SQL experiments stay opt-in until verified.

QueryFoundry is a public control, not an official winner:
[exact submitted state](https://github.com/Agnuxo1/QueryFoundry/tree/32c283f32fe35e9b5bd0e538bd6631f305863fb3).
Its docs/RULES.md refers to the older competition and is not used as the
authority for the active competition. Its current source was also inspected
at `8f312ba5c3cba52e8cccbd38c637c0bbd36f2558`.
