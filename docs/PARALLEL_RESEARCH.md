# Active 80% reduction research

The 80% objective is still active and unproven. The qualified prefix release
and its historical evidence remain intact. These new modes are experiments;
the GUI/default has not been promoted.

## Evidence obtained so far

Fresh 1M SQL-stage pilot (one run per mode, identical leader CPU 2 pin):

| Implementation | Seconds | Reduction versus this official run |
|---|---:|---:|
| Official original | 104.243072 | 0% |
| guarded_parallel | 30.811329 | 70.4428% |

All six outputs passed bidirectional EXCEPT ALL; schema, indexes, constraints
and relationships matched. Costs include extraction, global safe-domain proof,
statistics, all key rebuilding and full FK validation. This excludes the full
service preflight/counters/versions/GUI/SSH and is not an official score.
See `reports/fresh-guarded-parallel-1m-pilot.json`. Its backend CPU counter
excludes parallel workers; do not present it as total CPU consumption.

`guarded_parallel` additionally passed a native 64-row service run including
actual counters and all six version/provenance registrations, 642 differential
cases (zero differences), and 36 exception/backend-termination boundaries.
These do not replace fresh repeated timings, server-crash, dump/restore or
large-scale resource qualification of the final selected architecture.

## Native parallel projection

The old correlated OFFSET 0 projection leaves extraction above Gather.
Single-element native `unnest` arrays form reusable expression boundaries
without that correlated subplan. One record holds the 64 TEXT inputs; another
holds the numeric/date proof bundles. The original immutable safe-domain
condition is evaluated per row in workers, followed by a global bool_and before
any destination DML. Unsafe/nonempty cases still execute the exact original.

Rollback-only 1M component evidence:

- Old extraction 9.313s plus proof 9.006s.
- Fused parallel extraction/proof 3.372s plus global decision 0.244s.
- Eight workers requested, seven actually launched (recorded in real plans).
- Native JSON_TABLE was rejected: 24.836s extraction versus 9.475s operators;
  its JSON-null result also differed from `->>` under the tested column options.

Sources: `parallel-domain-probe-1m.json`, `parallel-extraction-probe-1m.json`,
`extraction-probe-1m.json` under reports. No UDF, catalog volatility change,
STABLE-function exemption, UNLOGGED durable state or durability setting change.

## CTAS candidates

A subsequent fixed-source 1M cohort completed all six-table and schema checks:

| Implementation | Seconds | Reduction versus its official run |
|---|---:|---:|
| Official original | 103.618095 | 0% |
| lab_parallel_ctas | 26.917116 | 74.0228% |
| lab_parallel_bulk | 20.787423 | 79.9384% |

Source: `reports/fresh-parallel-ctas-bulk-1m.json`, commit `e3ef5f2`.
The 80% threshold in this cohort is 20.723619s, so the best run misses it by
0.063804s. It is not rounded into a success. Native full-service qualification
and repeated fresh timing are still required.

After verification, 9.03 GiB of reproducible oracle copies were reclaimed;
`verification-scratch-cleanup.json` lists the exact receipts and namespaces.
Canonical references, candidate outputs and all timing/verification receipts
are retained. Each removed comparison can be rebuilt from its candidate DB.

`lab_parallel_ctas` uses a logged, transaction-local-lifetime work relation so
parallel workers can scan the shared projection. Native CTAS computes each
recipe SELECT, followed by the final INSERT with original identity allocation.
Every staging/cleanup/index/FK cost is timed; work relations are dropped before
commit. It is explicitly blocked from the native service until counter/version
integration is qualified. Whole-batch stage storage is not yet bounded for 300M.

`lab_parallel_bulk` also removes the position OFFSET barrier after the global
proof, and bulk-builds two additional mathematically unique keys:

- table5's partial unique parent key: one source row per parent, id_column_3
  is the canonical NULL projection. The unrelated non-null partial index stays.
- table6's (parent, reference) unique key: each unique source-parent and reference
  pair appears once, even if text reference keys repeat.

Exact native metadata is required. Nine native tests cover default restoration,
changed predicates/order/storage/include columns, external FK references and
interruption rollback. These modes retain all NOT NULL/CHECK constraints and
rebuild/validate before commit. Both guarded_parallel and lab_parallel_bulk
passed 642 differential cases each (zero differences, 120 fast / 522 fallback).

Initial 100k CTAS pilot: official 10.253627s, lab_parallel_ctas 3.652867s,
64.3749% reduction; exact outputs/schema matched. It is not a 1M result.
Two earlier harness interruptions are retained explicitly as failed attempts;
neither supplies a candidate timing. Future fresh cohorts pin and verify compiler
and driver identities so source changes cannot contaminate a running cohort.

The isolated 100k CTAS plans show table2 falling from about 527ms to 175ms when
the expansion can run below Gather. Table1 instead chooses a parallel hash join
with one worker; EXPLAIN-only alternatives show a non-parallel hash table can
allow eight workers. Execution and complete cost must still be measured.

## Reproduce

Use the existing libpq environment for the disposable local PostgreSQL cluster.

```powershell
.venv/Scripts/python.exe -m bench.parallel_probe --database lab_template_1m --experiment domain
.venv/Scripts/python.exe -m tests.semantics.fuzz --count 300 --modes guarded_parallel lab_parallel_bulk --output reports/semantics-parallel-research.json
.venv/Scripts/python.exe -m tests.recovery.parallel_indexes
.venv/Scripts/python.exe -m bench.fresh --template lab_template_1m --reference lab_1m --modes baseline lab_parallel_ctas lab_parallel_bulk --repeats 1 --cpu-affinity 2 --output reports/fresh-parallel-ctas-bulk-1m.json
```

Keep source fixed during each benchmark; component/profile numbers never
substitute for a full six-table cohort. Linux/SSH and official scoring remain
unverified; the user requested local verification first.
