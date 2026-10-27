# Native 1M resource observations

These are single instrumented runs per mode, separate from the uninstrumented
three-run timing cohort. They use fresh 1M clones and the same compiler as the
`7b0f077` repeated service cohort. Both completed all six output fingerprints,
original counters, six native versions and restored schema/sequence checks.

| Indicator | Official original | guarded_copy |
|---|---:|---:|
| Backend/worker RSS sum, sampled | 181.21 MiB | 1,750.86 MiB |
| Client-tree RSS sum, sampled | 120.52 MiB | 121.28 MiB |
| Private COPY files, sampled peak | 0.00 MiB | 273.74 MiB |
| Logged normalized stage, exact in own transaction | N/A | 177.51 MiB |
| Cluster WAL delta | 3,087.58 MiB | 659.86 MiB |
| Processing-window temp bytes delta | 74.69 MiB | 229.10 MiB |
| Parallel workers observed in window | 2 | 7 |
| PostgreSQL CPU, sampled lower bound | 98.39s | 57.55s |

The observed cluster WAL delta is 78.63% smaller.
The parallel candidate also has larger summed working sets and more temp I/O.
This is a measured tradeoff, not a claim to win the official resource score.

RSS sums double-count shared pages and do not measure unique physical RAM.
Sampling can miss short peaks and backend lifetimes. CPU is a sampled lower
bound. The window begins at child launch, including imports/initial lineage
setup outside the service timer, and ends at the processing-complete line;
output verification follows outside it. WAL is cluster-wide, including control
and background work. Temp counters can have PostgreSQL statistics flush lag.
Neither the sum nor the max of these storage indicators is a measured peak of
all filesystem usage at one instant.

An outside observer cannot see the uncommitted normalized relation's catalog
entry. The first attempted stage observation was corrected to unknown, not
zero, in the preserved earlier report. The v2 probe reads pg_total_relation_size
inside the owning transaction after projection. Its hook is benchmark-only;
the actual instrumented SQL is SHA-archived and its hash is retained in native
versions. It is not imported by ranked timing runs.

The shared stage currently spans the whole batch. This does not demonstrate a
bounded 300M representation or combined chunk-level restart; those remain open.

Sources: [comparison receipt](../reports/native-resource-comparison-1m.json),
[official resource probe](../reports/native-baseline-resources-1m.json),
[COPY v2 probe](../reports/native-copy-resources-1m-v2.json),
[probe implementation](../bench/native_resources.py) and
[in-transaction observation](../bench/native_resource_worker.py).
