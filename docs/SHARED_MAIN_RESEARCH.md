# Shared main proof and numeric reuse, 2026-10-30

Optimization resumed at the user's request. The earlier stop recorded in
`reports/final-optimization-attempt.json` is historical and is preserved.

The previous best single full-service pair is unchanged official 112.2643395s
versus native main 11.4610000s, or 89.791059% less wall time. Its actual 90%
threshold is 11.226434s. The later compact-main pair is 118.8149065s versus
13.3143771s (88.794018%), retained separately. No 90% claim is established.

`bench/native_main_shared_probe.py` keeps the earlier 13-column timestamp
normalizer and native table1 loader. It composes the separately pinned
per-call numeric memo details loader with a shared aggregate scan of the
existing main/details predicates. Neither predicate is removed or weakened.
The original independent main scan remains when details software is unavailable.
Only the current transaction's proof result is reused. Original SQL validation,
receiving guards, identities, counters, keys/FKs, durable commit and versions
remain active; application defaults and the organizer sources are unchanged.

PostgreSQL's [aggregate documentation](https://www.postgresql.org/docs/17/functions-aggregate.html)
defines `bool_and` over non-NULL inputs. The original aggregate expressions,
including their empty-input `coalesce`, are preserved verbatim.

Qualification receipts:

- `semantics-native-main-shared-native.json`: 121 actual COPY/first-error cases.
- `semantics-native-main-shared-boundaries-82.json`: 82 boundaries.
- `semantics-native-main-shared-layouts-11-v2.json`: 11 receiving layouts/defaults.
- `semantics-native-main-shared-text-112.json`: 112 cases across all 14 axes.
- `recovery-native-main-shared-atomic.json`: 24 actual faults and exact retry
  with six versions, including exceptions, backend loss, identity exhaustion,
  bad frames and bad/missing files.
- `semantics-native-main-shared-fallback.json`: unavailable-details injection
  exercises both acceptance and rejection by the independent main proof, with
  exact six outputs, native counters, source and catalog/sequence checks.

The fallback harness's first two attempts failed on test setup/diagnostic
access (a counter table does not yet exist before the first service execution;
diagnostics live on the service timing state). Those harness errors were fixed;
the candidate was unchanged. These attempts are not passing evidence.

The new alternating fresh comparison uses an untouched 1M generator template
and FILE_COPY clones for every trial. The retained official database supplies
only the exact output oracle. Its old timing is never used as the new control.
Both measured candidates use identical current server settings, include full
processing and receive six-table exact verification on every run. Any reported
gain from this comparison is against the prior candidate in this same cohort,
not an official-score reduction.

Completed three-round result (seconds, full service wall):

| Round/order | Prior native main | Shared main | Observed reduction |
|---|---:|---:|---:|
| 1, prior/shared | 15.943458 | 14.427152 | 9.510524% |
| 2, shared/prior | 33.150420 | 24.254005 | 26.836509% |
| 3, prior/shared | 24.421841 | 15.482002 | 36.605919% |
| Median | 24.421841 | 15.482002 | 36.605919% |

Every measured run passes six exact outputs, actual counters, six versions,
protected schema/sequence options, source digest, workspace cleanup and proof
that both typed loaders and full FK validation actually ran. All 352 native
semantic/layout/fault/fallback cases bind to the same candidate hook
`772733959070ed5faa53abedb89429ce19ef5db2ab84fe4e0fabf5adfdf5fe5d`.
The 52 compiler/report tests, six organizer tests and 32 upstream hashes pass.

**Large timing drift limits the effect-size claim.** Prior times range from
15.94 to 33.15s; shared times range from 14.43 to 24.25s. All three rounds favor
shared, but this one cohort does not prove a stable 36.6% speed benefit. The
paired-reduction median is separately 26.836509%; it is not the ratio of the
two elapsed-time medians. No failed/slow run is excluded. The official original
was not timed in this cohort; the historical 89.791059% figure is unchanged,
and this experiment does not evaluate achievement of the 90% objective.

Receipts: [complete cohort](../reports/native-main-shared-fresh-1m-20261030.json),
[audited analysis](../reports/native-main-shared-analysis-20261030.json).
The isolated 55433 cluster was returned to `wal_level=replica`; fsync,
full_page_writes and synchronous_commit stayed on. The first unprivileged WAL
restart attempt failed before changing settings and is retained separately.
The successful elevated restart and final restoration have their own receipts.

Reproduce the same candidate comparison against a retained official oracle:

```powershell
.venv/Scripts/python.exe -m bench.native_main_shared_fresh --reference lab_native_official_6a50a771eb --repeats 3 --output reports/shared-main-new-cohort.json
```

Each invocation requires a new report path and generates a fresh template.
The driver records the actual current server settings for both candidates;
the retained cohort used the isolated minimal-WAL configuration. Run semantic
checks through `tests.semantics.native_main_shared_copy` (ordinary, `boundaries`,
`layouts`, `axes`), `tests.semantics.native_main_shared_fallback`, and
`tests.recovery.native_main_shared_atomic` with the isolated libpq environment.

This adapter has not completed its own server-crash/dump-restore, Linux/SSH,
300M resource or official eligibility qualification. No default promotion.
