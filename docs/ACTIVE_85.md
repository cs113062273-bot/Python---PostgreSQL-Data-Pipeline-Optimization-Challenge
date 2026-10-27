# Active target: at least 85% less complete processing time

The target is not achieved. Latest fresh 1M complete-service comparison,
three runs per mode, rotating order, no affinity or destination warmup:

| Mode | Individual seconds | Median |
|---|---|---:|
| Unchanged official | 102.995721 / 103.941286 / 104.400414 | 103.941286 |
| refined_copy | 16.542056 / 16.968745 / 17.006804 | 16.968745 |

83.674682% less complete processing time. At this official median, 85%
requires <=15.591193s. Every future threshold uses its actually paired control.
This is local Windows/psql evidence, not official Linux/SSH timing or a score.
Report: native-refined-copy-official-1m-repeated.json.

Every run includes preflight, source manifest, proof/extraction, native COPY,
all key rebuilding and complete FK validation, commit and six control versions.
Exact six-table comparison passed first per mode; fingerprints, counters,
protected schema/sequence metadata and provenance passed every run. All six
native databases and their control history are retained. Independent full live
auditing of this new cohort is pending; the earlier eight-database live audit
does not automatically cover these six.

The source identity matches refined-cohort-source-snapshot.json, with 105
content-addressed Python source files. New driver cohorts archive sources at
startup and check all implementation identities before/after every run.
Actual transformed SQL is archived by hash and referenced in native versions.

## Research foundations, not a promoted default

- Private loopback server-file binary COPY checks existing file privileges
  and falls back to client COPY. No grants, PROGRAM, FREEZE or manual identity
  assignments. The PostgreSQL OS account needs access to the private directory;
  ordinary SSH remains unqualified.
- Normalize native text trim/case once; numeric/date input remains TEXT.
- Equivalent negative-pattern integer/numeric proof: 3,870 paired vectors,
  zero per-predicate differences. Whole-projection 1M medians 4.295507s to
  4.086250s, including complete typed projection fingerprints.
- Only a transaction-created disposable cache is UNLOGGED. All final/source,
  counter and version data stay logged; fsync/full_page_writes/
  synchronous_commit remain ON. The cache is dropped before commit and is
  recreated on retry. It is never an acknowledged checkpoint.
- Refined data SQL passed 121 native semantic cases and 16 actual exceptions /
  backend terminations, missing/bad binary files, complete rollback and exact
  retry with counters/six versions. Metadata connection reuse subsequently
  received more format/transaction isolation tests; no result cache exists.
- New stdin metadata reuse preserves native stdin statement boundaries and
  -c one-request boundaries. Nineteen native format/Unicode/error/rollback/
  partial-commit/state-reset tests and a complete 64-row service pass. It
  reexecutes 32 commands, including eight stdin programs, through two sessions.
- New exact scaled-int32 candidate: the proof bounds numeric magnitude to
  999999.999 and precision to three fractional digits. Scaling by 1000 is
  integral and fits int32. Each conversion is CASE-gated by a once-evaluated
  native ProjectSet proof; all unsafe inputs retain original batch fallback.
  Native 64-row service and 121 differential cases pass. Full timing pending.

## Retained negative or unqualified experiments

- Pure metadata -c/server COPY pilot: 19.223381s; exact six outputs/counters/
  versions/schema passed. Does not establish an improvement by itself.
- Pre-trim/NULL-normalize numeric TEXT pilot: 17.126925s, exact checks passed;
  not selected. Broader padding boundaries are not yet qualified.
- Early deletion of each completed binary file, seven native phases and
  Windows temporary-file hints: 18.679453s, exact checks passed; slower than the
  refined cohort. Private files were released, hints retained, no extra commit.
- Worker-degree 1M components: projection medians 4.132792s (8), 5.538506s (4),
  8.420819s (2), 23.273596s (0); reducing workers loses. Table6's tiny 4/8-worker
  difference does not establish an end-to-end win.
- Dense native-parent array lookup: exact typed transformation fingerprints
  matched, but all three joins became slower, plus 0.639185s cache build.
  Earlier NULL-bitmap array attempts were cancelled/timed out and retained.
- OFFSET projections had seven workers, but the expensive extraction sat
  above Gather in the leader. Worker count alone did not prove parallel work.
- Native ProjectSet payload/bundle barriers ran below Gather. 100k medians
  0.480948s original, 0.481491s payload-only, 0.467147s payload+bundle, with a
  reversal in one run. No full-pipeline win or default promotion is claimed.

Original atomic compiler is still
ce4f60cd7aae88e708301ccbcbe8b97dbafebded5cf8ea4cf34bd0f21028fbfd.
Generator/seed and all 32 organizer files remain unchanged. Linux/SSH, final
default integration, bounded 300M operation and managed COPY chunk resume
remain separate qualification gates. No public submission is claimed.

To provide space for current controlled tests, only four explicitly receipted
SQL-only clones without native versions/counters were retired (10.23 GiB).
One old winning SQL clone, QueryFoundry control, canonical oracle/templates,
all native-versioned databases and source/report evidence remain retained.
