# Active target: at least 85% less complete processing time

The target is not achieved. The later inline/trim full-service pair measures
108.272576s unchanged official versus 16.445128s inline_copy: **84.811364%**
less complete processing time, but only one pair. At its actual control,
85% requires 16.240886s, another 0.204241s less. Both runs pass all six exact
outputs, counters, six versions and protected catalog/sequence checks.

The next date-shape/combined-metadata worker regressed in its fresh pair:
103.209938s official versus 23.683014s candidate, **77.053553%** reduction.
All exact-output/integrity gates still passed; this slower result is retained
and is not promoted. Candidate leader CPU increased from 14.140625s in the
inline pair to 20.812500s, while the two official leaders used 92.437500s and
92.453125s. This does not prove a scheduling/clock cause; many data SQL stages
became slower, and controlled diagnosis remains necessary. The separate identical-CPU-2 comparison completed three runs per mode:
official median 109.057611s, candidate median 18.036482s, **83.461510%**
reduction. All gates passed. This fixed-core diagnostic does not replace the
slower default-affinity result or establish the target.

The preceding complete-service 1M comparison comprises
three fully verified pairs with identical processing engine/SQL, in AB/BA/AB
order, retaining the first pair plus two continuation pairs:

| Mode | Individual seconds | Median |
|---|---|---:|
| Unchanged official | 268.912693 / 272.559992 / 275.796549 | 272.559992 |
| calendar_copy | 40.652220 / 43.313545 / 41.899881 | 41.899881 |

**84.627281% less complete processing time.** The 85% threshold for this actual
control is 40.883999s: another 1.015882s must be removed. Every run passed six
outputs/counters/six versions/catalog/sequence checks. The first pair and first
continuation run per mode use exact full-value/multiplicity comparisons;
remaining runs use live original-row fingerprints. Windows leader CPU usage
was observed equally in both modes, without SQL/affinity/power changes; setup
was included. Absolute times drifted substantially from older experiments.

Evidence: calendar-table5-three-pairs-analysis.json. The initial planned n=3
cohort stopped after one complete pair because its post-timer fingerprint path
still referenced a retired JSON verifier cache. A continuation attempt then
failed before data SQL in the new CPU observer's startup wait. Both failures
are retained and excluded, with actual empty-data/zero-expanded-version checks
for the startup failure. Only the post-timer verifier and the observer's
wait-until-idle condition changed; processing hook identities stayed identical.
The observer subsequently passed 20 consecutive native startup tests.
The first pair and two continuation pairs are explicitly separate source
cohorts, not an uninterrupted n=3 claim. The complete independently live audit
of these six retained databases is still pending.

Previous fresh 1M complete-service comparison,
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
protected schema/sequence metadata and provenance passed every run. All six results and control history are retained; two refined candidate
databases (e753668678, 5ed5af86c7) now have independently verified full dump
streams and restored native metadata in offline archives, rather than live
copies. The archive receipts explicitly do not claim a populated full restore. Independent full live
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
  Native 64-row service and 121 differential cases pass. A later complete
  three-run cohort reports a raw 90.68% reduction, but both modes became much
  slower (official median 371.604866s; scaled 34.632144s). This is not promoted
  as achievement of the target: controlled same-session transform kernels show
  scaled is 13.23% slower than refined (15.364058s versus 13.568434s).
  The full unfavorable cohort and all checks are retained in
  native-scaled-copy-official-1m-repeated.json and scaled-refined-kernels-1m.json.

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
The remaining old SQL-only winner and QueryFoundry clones were subsequently
retired using the same explicit-receipt/no-native-history checks. Canonical
oracle/templates, every native official/refined result and all source/report
evidence remain. Losing native clones may be archived only after actual
pg_dump/pg_restore and complete ordered native binary SHA256 checks of every
table, catalog/sequence/ACL checks and unchanged control history; individual
manifests under reports/native-archives record whether a live copy was retired.

## Current experiments, not a new paired score

- Per-value native Memoize for table2: three same-session transform-kernel
  medians 13.654989s refined and 13.458069s candidate, with overlap/reversal.
  Table2 alone improved by roughly 0.37s. Native differential: 121 cases,
  zero acceptance/output/SQLSTATE/primary-error differences. Fresh complete
  1M pilot: 34.305058s, all six full-value/multiplicity comparisons, counters,
  six versions, protected catalog and sequence options passed. No paired
  official control exists for this pilot; no new percentage is claimed.
- The pilot also exercises native typed EXCEPT ALL verification after the
  complete processing timer. This checks all values in both directions;
  fingerprints do not substitute for exact comparison. It avoids a persistent
  1.5GB JSON verification cache, which was safely retired with a receipt.
- NULL-group proof-string allocation: bitwise string differential and complete
  TEXT/proof fingerprints pass, but 1M projection medians 5.581354s original,
  5.610364s sparse4 and 5.705206s sparse8. Rejected.
- Separate per-field numeric proof: all generated proof vectors and complete
  TEXT/proof fingerprints pass, but medians 5.204824s refined versus 8.417250s
  individual predicates. Rejected.
- Six real FK validations, rotating three-run memory comparison: 64MB
  4.522987s, 128MB 4.588443s, 256MB 4.543472s. No stable gain; no change.
- Explicit same-CPU leader diagnostics are now supported separately for both
  official and candidate batches, with setup inside the full timer and actual
  affinity receipts preserved by server COPY. Default cohorts still use no
  affinity. This is an investigation of measurement drift, not submission tuning.

- Pinning that FK component to CPU 2 still shows drift: median validation
  4.176309s / 4.172682s / 4.187212s at 64/128/256MB. Core placement alone
  does not explain the change; larger maintenance memory remains unselected.
- Reusing both table1/table5 scalar parses has no total kernel win (13.121031s
  versus 13.030589s). Table1 is rejected. Table5's separately timed transform
  improves in every run (roughly 0.948s to 0.856s); only table5 is selected for
  the next candidate, alongside the already tested table2 reuse.
- Equivalent Gregorian calendar proof: 148,800 vectors spanning a complete
  400-year cycle, including impossible dates, plus 16 shape/boundary cases:
  zero differences. Complete 1M TEXT/proof fingerprints match in every run.
  Three-run projection medians 5.593570s refined versus 5.118400s calendar
  (8.49% less time for this stage). This is not an overall pipeline percentage.
  This proof plus table2 reuse passes 121 native semantic cases and 16 native
  faults/exact retry; all source identities have been reconstructed and bound
  to content-addressed archives. The additional table5 composition now passes
  121 native semantic cases, 56 targeted rounding/calendar cases and 16 actual
  failures with exact retry, counters and six versions. Its actual archived SQL
  contains table5 reuse and excludes the rejected table1 scalar reuse.
  A fresh paired three-run full-service comparison is the next gate; there is
  no new overall percentage yet. See regression-before-calendar-cohort.json.
- New verification option groups by every native typed value and compares
  signed multiplicities, avoiding repeated sorts in two EXCEPT ALL directions.
  Both methods pass 14 verifier controls, including rejection of same-count
  edits, missing/duplicate/NULL rows, dimension-presence changes and edits with
  an unchanged global numeric sum. This remains full-value verification after
  the processing timer. Independent live auditing still defaults to EXCEPT ALL.
- Optional Windows GetProcessTimes observation now records actual leader CPU
  time on a retained process handle, without changing SQL/affinity/priority or
  power settings. Both official and calendar 64-row full-service smoke tests
  pass exact outputs, counters and six versions. Parallel-worker CPU is not
  included. Observer setup is counted in full processing wall for both modes.
  No results from this diagnostic are advertised as an algorithmic speedup.
- A later private-server availability interruption is separately recorded in
  private-cluster-availability-20261028.json. The server recovered on its
  original loopback port 55432 with all three durability switches ON. This is
  not a scored failure-recovery test or proof of the interruption's cause.
- Table1 inline quantization plus proof-based trim identity: the complete proof
  admits <=3 decimal places and <=6 integer digits, so scaled values are exact
  int32. NULL/zero/negative handling equals the original CASE/ROUND. Numeric
  parsing happens once without the rejected scalar FunctionScan overhead.
  Removing BTRIM only on directly referenced proof-covered fields is valid
  because that domain excludes leading/trailing whitespace; padded inputs
  still execute the original batch. Three-run table1 real-COPY component
  medians: 2.014088s current, 1.813362s inline, 1.758480s inline+trim. Every
  complete typed transform fingerprint matched. Native qualification: 121
  semantic +82 targeted boundary cases, zero differences, plus 16 actual
  faults and exact retry with counters/six versions. This remains an isolated
  candidate until full processing is measured; no new percentage is claimed.
- That inline/trim candidate subsequently completed the 84.81% single pair
  above. No larger earlier official denominator is reused for it.
- Timestamp ASCII shape fast path: 116,410 native shape comparisons, zero
  differences; complete 1M TEXT/proof fingerprints match. Three rotating
  projection medians 3.980944s current versus 3.718889s candidate, every run
  faster. Other timestamp layouts use the original regex; all original
  calendar checks remain. Native 121+82 semantic/boundary cases and 16 actual
  failures/exact retry pass. Explicit C collation prevents locale equality
  from broadening acceptance. Only immutable translate is added to validation.
- Combined native metadata framing reexecutes every original SQL command;
  -c retains one native request and stdin retains original statement commits.
  Setup, original output and rollback/DISCARD use one phase with quiet framing.
  Nineteen native format/error/transaction/Unicode/fallback isolation cases
  pass. Thirty-two-query replay medians: separate phases 0.094258s, combined
  0.065956s. Complete 64-row service, 32 reexecuted commands including 8 stdin,
  two closed sessions, six versions and exact outputs pass; 16 actual native
  faults and retry pass. No results, validations or version operations skipped.
  The full 1M pair still regressed to 77.05%, retained above.
- Bounded binary64 quantization was mathematically scoped and compared over
  1,034,012 native vectors with zero integer differences, but component medians
  0.784066s numeric versus 0.779896s binary64 overlap strongly. Rejected; the
  selected worker retains NUMERIC, never approximate financial-style output.
- Six versus eight workers over the same full extraction/proof/transform
  kernels: medians 13.924562s versus 13.230836s. Six rejected; keep eight.
- Superseded native pilots have restorable full or template-backed archives,
  with explicit manifests under reports/native-archives. The latter include
  actual full input reconstruction, all native values/ACL/sequences and all
  restored FKs, with control history unchanged. A separate full custom-dump
  stream verifier compares every rendered COPY row and actually restores the
  schema/ACL/sequences without a populated duplicate; it explicitly does not
  claim a populated restore. Escaped Unicode/control-byte and wrong-data
  rejection controls pass. Failed restore attempts are retained separately.
- Thirty-eight early 100k SQL-only baseline/QF clones were retired with exact
  raw-byte equality to the retained 100k template, matching catalog/sequence
  metadata, no extra objects, and zero native versions/counters/clients. Two
  SQL controls, all winning variants, canonical references/templates and all
  native official controls remain. No generator, source data or user file was
  changed. This creates space for a complete six-database comparison.


## Fresh-parent statistics experiment (awaiting 1M measurement)

Native nested-plan profiling found that position expansion executes 1M times
without Memoize, although weight/gap parsing already memoizes correctly. The
fresh table1 has no axis-count statistics yet. Adding ANALYZE of just that
column after its load and native key rebuild introduces position Memoize in
the actual 64-row native pipeline (59 hits, five misses). Its entire cost
stays inside processing. Profiling is instrumented and excluded from scoring.

The parent_hint_copy research worker combines these statistics with temporary
file attributes on the six disposable COPY files, within the same native data
program and without extra commits. It passed 121 SQL semantic cases, 82
boundaries, 16 actual fault/retry cases, and exact 64-row full-service output
with six versions. No 1M improvement is claimed before a fresh paired run.
Qualification: reports/parent-hint-qualification.json. Linux/SSH, large-scale
resource bounds and submission integration remain unqualified.
