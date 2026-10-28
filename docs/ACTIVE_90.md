# Active target: at least 90% less complete processing time

The user raised the objective to 90%. This target is not achieved. The metric
is complete processing against the unchanged official in its actual paired
cohort; no slower control from another experiment may be substituted.
All source/destination validation, native identities, counters, full constraint
validation, durable commit, versions, and required recovery behavior remain
in scope. Timing and final eligibility must be proved separately.

Before native C research, the best single default-affinity full-service pair
was 108.272576s official versus 16.445128s inline_copy (84.811364% less wall
time). The newer C result is 87.274601%, with limits detailed below. Three verified
calendar pairs, comprising one pair plus a documented continuation, measure
84.627281%; those are separate cohorts with large absolute timing drift.
The last parent-statistics/scratch-hint pair gives 113.174393s versus
18.202447s (83.916462%). Its 90% threshold is 11.317439s.
Four live databases from the latest and best single pairs passed an independent
full EXCEPT ALL/source/version audit plus catalog/durability checks.
Historical experiment details remain in ACTIVE_85.md.

## Current work

**New best single 1M pair (2026-10-29): 87.274601% less complete service wall**,
unchanged official **106.639991s** versus compact native C **13.570365s**.
The candidate's actual six-table native typed value/multiplicity comparison,
managed counters, protected catalog, sequence options/ownership, source digest,
workspace/extension cleanup and six compiler-bound versions all pass.
This is one fresh AB pair on the separate Windows 55433 cluster, not a repeated
result, official ranking, or proof of eligibility for a C-based submission.
The corresponding 90% threshold is 10.663999s; **2.906365s remains to save**.
MEASURED PROCESSING TOTAL is separately retained in the child reports.
See native-c-compact-official-1m-pair.json. Original and candidate remain live.

An independent later audit rechecks both live databases after the rollback
experiments: all six full bidirectional EXCEPT ALL comparisons, actual source
digest, counters, validated constraints, logged durable final tables, sequence
options, transient-object absence, six versions and recorded batch SQL SHA
pass. Only the two verified containing-database names are excluded from
cross-database information_schema comparison. The first audit deliberately
failed on those expected names and is retained; v2 passes without weakening
any type/default/index/constraint comparison. See
native-c-compact-1m-independent-audit-v2.json and
native-c-compact-local-qualification.json. 52 offline tests pass; all 32 official
application file hashes are unchanged.

The 1M compact breakdown is 1.2149s normalization/proof, 0.4728s source
manifest, 1.3489s table1 transform/COPY, 2.4846s table2 transform/COPY,
3.1599s four table key rebuilds, and 2.1559s full FK validation. New rollback
maintenance experiments target these native remaining costs; component times
cannot substitute for complete processing or the pair's control.

Four rollback-only native index/FK maintenance settings, three rotating trials
each, pass complete catalog/sequence restoration. Medians are 5.194606s
(128MB/2 maintenance workers), 5.324090s (128MB/0), 5.275381s (512MB/0), and
5.195797s (512MB/2). Increasing memory does not produce a material component
gain; no tuning change or full-service improvement is adopted. The first two
harness attempts failed on search-path-dependent metadata rendering, rolled
back and are retained; the passing v3 uses pg_catalog for both snapshots and
constraint definition comparison. See native-c-maintenance-components-1m.json.

The complete 48-trial native binary-export worker experiment passes full
bidirectional EXCEPT ALL of each replayed binary file and rolls back all private
work. The fastest median table1 export uses 8 workers; table2/5/6 show only
small 4-versus-8 differences. No meaningful full-service gain is established
or adopted. The first long-transaction harness hit disk capacity because dropped
trial storage was deferred until transaction end; it rolled back, and its
incomplete receipt/log remain. The v2 uses an aborted creation subtransaction
per replay, bounding storage instead of accumulating relfilenodes. Source and
final rows were not changed. See native-c-export-components-1m-v2.json.

A follow-up reproducing the production cache's four-column ANALYZE also passes
exact table2 binary replay for all worker choices. Its 8-worker export is
0.804237s versus 0.819466s median with all-column statistics in the separate
prior component. EXPLAIN shows Gather/hash join/function scan, with no Memoize;
there is no demonstrated statistics-related opportunity to claim or adopt.
See native-c-table2-core-statistics-1m.json; the two runs are not a full-service
comparison and their component difference is not counted toward the target.

Latest new native 100k pair (2026-10-29): unchanged official 13.137550s,
compact C candidate 3.039362s, **76.865078% less complete service wall**.
This is a single fresh AB pair, not a 1M result or the 90% objective.
The actual baseline establishes its original outputs without warmup or a
circular self-comparison; the candidate passes all six native typed values
and multiplicities against that separate baseline. Both retain mandatory
validation, counters, durable commit, original indexes/FKs and six versions.
See native-c-compact-official-100k-pair.json and its two full child reports.

The compact adapter's stage rename had hidden safe_domain from the original
mix-in's exact-name telemetry parser. Its original protocol name is restored,
so version provenance again records true/false. New identity also includes
native_c_lab.py. This hook (44ea8a2b...) passes 11 receiving-layout/default
cases, 16 actual native fault injections/full rollback/exact retry/six versions,
a populated dump/restore preserving native identity and sequence state,
eight metadata mutations rejected and three full-original fallback scenarios.
An actual immediate stop of only the isolated 55433 cluster during uncommitted
COPY also passes WAL rollback, file cleanup, fresh-service retry and six
versions (recovery-native-compact-server-crash.json). No power-cut or
post-commit version-saga recovery claim. The original 121/82/112 SQL cases
predate only this protocol/provenance correction and are retained separately.
Trusted native software eligibility remains unproven; defaults are unchanged.

The former slim-inline candidate ce40cd275b now has a full stream/metadata
restore verified archive; its complete timing/verification evidence is retained.
All original controls and the best inline candidate are retained live.

An opt-in NULL-normalized disposable TEXT cache passed 171 native SQL cases
and 82 native boundaries without output/first-error differences. The first
report filename retains an earlier 121-case label; its actual case array and
terminal log contain 171 cases, including 150 random-count input. The 171
case replay uses client COPY; the 82-boundary replay uses server COPY.
The original source/final/counter/version data are unchanged.

A rotating rollback-only 1M component comparison (three runs each, same
session) reduces cache storage from 186,138,624 to 146,817,024 bytes, 21.13%.
Projection plus six transform plus domain-scan medians are 7.353754s original
and 7.132352s NULL cache, about 3.01%; individual runs overlap/reverse.
This is not a complete processing percentage. No default promotion.

The next worker also removes only canonical constant-NULL fields from the
binary COPY projection and target list. Native COPY fills omitted fields
from defaults, so a new guard requires no defaults on every omitted column;
otherwise the unchanged original full INSERT program executes. Full layout,
source/domain, sequence/default identity, counter and FK validation remain.
No nonconstant expression or predicate is omitted. The altered-default tests
include non-NULL defaults and a division-by-zero default to catch unintended
evaluation. Native semantic/boundary/layout/fault suites and a separate
export/load component must pass before a fresh full-service pair is accepted.

PostgreSQL references:
- [COPY columns and default semantics](https://www.postgresql.org/docs/17/sql-copy.html).
- [Parallel-query limits for writes](https://www.postgresql.org/docs/17/when-can-parallel-query-be-used.html).
- [WAL compression durability/CPU tradeoff](https://www.postgresql.org/docs/17/runtime-config-wal.html).

## Evidence retention and limits

Three old calendar candidate databases have self-contained custom dumps.
Every rendered COPY row was checked against a stable locked source snapshot;
schema/owners/ACL/native sequence state were actually restored and checked
before retiring the disposable originals. Their native control histories and
all benchmark receipts remain. These are offline archives, not live databases;
a full populated restore is explicitly not claimed. The two official pairs,
inline candidate and parent-hint candidate remain live and independently
audited. See native-archives/*-stream-verified.json.

No official score/ranking, Linux/SSH qualification, 300M bound, default
submission integration, or completed managed native-COPY chunk resume is
claimed. Successful local tests do not satisfy those outstanding requirements.


## Qualification before the first fresh full pair

slim_copy on the parent-statistics/hint base passed 121 server-COPY semantic
cases, 82 boundaries, 11 receiving-layout/default cases and 16 actual fault
injections plus exact retry/six versions. The first layout-test log contains
a test-assumption failure: PostgreSQL optimized a benign DEFAULT NULL away.
The corrected test retains rejection requirements on all ten unsafe cases
and compares exact outputs/counters/catalog for the benign case as well.
The candidate implementation did not change for this test correction.

The complete binary export/load component (three rotating runs per mode)
measures 5.067765s full columns, 4.700300s slim binary, 5.949213s slim text.
Files total 287,041,703 / 207,183,423 / 126,580,900 bytes. Native text parsing
therefore loses overall despite smaller files; retain binary. All complete
nonidentity typed fingerprints, including omitted NULL values, match; logged
private tables are rolled back and original sequences/metadata remain intact.
This is a component result, not a whole-pipeline percentage.

The fresh full pair uses slim_inline_copy, composing the NULL cache and slim
binary projection atop the earlier inline base. It excludes the subsequently
proposed date-shape, combined-metadata, parent statistics and scratch hints.
This separate composition passed its own 121 semantics, 82 boundaries,
11 layout/default cases, 16 actual faults and exact retry/six versions. Its
actual populated custom dump/restore also preserves physical native identities,
sequence state, source, counters, all six outputs and six version records.
Evidence: slim-inline-qualification.json, recovery-slim-inline-dump.json.
The 51 compiler tests, six organizer tests and all 32 pinned upstream hashes
pass. This still does not establish full crash/resource/official eligibility.


## Later complete pair and native research (2026-10-29)

The slim-inline full native pair gives 104.260793s unchanged official versus
17.140230s candidate: **83.560235% less wall time**, not 90%. Its separate
MEASURED PROCESSING TOTAL values are 104.058150s and 17.081088s. Both runs
passed exact six outputs, counters, six versions and protected catalog/sequence
checks. No older, slower control is substituted. This does not replace the
84.811364% best single inline pair or prove a reliable total improvement.

Deferred axis text normalization passed 121 native semantics, 82 boundaries
and 112 explicit eight-value/all-14-position cases, without differences.
The 1M rollback component medians are 7.712412s normalized versus 7.448433s
deferred. All integer/numeric/date fields still enter the global proof; only
total text trim/case operations move after position selection. No full score.
Text Memoize group sizes 1/2/4 on 100k overlap/reverse, so are not selected.
All-TEXT jsonb_to_record has medians 0.464837s operators / 0.525584s record,
and still fails the unchanged user-recipe function policy; not selected.

A separate native C prototype on loopback 55433 traverses JSONB once and
performs a pure argument-only sufficient-domain proof. It has no SPI, relation
reads/writes, network, filesystem or identity-generation calls. The exact
64 TEXT extractions/proof match 4,247 payloads; three unsafe return descriptors
are rejected. A 100k three-pair CTAS-only comparison gives 0.484485s SQL versus
0.240384s native, every resulting row/all 64 values/proof compared exactly.
This roughly halves only preprocessing and does not establish 90% overall.
The measured initial C source is preserved by SHA under reports/native-source.

The extended compact API uses bounded integer arithmetic, keeps native SQL
timestamp semantics, and encodes selected axis tuples in at most 140 bytes.
A separate pure-argument decoder validates every frame. Actual original six
INSERTs versus compact transforms pass 461 mutated-fixture cases (260 fast
domain), including error SQLSTATE/primary text, scaling bounds and all 14 axis
positions. Native identities and relational constraints remain PostgreSQL's.
Managed counters/versions/full timing/recovery are not yet qualified here.

A private trusted-extension package was tested with a NOSUPERUSER database
owner. The owner can install/use/remove its three pure functions, cannot
alter their code or create arbitrary C functions, and all member functions
remain immutable/parallel safe/security invoker and superuser-owned. The
extension and work namespace are removed before commit. The first package
script failed because @extschema@ is incompatible with fully relocatable
installation; the failed package/log remain, and v2 disables relocation as
PostgreSQL documents. Application user-recipe function validation is unchanged.
Whether this trusted internal implementation preserves all required guarantees
needs further integration/testing; official eligibility is explicitly unproven.

Two private services unexpectedly stopped during this work; cause unconfirmed.
Some recent native build records contained only zero bytes and are preserved
as invalid evidence. Previously completed slim and native component records
were checked intact. Both clusters recovered with durability settings on; this
incident is not a scored recovery test. SDK files matched the authenticated
archives; a corrupt compiler cache was avoided with workspace-specific caches.
Report writes now flush/fsync a unique temporary file before atomic replace.
The injected replacement-failure test preserves the older valid receipt; this
is not a power-cut test. 52 compiler/report tests and 32 upstream hashes pass.

The previously audited parent-hint candidate 6c1c55a43e has a complete verified
stream/metadata-restore archive and retained control history, rather than a
live copy. Calendar candidates were archived similarly. All official controls
and the best inline/slim-inline candidates remain retained. See archive
receipts for precise limits: populated full restore is not claimed by streaming
archives. Native C research remains isolated and unintegrated into defaults.
