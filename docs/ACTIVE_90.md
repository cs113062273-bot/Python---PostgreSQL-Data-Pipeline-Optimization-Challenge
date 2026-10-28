# Active target: at least 90% less complete processing time

The user raised the objective to 90%. This target is not achieved. The metric
is complete processing against the unchanged official in its actual paired
cohort; no slower control from another experiment may be substituted.
All source/destination validation, native identities, counters, full constraint
validation, durable commit, versions, and required recovery behavior remain
in scope. Timing and final eligibility must be proved separately.

Best single default-affinity full-service pair remains 108.272576s official
versus 16.445128s inline_copy (84.811364% less wall time). Three verified
calendar pairs, comprising one pair plus a documented continuation, measure
84.627281%; those are separate cohorts with large absolute timing drift.
The last parent-statistics/scratch-hint pair gives 113.174393s versus
18.202447s (83.916462%). Its 90% threshold is 11.317439s.
Four live databases from the latest and best single pairs passed an independent
full EXCEPT ALL/source/version audit plus catalog/durability checks.
Historical experiment details remain in ACTIVE_85.md.

## Current work

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
