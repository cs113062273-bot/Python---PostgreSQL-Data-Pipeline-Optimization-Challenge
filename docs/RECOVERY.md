# Experimental chunk recovery

`pipeline.checkpoints.ChunkRunner` and `pipeline.version_receipts` are opt-in
research code, separate from the preserved application. Supported scope is the
exact six recipes and initially empty destinations, positive raw IDs, with one
active expansion per database. No GUI integration or official eligibility is
claimed for this prototype.

Preflight retains source existence/columns, source version coverage, recipe
source/destination safety, native immutable function checks, row counters and
managed dependency validation. The blanket trigger guard receives one narrow
exception for the exact, verified read-only statement fence installed by this
protocol; every other trigger, rule and unsupported relation is still rejected.
Definitions are checked again against a stored catalog signature after locks.

1. Acquire a database-scoped session advisory lock before any transaction
   snapshot. Concurrent requests serialize; a new UUID cannot take an active
   job's database. Full request metadata, actor, workstation, recipe, scope,
   source version references and chunk size bind the UUID by SHA-256.
2. In one transaction lock source and all six destinations against writers,
   install durable statement fences, fingerprint actual source rows (JSON and
   metadata), create the logged job, and load global table3/table4 dimensions.
   Dimensions are built once so DISTINCT keys are never duplicated per chunk.
3. Process ordered raw-ID ranges. Within each transaction load table1, table5,
   table6 and table2 using the upstream managed DML counter wrapper; persist
   counts and the completed range in that SAME transaction. No ON CONFLICT
   shortcut hides errors. Dependent main_row joins also carry the implied raw-ID
   bounds so they can use bounded index ranges as table1 grows. Only one bounded
   list of chunk raw IDs is in Python; verify actual plans at larger scales.
4. A failed chunk rolls back its data, counters and checkpoint. Committed chunks
   survive and retain their original completion timestamp on replay. A new
   backend authenticates its job against durable state before writing.
5. After all chunks, revalidate source content/catalog, relationships and typed
   six-table fingerprints. Commit `data_complete` with the full source manifest
   and versioning metadata. Stored raw_hash is not a proof of payload integrity:
   actual SHA-256 hashes of canonicalized source JSON/metadata are recomputed.
6. In the separate control database, retain the organizer's six version and
   dependency inserts, and commit their audit receipt in the same control
   transaction. A retry verifies the exact version/dependency rows. Lost control
   replies reuse the receipt rather than adding another six versions.
7. A final data transaction marks `complete`, removes all fences, and releases
   the active database slot. A crash before this step resumes finalization only.
   Ordinary application operations again pass its unchanged trigger guard.

This is a recoverable saga, not a distributed atomic transaction. During loading
or pending finalization, readers can see complete committed chunk prefixes plus
global dimensions. The final version is not advertised until all data commits.
Direct readers must check job state; this prototype does not provide atomic
publication to arbitrary readers. An invalid later chunk leaves earlier chunks
and an active fence for investigation; automatic cancellation/undo is not yet
implemented. These restrictions prevent enabling recovery in the default GUI.

The persistent source fence rejects INSERT/UPDATE/DELETE/TRUNCATE across client
death and server restarts. It relies on normal PostgreSQL privilege boundaries;
owners/superusers can disable triggers. A bypass preserving raw_hash is detected
on resume by content rehash. Arbitrary adversarial DDL during active execution
is not supported. Catalog signatures cover columns, constraints, indexes,
trigger enablement/function text and server version; typed output hashes are
noncryptographic diagnostics, not adversarial proof. Fresh completion performs
three full source scans (initialization, data completion, version finalization),
and resume adds revalidation scans. Measure this cost before claiming efficiency.

`python -m tests.recovery.run` tests 12 boundaries, exact six-table outputs,
counter reconciliation, duplicate retries, changed-request rejection and fences.
`python -m tests.recovery.tamper` tests preserved-hash source edits, same-count
destination edits, schema rename and changed version dependency rejection.
`reports/recovery*.json` contains actual results. Real SSH, Linux restart, power
loss and GUI termination remain separate release gates.


## Independent atomic bulk strategy

The opt-in bulk strategies execute DDL, six inserts, original counter updates,
index rebuilding and full FK validation inside one data transaction. They do
not persist partially missing constraints across transactions. A failure rolls
back both schema and destination contents; the caller may retry the entire
expansion. Identity sequences may retain ordinary rollback gaps. Data/control
version finalization still follows the original service behavior; the separate
chunk protocol above remains the recoverable version-saga prototype.

`python -m tests.recovery.bulk_atomic --mode bulk_full` tests 34 exception and
backend-termination boundaries, comparing all six empty destination contents
and exact restored constraints/indexes. See recovery-bulk-full.json. These
atomic tests do not claim chunk reuse or cover power loss / remote Linux.


The guarded compiler additionally passed a real immediate stop of the private
PostgreSQL cluster after all six managed inserts but before FK restoration or
commit. After WAL recovery, schema, destination contents, source SHA and native
counters matched the pre-transaction state. Original-service retry and six
version registrations then passed exact comparison. A native custom-format dump
and same-name restore preserved source/data/constraints/indexes/counters and
control-version history. Reports: recovery-guarded-server-crash.json and
recovery-guarded-dump.json. Startup time includes fsync of the whole private
cluster with many retained experiment databases; it is not a recovery-speed
score. Post-commit version-gap recovery remains the original atomic service
behavior; durable chunk/saga recovery is still an independent prototype.


The production SQL prelude now acquires source and all target locks before the
first RR data snapshot. A controlled target writer commits a sentinel row and
its counter while our job waits; the later empty-target proof sees it, falls back
to original SQL, preserves it and maintains exact counters. See
recovery-guarded-concurrency.json. This closes a timing-table/snapshot race; the
ranked SQL diagnostic driver already acquired locks in that order.

The 80% native COPY successor at `7b0f077` now has its own qualification:
22 real-batch exception/backend-termination cases, actual private-server
immediate stop and complete native retry, same-name pg_dump/pg_restore and
concurrent-writer fallback. The strengthened dump check preserves physical
identity values plus sequence last_value/is_called as well as sequence options,
source SHA, logical graph, schema, counters and six version records. Reports:
recovery-native-copy-atomic.json, recovery-native-copy-server-crash.json,
recovery-native-copy-dump-values.json and recovery-native-copy-concurrency.json.
These are local Windows checks; atomic COPY still retries the whole transaction
before commit and retains the original post-commit control-version gap. The
independent durable chunk saga is not combined with COPY. Client hard-kill /
SSH publication behavior is not established by backend-kill tests alone.
