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
