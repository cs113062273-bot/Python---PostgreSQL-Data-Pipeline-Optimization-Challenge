# Bounded native COPY and publication experiments

The ranked local result remains 81.876967% less complete processing wall,
three runs per mode at frozen source `7b0f077`. None of these component or
small-fixture experiments replaces that paired result or completes submission
qualification.

## Persistent native transport

`pipeline/psql_session.py` keeps one native psql process/connection alive across
complete SQL phases. psql executes SQL, metacommands and binary-aware COPY;
Python does not interpret SQL or serialize data rows. Random meta-echo markers
preserve the actual COPY ROW_COUNT across phases. Separate pipe-reader/writer
threads keep deadlines active even when stdin exceeds pipe capacity. Optional
cancellation checks the backend PID plus backend start instant to avoid PID
reuse. Closing an uncommitted session rolls it back.

The five actual PostgreSQL tests cover binary Unicode/control/NULL values and
native identities, real command-tag capture, gset/conditional branches, literal
SQL LF/CRLF/CR bytes, EOF rollback, native SQLSTATE and query/blocked-input
timeouts. Windows text-mode stdin is intentional: a direct binary-stdin probe
changed CRLF inside SQL literals through Windows psql's C text input. This does
not affect binary COPY files, which psql opens in binary mode.

The isolated adapter `bench/phased_transport.py` passed 121 native semantic
cases against the original one-shot baseline, with zero differences. Its fresh
1M complete-service pilot took 17.7371s with exact outputs/schema/sequence/
counters/six versions. This is one unpaired pilot, not a qualified improvement
over 20.0303s or a new percentage. The initial attempt rejected `-f -` before
the protected data batch; that failed attempt is retained separately. No GUI
default or ordinary SSH transport has been changed.

## Bounded shared projection

The 1M probe discovers the next high-water PK with ORDER BY/LIMIT and constrains
the normalized stage to that range. A separate 997-row slice within the 1M
source produced exactly 997 stage rows and at most 270336 bytes in all three
variants, then rolled back all scratch DDL. It does not establish 300M resource
bounds for the complete pipeline.

Fusing per-row source hashing into the 1M projection did not win: plain stage
plus a separate legacy hash was about 8.18s; fused legacy hash plus its ordered
stage reduction was about 9.23s. A binary-hash fusion has a different receipt
contract and also lost to its separately parallelized reference. Neither is
promoted. The compatible source-hash acceleration is documented separately in
SOURCE_FINGERPRINT.md.

## Publication choice: keep the original objects

Both small publication strategies survived an injected rollback and matched
the six-table canonical result/schema/sequence options. However, moving old
public objects aside and renaming new ones into place failed the older-reader
gate: a repeatable-read transaction whose snapshot preceded publication saw 64
new table1 rows. It also changed public table OIDs. This strategy is rejected.

Copying the already-native rows into the original public objects preserved
those OIDs and the old reader saw zero rows, as required. Native identities are
allocated by private tables' native defaults; publication transfers those
actual stored IDs and advances sequence state only forward. It does not invent
IDs or rewind sequences. This extra publication copy has a real I/O/time cost
that must be included in any future full-service benchmark. Ordinary sequence
allocation gaps may remain after failed transactions.

## Four committed chunks and restart integrity

`bench/bounded_copy_resume_probe.py` is a fixture-only experiment with sparse
negative source IDs, 64 rows and a strict 17-row stage cap. All six private
tables are logged and retain keys/checks/FKs. Dimension DISTINCT is accumulated
as set union; the four remaining recipes use the shared bounded normalized
stage. Only proven fixture input is accepted; this is not a general malformed
input fallback policy.

Each native COPY records its real command tag, file SHA and actual native ID
range in the same transaction as its data. For the explicit smallint dimension,
the receipt stores actual keys instead. Zero-row COPY avoids undefined currval.
The private native sequences are checked for increment/cache 1 and no cycle.
Restart verification exports actual payload columns in native ID order, compares
binary file SHA/range/count, checks whole-target totals and recomputes the actual
complete source fingerprint. It does not trust supplied raw_hash or row count
alone. Python hashes byte blocks and does not serialize individual rows.

Nine checks passed: first private commit, valid binary payload corruption
rollback, actual backend termination after an in-flight table1 COPY, resumed
four-chunk completion without rewriting the first six receipts, four actual
verifier rejections (same-count payload edit, shifted identity, extra row,
source edit preserving raw_hash), and final exact six outputs with original
objects/schema/sequence options/full FKs. The maximum stage was 17 rows/16384
bytes, with 24 committed COPY receipts including five zero-row copies. Original public targets remained empty
through private loading. Final publication in this experiment uses ordinary
INSERT SELECT; it does not yet test a final native COPY optimization.

This is not the managed chunk service. Concurrent durable fencing/ownership,
whole-input error semantics/fallback, actual managed counters and control-version
saga, client hard-kill, dump/resume and a paired full-service scale benchmark
remain open. In particular, source SHARE locks on a separate keeper backend
conflict with rebuilding the FK to raw_data from another backend. Lock handover
and publication must be tested before any keeper design is adopted.

Evidence: [transport tests](../reports/recovery-psql-session-file-stdin-final.json),
[121 semantic cases](../reports/semantics-phased-native-copy.json),
[unpaired 1M pilot](../reports/local-service-phased-native-copy-1m.json),
[997-row stage](../reports/chunk-projection-bounded997.json),
[1M fusion comparison](../reports/chunk-projection-probe-1m-v2.json),
[publication alternatives](../reports/shadow-publication-primitives.json),
[bounded COPY restart](../reports/bounded-native-copy-resume-final.json).
