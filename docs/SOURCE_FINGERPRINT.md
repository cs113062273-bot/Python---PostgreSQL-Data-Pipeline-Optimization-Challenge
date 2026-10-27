# Compatible source-fingerprint acceleration

This optimizes only the independent durable chunk protocol's source integrity
passes. It does not change the native atomic COPY pipeline that reached 81.88%
less complete processing wall, and those two architectures are not combined.

The old protocol already computes SHA-256 of actual complete rows in PostgreSQL,
including raw JSON and metadata. It fetches batches of 10,000 hex checksums and
updates a Python SHA with `raw_id:checksum` followed by LF. Stored raw_hash alone
is not trusted.

A read-only 1M component cohort, three runs per mode, measured:

| Method | Median seconds | Actual worker observation |
|---|---:|---:|
| Existing batched JSON/hex | 20.840117 | 0 |
| Same format, TEXT COPY stream | 20.563448 | 0 |
| Same format, parallel TEXT COPY | 4.099382 | 7 |
| Different binary record framing, research only | 14.306256 | 0 |

Parallel TEXT COPY is 80.33% less time for this fingerprint component.
Every JSON variant produced the exact same legacy digest. This is not a new
whole-pipeline 80% claim; the binary experiment has a different hash contract
and is not used for production checkpoint receipts.

## Preserving existing jobs

`source_digest` now uses a single ordered COPY SELECT for the exact six native
source columns, an ordinary non-inherited heap without RLS, and a validated
non-deferrable native bigint primary key. COPY outputs only integer ID, colon, lowercase hex and LF;
there are no characters requiring text COPY escaping. The incremental stream
therefore matches the legacy bytes exactly, without a full-source client array
or a changed request/protocol. Unsupported source layouts use the old function.

For source heaps at least 16 MiB, query-local parallel/work_mem settings are
saved and restored. Small sources avoid forced worker startup. The heap-size
threshold is an execution-cost choice, not a generator/axis-count assumption.
SQL errors/cancellation revert SET LOCAL on rollback; successful calls restore
caller settings before return. No durability setting or recipe guard changes.

Actual PostgreSQL tests passed 57 cases: payload edits preserving raw_hash,
NULL/Unicode/control values, all source metadata fields, signed bigint edges,
three TimeZone settings, empty/repeated scopes, changed-layout/RLS/inheritance/
missing-primary-key legacy fallback,
and existing-job resume. A real job was interrupted after one committed chunk
using the old digest function; the new function resumed that same job, preserved
the first chunk timestamp and stored SHA, and produced exact six outputs,
correct counters and six versions. The large path matched the existing 1M digest
in 4.074873s with custom caller settings restored. A second large pass after
transactional trigger DDL also matched in 4.232606s. Actual COPY cancellation
also restored settings and left the connection usable.

All 12 interruption/idempotency cases and five source/destination/schema/counter/
version tamper cases passed again after integration. The TimeZone dependence
of the existing JSON-row hash contract is preserved; this change does not
silently redefine already-stored receipts. The initial altered-raw-id test
harness hit a prepared-result-type cache error; the isolated DDL matrix now
disables prepared statements equally for both compared implementations.

Sources: [component cohort](../reports/source-hash-probe-1m.json),
[compatibility and old-job resume](../reports/recovery-source-fingerprint-final-v2.json),
[12 interruption cases](../reports/recovery-streaming-source-final.json),
[five tamper cases](../reports/recovery-streaming-source-tamper-final.json),
[implementation](../pipeline/checkpoints.py).

Primary API references:
[PostgreSQL binary string/hash functions](https://www.postgresql.org/docs/17/functions-binarystring.html),
[Psycopg COPY streaming and parameters](https://www.psycopg.org/psycopg3/docs/basic/copy.html).

The next architecture work must bound the shared projection and integrate fast
loading with chunk receipts/source consistency. It must retain error behavior
across chunks and count all proof/hash/index/FK/version costs. A source keeper
connection is only a hypothesis: source SHARE locks conflict with rebuilding
table1's FK to raw_data from another backend. Do not remove source revalidation
or claim a complete lease protocol without resolving and testing that lock
handover and finalization gap.
