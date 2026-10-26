# Current status

The validated atomic `bulk_fks` candidate leads the exact public QueryFoundry
control in local fresh-database SQL diagnostics. The next target, requested by
the user, is at least 50% less time than the original official implementation.
No official score or competition placement is claimed.

## Controlled results

Every run starts from a new clone of unchanged official generator output,
with six empty, unanalyzed destinations. All architecture setup, rebuilding
and FK validation are included in the SQL-stage sum. Complete preflight,
managed counters, versions, GUI/SSH and lock waits are outside this diagnostic;
commit latency is reported separately. Do not mix this with MEASURED PROCESSING TOTAL.

| Same cohort / scale | Official original | QueryFoundry original | Our bulk_fks | Ours vs official |
|---|---:|---:|---:|---:|
| 100k, median of 3 | 10.456s | 7.854s | 6.870s | 34.30% less time |
| 1M, single pilot | 103.975s | 91.322s | 64.920s | 37.56% less time |

Sources: `reports/fresh-bulk-100k.json`, `reports/fresh-bulk-1m-pilot.json`.
The 100k candidate is 12.54% below QueryFoundry's median; its slowest run is
below the control's fastest run. All six tables passed exact bidirectional
EXCEPT ALL once per mode, typed fingerprints/counts every run, and exact
schema/index/constraint and relationship checks. Strict bulk_fks fuzzing:
1,228 cases, zero observed differences (finite testing, not universal proof).

Fresh full local organizer-service pilots: baseline 11.016s wall, QueryFoundry
11.007s, bulk_fks 8.771s. Actual original preflight, counters and six version/
dependency registrations ran. These are separate, single-run wall diagnostics.
The bulk report's commit timer originally overlapped its FK restoration timer;
this reporting overlap is corrected in code and does not affect actual wall time.

## Current experiments and rejections

The 40.43% 100k pilot (bulk_array) is INVALID under the strict semantic policy:
a later seed found inputs where baseline raised 22P02 and the candidate accepted.
bulk_payload, bulk_full, bulk_full64, bulk_normalize and bulk_normalize64 also
have counterexamples. Their timing reports remain negative research evidence,
not effective performance claims. They are removed from GUI mode choices.

The original bulk_fks passed the same new seed (528 cases, zero mismatches).
The launcher keeps baseline, bulk_fks and the guarded_normalize research mode.
The new guarded mode first extracts TEXT without casts or filtering, proves a
conservative error-free numeric/date domain using immutable native operators,
and uses the original six statements/constraints/settings whenever the proof
fails. Inside the proven domain it reuses the extracted fields and bulk-validates
FKs. The gate and all preparation/validation costs are measured. Initial suite:
The latest sparse guarded version passed 634 cases: 262 fast-domain cases and
372 exact fallbacks, zero differences. Native domain tests passed 32 numeric/date
boundaries and 6,000 calendar/DateStyle cases; all added data functions pass the
unchanged immutable-function guard. Full local original-service tests passed
both fast and valid-fallback inputs, exact six outputs, counters, six version
records and compiler/batch provenance in version history. Thirty-six atomic
interruption boundaries passed. Final repeated timing remains pending.

Index rollback passed 34 termination/exception boundaries and seven metadata
fallback/refusal checks. Recovery alone does not make a semantically different
SQL candidate eligible. The new counterexamples are permanent fuzz cases.

## Recovery and release gates

The independent chunk protocol v2 retains active constraints, durable chunk
receipts, a persistent source/destination fence, actual-content source SHA-256,
original counters and an audited control-database version saga. Twelve final
interruption tests, actual local crash/WAL recovery, dump/restore and five
tamper scenarios passed. It is not combined with the atomic bulk strategy.
The 100k four-chunk pilot cost 20.028s including source hashes and finalization.
Atomic publication, cancellation/undo and default GUI resume remain unfinished.

All 32 organizer files and the exact vendor recipe module remain byte-preserved.
The default GUI remains baseline; experiments require an explicit mode.
Direct QueryFoundry lazy paths retain malformed-input counterexamples and are
controls, not the default safe candidate. jsonb_to_record remains lab-only.

Remaining gates: repeated fresh 1M scale confidence, standardized Linux/SSH and
actual GUI timing, GUI dump/version replay, larger scales, public repository
and exact public submission commit. Historical repeated-TRUNCATE timings kept
learned statistics and are superseded; their output-correctness evidence remains.


## Benchmark stability

A later unpinned pilot showed large regression even for unchanged bulk_fks;
no stable current 50% claim is made from a single run. The harness now permits
identical local measured-backend CPU affinity for all modes, uses FILE_COPY
clones (checkpointing completed before timing), records backend CPU as well as
elapsed time, and rotates mode order in a Latin square. Clone/setup remains
outside processing timing equally for every mode. Earlier timing reports remain
historical evidence under their recorded method. No antivirus, durability or
global OS priority setting was changed. User confirmed no Linux/SSH environment
is available; continue local verification and label this limitation explicitly.
