# 80% candidate qualification audit

The target is at least 80% less processing time than the unchanged official
original under the same controlled workload, without weakening eligibility.
The broader user prompt also requests bounded shared extraction, durable chunk
resume, recovery consistency, reproducibility and final default integration.
Local validation takes priority because the user has no Linux/SSH test host.

This is a gate ledger, not a declaration of completion. Evidence for an older
mode is not automatically evidence for native COPY. A finite test suite is not
a universal semantic proof.

| Requirement | Current authoritative evidence | State |
|---|---|---|
| Complete-processing time reduced by 80% | Historical `native-fresh-copy-1m-repeated-v2.json`: medians 110.523788s to 20.030262s, 81.876967%. New fresh post-recovery pair: 107.541923s to 23.898150s, 77.777829% | Historical cohort passed; 80% is not stable across all rechecks. Not an official score |
| Independent integrity recheck | `ranked-cohort-live-audit-v4.json` and catalog supplement: eight databases, 48 bidirectional logical-table comparisons, 42,572,424 result rows, actual full source SHA/counts, six counters/versions, valid indexes/logged metadata; checker rejects deliberate faults | Passed tested retained datasets; does not diagnose server stop or prove universal eligibility |
| Repeated SQL processing result | `fresh-parallel-cache-1m-repeated.json`: three-run medians 107.190432s and 20.111344s, 81.237743%, frozen `9b6e61c` | Passed within SQL-only scope |
| Six exact outputs | Bidirectional EXCEPT ALL once per mode in the repeated cohort, every run typed fingerprints/graph/protected schema/sequence checks | Passed repeated cohort |
| Preserve malformed-input behavior | `semantics-native-copy-pilot.json`, 121 actual native batches, zero differences; conservative proof or original fallback | Passed tested cases |
| Safe binary receiving layout | `semantics-copy-layout.json`, six altered-type/index/check native cases choose original fallback | Passed tested cases |
| Actual counters and version provenance | Every repeated run retains original managed-counter effects and six control records, compiler/batch hashes | Passed repeated cohort |
| Restore every temporarily removed key/FK | Exact restored schema/constraints verified; native index and sequence metadata fallback/rollback suites | Passed tested metadata |
| Exceptions, disconnects and backend loss before commit | `recovery-native-copy-atomic.json`, 22 actual-batch fault cases | Passed tested boundaries |
| Real server crash and retry | `recovery-native-copy-server-crash.json`, immediate stop after real COPY before FK restore; WAL recovery, exact rollback and native retry/six versions | Passed; not a recovery performance score |
| Dump/version behavior | `recovery-native-copy-dump-values.json`, real custom pg_dump/pg_restore, same data DB name, retained control DB, source/schema/seq/counters/outputs/history, physical identity values and sequence last_value/is_called | Passed tested local path |
| Concurrent write during lock wait | `recovery-native-copy-concurrency.json`, observes lock wait, commits sentinel/counter, forces original fallback and preserves writer | Passed tested race |
| Explain unexpected Windows heap corruption | `native-unexpected-crash-dump.json`, `CRASH_INVESTIGATION.md` | Root cause unresolved; keep visible |
| Durability and unchanged generator | `verify_upstream.py` checks 32 organizer files; reports record fsync/full_page_writes/synchronous_commit ON | Passed local checks |
| Fresh source/dependency reproduction | `clean-room-native-copy-7b0f077.json`: new archive/venv/install, 51 compiler tests + six application tests, actual 64-row native DB execution with matching compiler and all output/counter/version/schema/sequence gates | Functional reproduction passed; not another clean-room 1M timing cohort |
| Exact-source repeated service timing | Detached `.runtime/bench_native_80` at `7b0f077`, compiler/driver hash checked before/after every run; batch SQL snapshots SHA-verified into root reports | Passed; overlapping initial attempt explicitly rejected |
| RAM/temp/stage/WAL efficiency | `native-resource-comparison-1m.json`, separate fresh instrumented probes, exact in-transaction stage size, sampled client/backend RSS/COPY files, WAL/temp counters, native outputs/counters/versions verified | Measured locally with sampling/shared-page limits; no physical-RAM or judge-resource-score claim |
| Bounded shared representation at 300M | Current logged normalized relation spans the whole source batch | Incomplete; do not claim bounded 300M operation |
| True durable chunk resume, source consistency and version saga | Separate `pipeline/checkpoints.py`, final 12 interruption tests; compatible source hash passes 57 cases. `bounded-native-copy-resume-final.json` adds nine fixture-only COPY primitive checks with four committed private chunks and 17-row stage cap | Managed saga exists separately; COPY primitive has no production fences/counters/versions or full error fallback |
| Native phased transport foundation | Five actual psql transport cases, 121 differential cases with zero differences, unpaired fresh 1M pilot 17.7371s | Foundation passed locally; pilot is not a new ranked comparison/default |
| Private-result publication visibility | `shadow-publication-primitives.json`: replacing table objects exposes new rows to an old RR snapshot; inserting into original objects preserves visibility/OIDs | Replacement rejected; original-object primitive passed, full managed publication pending |
| Fastest qualified default / ordinary SSH transport | GUI defaults to original; guarded_copy requires private local client workspace | Incomplete; do not expose an unqualified remote default |
| Final Kaggle writeup / public exact commit | Existing research docs and local Git commits | Final writeup pending; no public push or official score claimed |

Reports are under `reports/`. The completed complete-service cohort
uses fresh FILE_COPY databases, alternating starting modes, no affinity
override, and the original preflight/counters/versions. Its first run per mode
does exact six-table comparison; every run also checks fingerprints, schema,
sequence options, counters and version provenance. Clone/oracle verification
time is excluded; transformation, proof, staging, COPY I/O, cleanup, rebuilding,
validation, commit and six control registrations are included in processing wall.
