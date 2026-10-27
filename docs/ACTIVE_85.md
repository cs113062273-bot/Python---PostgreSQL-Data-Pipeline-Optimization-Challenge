# Active target: at least 85% less complete processing time

The user superseded the earlier 95% target with an explicit 85% objective.
Acceptance is a fresh multi-run official/candidate comparison, complete-service
time and all original correctness/counter/version/schema/durability checks.
At the latest official 107.541923s, 85% requires <=16.131288s; the threshold
must always use the actually paired official run, not an older denominator.

The target is not yet achieved. Latest paired result remains 77.777829%:
107.541923s official versus 23.898150s ordinary guarded_copy. Historical n=3
81.876967% is retained separately, never an every-run guarantee.

New isolated transport direction:
- Native server-file binary COPY in a fresh task-owned loopback directory,
  existing file privileges checked, client COPY fallback otherwise. No role
  grant, PROGRAM/FREEZE/manual IDs or weaker durability. Requires the local
  server OS account to access that directory; ordinary SSH remains unqualified.
- 31 native semantic cases passed, zero differences. A separate unprivileged
  no-file-role TEMP-table test fell back to client COPY and retained six real
  COPY receipts (18 rows) and native default identities.
- First 1M server-file pilot: 17.742858s, fresh FILE_COPY, no destination warmup,
  exact six outputs/counters/six versions/schema/sequence/file cleanup passed.
  It is unpaired and does not establish 85% or replace either earlier cohort.
- Native metadata SELECT-session reuse smoke: 64 rows, exact outputs/counters/
  versions/schema passed; 20 metadata statements actually reexecuted through
  three sessions and no result cache. Further native batch/format isolation
  testing and full-scale timing are required before promotion.
- OFFSET projection barriers were tested with the same seven actual workers
  as the array barrier; both OFFSET variants were much slower on 100k and are
  rejected. Parallel worker count alone does not establish an improvement.

Original atomic compiler remains ce4f60cd7aae88e708301ccbcbe8b97dbafebded5cf8ea4cf34bd0f21028fbfd;
actual transformed batch SQL and hook hashes are retained in reports/control
versions. Default production/compiler paths are unchanged. Preserve original
generator/seed, native benchmark DBs/control history and the user's prompt/HDL.
