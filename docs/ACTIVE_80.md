# Active 80% objective handoff

The active goal remains the user's 80% challenge objective. The local 1M
performance threshold now has repeated complete-service evidence. Broader
submission gates in RELEASE_80_AUDIT.md remain incomplete; do not claim an
official score, Linux/SSH readiness or combined bounded chunk/COPY recovery.
The user has no Linux/SSH host and requested local validation first.

Authoritative repeated native result, frozen source 7b0f077 in detached
.runtime/bench_native_80 (retain the worktree):
reports/native-fresh-copy-1m-repeated-v2.json and its same-stem child directory.
Fresh FILE_COPY clone per run, no destination warmup, no CPU pin, alternating
starting modes, three runs/mode. Official 109.854989 / 110.523788 / 111.100602s;
COPY 20.030262 / 19.728010 / 20.746827s. Medians 110.523788 versus 20.030262s:
81.876967% less complete processing wall. Every paired run exceeds 80%.
Native included-item medians 110.340067 versus 19.940298s: 81.928325% less
locally accounted time. All preflight/counters/proof/staging/COPY/rebuild/FK
validation/commit/six versions included. Every child validated schema/sequence/
namespace/counters/six versions; exact EXCEPT ALL once/mode, fingerprints every
run. Frozen compiler ce4f60cd7aae88e708301ccbcbe8b97dbafebded5cf8ea4cf34bd0f21028fbfd.
Batch SQL copied into root reports/sql after normalized-LF SHA verification.
The overlapping first attempt was deliberately aborted before accepting any
timing; native-fresh-copy-1m-overlap-rejected.json is retained. Accepted handle
63153 is terminal success. Do not resume/restart it.

The earlier SQL n=3 81.237743% cohort is a different metric/source (9b6e61c in
.runtime/bench_cache_80); retain its explicit scope. Full-service n=1 pilot is
113.008988 versus 20.975695s (81.438914%); superseded for ranking by n=3 above.

Additional completed local qualification:
- recovery-native-copy-atomic.json: 22 actual native COPY exception/backend
  termination boundaries; native-copy differential 121 zero differences;
  semantics-copy-layout.json six altered-type/index/check fallback cases.
- recovery-native-copy-server-crash.json: actual immediate stop after COPY
  before FK restore, exact WAL rollback of schema/outputs/source/counters/seq
  options, full native retry/six versions. Handle 47169 terminal success.
  Recovery 283.84s includes fsync of the whole huge retained research cluster;
  not a competition recovery duration. Do not stop a running benchmark again.
- recovery-native-copy-dump-values.json: real same-name custom dump/restore,
  all source/physical identity rows/graph/schema/sequence options AND last_value/
  is_called/counters/control history preserved. Earlier dump receipt retained.
- recovery-native-copy-concurrency.json: concurrent target commit during lock
  wait becomes visible, selects original fallback and preserves writer/counters.
- clean-room-native-copy-7b0f077.json: fresh archive/venv/locked install, 51
  compiler tests + six app tests + 32 upstream byte hashes. Actual service from
  that archive/venv also passed 64-row exact/counter/version/schema/seq checks,
  matching compiler SHA. Not a separate clean-room 1M timing cohort.

Separate resource probes, all terminal success (55407 initial, 91628 v2,
82613 baseline), source compiler matches ranked cohort:
reports/native-resource-comparison-1m.json and docs/NATIVE_RESOURCES.md.
COPY stage exact 186130432 bytes, private files sampled peak 287041703 bytes;
7 workers observed, summed PostgreSQL RSS about 1750.86 MiB versus baseline
181.21 MiB (shared pages double-counted; not physical RAM). Observed WAL about
659.86 versus 3087.58 MiB, 78.63% lower. Temp about 229.10 versus 74.69 MiB.
Read precise sampling/window limits. The first resource report corrected an
invalid external uncommitted-stage lookup to unknown, not zero. New
bench/native_resource_worker.py reads stage size in its owning transaction,
archives actual instrumented SQL SHA and updates native version provenance.
No ranked run imports that hook.

Unexpected earlier Windows fault remains unresolved:
05:53:29 PID19876 0xc0000374, postmaster reinitializes05:53:33, ready05:58:47.
It preceded any deliberate stop in native crash testing. Limited dump metadata
shows --forkchild and raw candidate addresses near directory/timezone routines;
these are not unwound frames or proof of root cause. See CRASH_INVESTIGATION.md.
All subsequent new tests and cohorts passed without repeating that heap fault;
do not claim that this explains or resolves it. Original dump remains outside
Git at the user's Local CrashDumps directory; no security/OS/binary change made.

Current open gates:
1. Whole-batch logged normalized stage and COPY files still scale with batch;
   bounded shared staging at 300M is not established.
2. pipeline/checkpoints.py provides separate durable chunk/source-fence/control
   saga recovery, not combined with fast COPY. Atomic COPY pre-commit failure
   retries the batch; post-commit control gap retains original service behavior.
   Client hard-kill/SSH behavior is not proved by backend termination tests.
3. COPY is blocked on ordinary SSH until a private cwd/cleanup transport is
   implemented and qualified. CTAS uses ordinary SQL transport. GUI/default
   still original/older guarded choices; do not promote unqualified modes.
4. Full official GUI/Linux/SSH/dump replay/public exact submission commit and
   300M resource claims remain unverified. Local 80% result is real but not a
   completed end-to-end contest submission.

Additional bounded/native research since bccdf50:
- source_digest preserves protocol 1/2 bytes with ordered TEXT COPY, actual
  six-column/native-PK/no-RLS/no-inheritance guard and exact legacy fallback.
  57 real-DB compatibility/cancel/old-job-resume cases passed; source hash
  component median 20.8401 -> 4.0994s, not a new complete-pipeline score.
- Persistent native PsqlSession passed five actual transport checks and the
  isolated phased candidate passed 121 semantic cases with zero differences.
  A fresh 1M pilot was 17.7371s, UNPAIRED: retain 81.876967% as ranked result.
- Publication by table replacement is rejected: older RR reader saw 64 rows
  outside its snapshot; original-object publication passed visibility/OID gates.
- bounded-native-copy-resume-final.json: nine fixture-only checks, four durable
  private chunks, 17 normalized rows maximum, 24 actual COPY receipts, real
  backend loss/valid binary corruption rollback and exact six final outputs.
  Actual restart verifier rejects payload/identity/extra-row/source tampering.
  This is NOT managed counter/version/fencing or performance qualification.
- A 997-row stage inside the 1M source stayed bounded; fused legacy/binary
  source hashes lost to separate projection+hash, and were not promoted.
- Read BOUNDED_COPY_RESEARCH.md and SOURCE_FINGERPRINT.md for exact scope.
  Final existing chunk regression: recovery-streaming-source-final.json,
  all 12 interruption/idempotency cases passed. No new ranked cohort running.

Disk after resource probes about 17 GB free. Only our reproducible verification
schemas were removed after n=3 completion (two copies, 2.865 GiB); receipts and
candidate/native-versioned databases remain. bench/clean_verification.py now
accepts completed native receipts and enforces this private local cluster,
exact six jsonb tables, NOWAIT locks and DROP RESTRICT. Do not remove canonical
references, chosen outputs, control versions or user files. Existing clean_clones
only supports explicitly disposed SQL-only clones, not native-versioned DBs.

Connection in every fresh shell: PGHOST127.0.0.1, PGPORT55432, PGUSERchallenge,
CHALLENGE_PSQL absolute root/.runtime/postgresql/pgsql/bin/psql.exe. Python is
root/.venv/Scripts/python.exe. fsync/full_page_writes/synchronous_commit ON;
no UNLOGGED durable state, no generator/seed change, no volatility spoof.
Do not spawn agents. Preserve the untracked user Chinese prompt and unrelated
Downloads/lab4_adv.sv. Consult live processes before assuming any handle running;
the last listed handles above are terminal, no deliberate jobs remain.
