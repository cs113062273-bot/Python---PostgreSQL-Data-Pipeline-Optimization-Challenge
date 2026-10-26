# Active 80% objective handoff

Goal is active. Do not mark complete on the SQL-only result or a single native
pilot; finish repeated native timing and recovery/dump/clean-room qualification.
The user has no Linux/SSH environment and requested local verification first.

Authoritative new evidence:

- Frozen SQL cohort commit `9b6e61c`, worktree `.runtime/bench_cache_80`:
  `reports/fresh-parallel-cache-1m-repeated.json`, fresh 1M n=3. Official median
  107.190432s, lab_parallel_cache 20.111344s: 81.237743% reduction. All six
  canonical outputs, restored schema/constraints and sequence options checked.
  Main/leader CPU 2 pin; workers use the ordinary server pool. SQL snapshots
  referenced by this report have been copied from the frozen worktree to reports/sql.
- Complete native-service pilot commit `8246596`:
  `reports/native-fresh-ctas-copy-1m-pilot.json` and its child report directory.
  Fresh 1M n=1, original preflight, counters, commit and six control versions:
  official wall 113.0089881s; guarded_ctas 22.5813069s (80.018132%);
  guarded_copy 20.9756955s (81.438914%). Exact six-table comparisons, protected
  schema, sequence options/ownership, workspace cleanup and versions passed.
  This cohort had no affinity override. It is not an official score.
- tests/semantics/native_copy.py: 121 actual captured psql-batch differential
  cases, zero differences, including actual COPY receipts/counters, rollback,
  schema/sequence/workspace checks. Report semantics-native-copy-pilot.json.
- tests/recovery/native_branches.py: full native CTAS fast, COPY valid fallback
  (`1e2`), and preexisting work-schema collision. Exact output/counters/six
  versions; unrelated sentinel preserved. recovery-native-branches.json.
- Earlier common-core tests: guarded_parallel and lab_parallel_bulk 642 cases
  each, zero differences; lab_parallel_cache 342 cases, zero differences;
  32 native sequence-cache metadata/rollback/termination/commit checks;
  nine additional unique-index fallback/rollback checks; 36 guarded_parallel
  atomic interruption boundaries. Prior qualified guarded_prefix has its own
  3,042 semantic/crash/dump/concurrency evidence, not new-COPY qualification.
- Latest unit suite 51 passes, all 32 organizer files byte-identical. Generator
  and fixed seed unchanged. fsync/full_page_writes/synchronous_commit remain ON.

Native implementation:

- pipeline/workspace.py creates a logged work relation in an otherwise absent
  pgdm_pipeline_work namespace, in the atomic data transaction. Collision/no
  CREATE permission chooses original fallback. DROP RESTRICT before COMMIT.
- guarded_ctas: parallel CTAS of exact transformed SELECT, then original
  INSERT/default identities, native counters and six versions. No filesystem.
- guarded_copy: psql binary-aware `\\copy` of the transformed SELECT to a
  private client directory, then `\\copy` into the original targets. Actual
  psql ROW_COUNT captured immediately in a temp receipt feeds the unchanged
  managed counter updates. Outside proof, exact original INSERT+GET DIAGNOSTICS.
  Generic `\\g` binary redirection was rejected after a Windows CRLF signature
  corruption; psql-binary-redirection-probe.json records actual bytes.
- LocalService creates/cleans a private per-batch cwd, only six fixed filenames;
  setup/cleanup costs counted. COPY is deliberately blocked on ordinary SSH
  transport until a private cwd/cleanup implementation is qualified. CTAS can
  use ordinary SQL transport. GUI/default not promoted yet.
- Native cache tuning uses real sequences, CACHE 1000 only under native known
  metadata, restores declared options. No manual IDs/sequence rewind. Ordinary
  rollback/preallocation gaps are allowed, checked metadata exact.

Next work:

1. Add stronger binary-layout metadata gate for COPY (e.g. changed target
   timestamp/timestamptz or int widths must select original fallback, not receive
   incompatible binary bytes). Current official fixed schema is already exact.
2. Consider identical local big-batch backend CPU affinity for a controlled
   repeated native cohort; baseline and candidate must share it, all overhead
   included/reported. If implemented, record actual backend PID/CPU. The SQL
   cohort already used CPU 2. Avoid source edits in a running benchmark; freeze
   a detached worktree if implementing independently.
3. New native COPY failure boundaries, actual private-server immediate crash,
   retry, dump/restore, concurrency; full native repeated 1M baseline/candidate.
4. Clean-room from final exact commit, docs/default/SSH readiness. Whole-batch
   shared stage is not yet bounded for 300M; chunk protocol remains independent.

Disk: about 20 GB free after native pilot. Before further 1M repeated clones,
reclaim only our reproducible oracle copies/old disposable SQL-only clones.
bench/clean_verification.py validates explicit completed receipts, six JSONB
tables and uses DROP RESTRICT. verification-scratch-cleanup.json audits an
earlier 9.03 GiB reclamation. Native service_run currently discards the returned
verification schema name; record it for future cleanup receipts. Do not delete
raw templates, canonical challenge_reference, successful chosen output or user
files. The old SQL-only lab_fresh clones have no control-version registrations
and are safer cleanup candidates than native-versioned databases.

No processes were running when this note was written. Last native pilot handle
17040 and frozen SQL handle 6777 both returned terminal success. Main tracked
code is commit 8246596, with many new reports still untracked; the user's Chinese
prompt is also untracked and must remain untouched. Do not spawn agents. The
unrelated Downloads/lab4_adv.sv is untouched.
