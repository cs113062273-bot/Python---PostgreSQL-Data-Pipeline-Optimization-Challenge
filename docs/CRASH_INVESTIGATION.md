# Local Windows PostgreSQL incident, 2026-10-27

Later availability observation (2026-10-28): no postgres process or listener
remained on the private port, although the PID file was present. The last
prior log reported Windows shared-memory reservation error 487. This does not
establish why the process subsequently stopped. The cluster was recovered on
127.0.0.1:55432, with fsync/full_page_writes/synchronous_commit ON. An initial
restart omitted the original port options; it was stopped during recovery and
restarted with explicit original options, without configuration-file changes
or task SQL on the temporary port. Details are in
[the availability receipt](../reports/private-cluster-availability-20261028.json).
No speedup or scored recovery duration is inferred from this event.

At 05:53:29 Asia/Taipei, postgres.exe PID 19876 exited with exception
`0xc0000374` (heap corruption). The postmaster logged the child failure at
05:53:33, terminated the other processes and performed automatic WAL recovery.
It was ready for connections again at 05:58:47. This occurred between tests,
before the native COPY deliberate server-crash test issued any stop command.
That test's first attempted connection failed because recovery was already
running; it was not a successful deliberate-crash test.

The preserved read-only
[minidump metadata](../reports/native-unexpected-crash-dump.json) identifies
the `--forkchild` startup mode and candidate code pointers in `readdir`,
`opendir`, `AllocateDir` and `pg_open_tzfile`. These are raw executable-address
candidates found on the captured stack, **not an unwound call stack**. Nearest
export names alone do not identify the actual function or corruption origin.
There was no server log line identifying a running SQL statement. This evidence
suggests investigating startup/directory/timezone handling but does not prove
that the optimization, PostgreSQL itself or another component caused the fault.

Relevant primary source for the exact server tag:
[Windows directory routines](https://github.com/postgres/postgres/blob/REL_17_11/src/port/dirent.c)
and [timezone file lookup](https://github.com/postgres/postgres/blob/REL_17_11/src/timezone/pgtz.c).
No PostgreSQL binary, timezone setting, OS security setting or durability
setting was changed to conceal or bypass the incident.

The metadata reader retains module basenames, exception metadata and executable
code addresses only. It omits raw memory, SQL, complete command lines and
environment values. The original minidump remains outside version control.
The recovery log spends roughly 314 seconds fsyncing this large retained
research cluster, then milliseconds replaying WAL. That duration is not a
competition recovery score or a measurement of one batch's replay cost.

Root cause remains unresolved. Subsequent deliberate crash/retry and dump
checks must be recorded separately and cannot retroactively explain this fault.

Later, when work resumed, the private server was absent and pg_ctl reported no
running server. The last old log checkpoint completed at 13:12:51; at 13:11:27
the postmaster had logged Windows error 487 reserving shared memory for a child.
No new postgres crash dump was found. Neither that message nor the absence of
a dump proves why the server later stopped. This is distinct from the earlier
confirmed heap-corruption exception; do not conflate them.

The private cluster was started again at 17:39:15 and ready at 17:42:31 after
fsync/WAL recovery, with no durability settings weakened. Subsequent fresh
official/candidate runs and eight-database exact logical-row/source/counter/
version/catalog checks passed. They show retained committed data consistency,
not a diagnosed stop cause or a scored recovery-duration result. Original log
tail/SHA and chronology: unexpected-postgres-unavailable-audit.json.
