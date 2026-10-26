# Local Windows PostgreSQL incident, 2026-10-27

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
