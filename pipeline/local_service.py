"""Local psql transport for integration tests; no SSH/official timing parity.

All organizer SQL, preflight, counters and version code still execute. This
transport refuses shell commands; dump filesystem paths require the real SSH
service and are not simulated as successful.
"""
import os
from pathlib import Path
import shlex
import subprocess
import time

from pipeline.upstream import PostgresAdminService


class LocalService(PostgresAdminService):
    def __init__(self, psql=None):
        super().__init__()
        self.psql = psql or os.environ.get("CHALLENGE_PSQL", "psql")
        self.sql_username = os.environ.get("PGUSER", "challenge")
        self.postgres_port = int(os.environ.get("PGPORT", "5432"))

    def run_remote_command(self, command, stdin_text=None, **kwargs):
        args = shlex.split(command)
        if not args or args[0] != "psql":
            raise RuntimeError("Local diagnostic transport supports psql only: filesystem/dump commands require SSH")
        args[0] = str(Path(self.psql).resolve()) if Path(self.psql).exists() else self.psql
        args[args.index("-h")+1] = os.environ.get("PGHOST", "127.0.0.1")
        args += ["-p", str(self.postgres_port)]
        if stdin_text is not None and not isinstance(stdin_text, (str, bytes)):
            stdin_text = stdin_text.read()
        if isinstance(stdin_text, bytes):
            stdin_text = stdin_text.decode("utf-8")
        started = time.perf_counter()
        result = subprocess.run(args, input=stdin_text, capture_output=True, text=True, encoding="utf-8", errors="replace",
                                timeout=kwargs.get("timeout_seconds"), shell=False)
        context = getattr(self._expansion_timing_local, "context", None)
        if context:
            self._append_expansion_timing(context["report"], context["stage"], context["name"] + " - LOCAL diagnostic command",
                time.perf_counter()-started, "local_psql_process_wall", include_in_total=context["remote_include_in_total"])
        if result.returncode:
            raise RuntimeError(result.stderr.strip())
        return result.stdout
