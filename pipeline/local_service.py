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
import tempfile

from pipeline.upstream import PostgresAdminService


class LocalService(PostgresAdminService):
    supports_private_psql_workspace=True
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
        copy_directory=None
        setup_seconds=cleanup_seconds=0.0
        cleanup_errors=[]
        from pipeline.native_copy import MARKER
        if isinstance(stdin_text,str) and MARKER in stdin_text:
            if getattr(self,'bulk_recipe_mode',None)!='guarded_copy' or not getattr(self._expansion_timing_local,'bulk_fk_active',False):
                raise RuntimeError('Private COPY workspace requires the exact guarded native batch')
            from pipeline.recipes import ROOT
            parent=(ROOT/'.runtime'/'native_copy').resolve()
            parent.mkdir(parents=True,exist_ok=True)
            copy_directory=Path(tempfile.mkdtemp(prefix='copy_batch_',dir=parent)).resolve()
            if copy_directory.parent!=parent or not copy_directory.name.startswith('copy_batch_'):
                raise RuntimeError('Unexpected COPY workspace path')
            setup_seconds=time.perf_counter()-started
        try:
            result = subprocess.run(args, input=stdin_text, capture_output=True, text=True, encoding="utf-8", errors="replace",
                                    timeout=kwargs.get("timeout_seconds"), shell=False,cwd=copy_directory)
        finally:
            if copy_directory is not None:
                cleanup_started=time.perf_counter()
                from pipeline.recipes import ORDER
                # Delete only our six fixed basenames, never recursively.
                for table in ORDER:
                    path=copy_directory/(table+'.bin')
                    try:path.unlink(missing_ok=True)
                    except OSError as error:cleanup_errors.append(type(error).__name__+':'+path.name)
                try:copy_directory.rmdir()
                except OSError as error:cleanup_errors.append(type(error).__name__+':workspace')
                cleanup_seconds=time.perf_counter()-cleanup_started
                self._expansion_timing_local.copy_workspace_result={'cleanup_success':not cleanup_errors,'cleanup_errors':cleanup_errors,'setup_seconds':setup_seconds,'cleanup_seconds':cleanup_seconds}
        context = getattr(self._expansion_timing_local, "context", None)
        if context:
            self._append_expansion_timing(context["report"], context["stage"], context["name"] + " - LOCAL diagnostic command",
                time.perf_counter()-started, "local_psql_process_wall", include_in_total=context["remote_include_in_total"])
            if copy_directory is not None:
                # The big-batch envelope is diagnostic-only; explicitly count
                # filesystem setup/cleanup outside the PostgreSQL stage clocks.
                self._append_expansion_timing(context['report'],'bulk_load','Create private psql COPY workspace',setup_seconds,'local_filesystem_wall')
                self._append_expansion_timing(context['report'],'bulk_load','Remove private psql COPY files',cleanup_seconds,'local_filesystem_wall',details='; '.join(cleanup_errors) or 'All six fixed basenames removed; empty directory removed')
        if result.returncode:
            raise RuntimeError(result.stderr.strip())
        return result.stdout
