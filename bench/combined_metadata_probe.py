"""One native psql phase per metadata call; original SQL is always reexecuted.

Quiet framing hides only our setup/reset command tags. Original -c SQL still
uses one native gexec request; stdin retains native statement boundaries.
Rollback and DISCARD ALL run before returning. No result or validation cache.
"""
from contextvars import ContextVar
import hashlib
import os
from pathlib import Path
import subprocess
from psycopg import sql
from bench import date_shape_probe as shape,fast_transport_probe as fast,script_transport_probe as script,server_copy_probe as server
from pipeline.native_copy import MARKER
from pipeline.psql_session import PsqlSession

ENABLED=ContextVar('combined_native_metadata_enabled',default=True)


def source_identity():
    return hashlib.sha256((shape.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def install():
    shape.install()
    import pipeline.local_diagnostics as diagnostic
    from pipeline.local_service import LocalService
    underlying=diagnostic.run_psql
    def hooked(args,text,*,cwd,timeout,cpu=None):
        service=server.SERVICE.get()
        is_c=fast.eligible(args,text,cwd)
        is_stdin=script.eligible(args,text,cwd)
        if ENABLED.get() and service is not None and cpu is None and (is_c or is_stdin):
            state=service._expansion_timing_local
            sessions=getattr(state,'metadata_sessions',None)
            if sessions is None:sessions={};state.metadata_sessions=sessions
            base=list(args)
            if is_c:
                index=base.index('-c');command=base[index+1];del base[index:index+2]
            else:
                if '-f' in base:index=base.index('-f');del base[index:index+2]
            style,tuples,quiet=fast.native_format(base);base=fast.format_neutral_args(base);key=tuple(base)
            try:
                if key not in sessions:
                    sessions[key]=PsqlSession(base,timeout=timeout);state.metadata_opened=getattr(state,'metadata_opened',0)+1
                session=sessions[key];session.timeout=timeout
                framing='\\set QUIET on\n'
                if getattr(session,'metadata_format',None)!=(style,tuples,quiet):
                    framing+='\\pset format '+style+'\n\\pset tuples_only '+tuples+'\n'
                framing+='SET application_name='+sql.Literal(os.environ.get('PGAPPNAME','psql')).as_string()+';\n\\set QUIET '+quiet+'\n'
                if is_c:framing+='SELECT '+sql.Literal(command).as_string()+'\n\\gexec\n'
                else:framing+=text+'\n'
                framing+='\\set QUIET on\nSET client_min_messages=error;\nROLLBACK;\nDISCARD ALL;\n\\set QUIET '+quiet+'\n'
                output=session.send(framing);session.metadata_format=(style,tuples,quiet)
                state.metadata_reexecuted=getattr(state,'metadata_reexecuted',0)+1
                if is_stdin:state.metadata_stdin_reexecuted=getattr(state,'metadata_stdin_reexecuted',0)+1
                state.metadata_combined_phases=getattr(state,'metadata_combined_phases',0)+1
                return subprocess.CompletedProcess(args,0,output,'')
            except BaseException:fast.close_sessions(service);raise
        if ENABLED.get() and isinstance(text,str) and MARKER in text:text='-- lab combined native metadata source_sha256='+source_identity()+'\n'+text
        return underlying(args,text,cwd=cwd,timeout=timeout,cpu=cpu)
    diagnostic.run_psql=hooked
    registered=LocalService.register_cross_table_expansion_versions
    def register(self,*args,**kwargs):
        try:return registered(self,*args,**kwargs)
        finally:
            state=self._expansion_timing_local;diag=getattr(state,'local_backend_diagnostics',None)
            if isinstance(diag,dict):diag.update({'metadata_combined_hook_sha256':source_identity(),'metadata_combined_native_phases':getattr(state,'metadata_combined_phases',0),'metadata_queries_reexecuted_no_result_cache':True})
    LocalService.register_cross_table_expansion_versions=register


def main():
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
