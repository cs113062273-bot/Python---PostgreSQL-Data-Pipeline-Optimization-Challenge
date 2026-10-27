"""Local server COPY plus fresh-execution native metadata sessions.

Never caches query results or skips validation. Native psql gexec sends each
original -c SQL string as one request, preserving its transaction boundaries.
DISCARD ALL after every command matches fresh-session cleanup. The protected
data transaction retains its original transport. Session setup/close is inside
the full service timer. This is still an isolated research worker.
"""
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys
import time

from psycopg import sql
from pipeline.psql_session import PsqlSession
from pipeline.upstream import PostgresAdminService
from pipeline.native_copy import MARKER
from bench import server_copy_probe as server


def source_identity():return hashlib.sha256(Path(__file__).read_text(encoding='utf-8').encode()).hexdigest()


def eligible(args,text,cwd):
    if text is not None or cwd is not None or args.count('-c')!=1:return False
    if any(flag in args for flag in ('-f','-x','--expanded','-H','--html','-z','-0','-1','--single-transaction')):return False
    i=args.index('-c');command=args[i+1]
    # Windows psql's command-line code page differs from its UTF-8 pipe.
    # Preserve the original transport, including its failures, for such SQL.
    if os.name=='nt' and not command.isascii():return False
    if command.lstrip().startswith('\\') or re.match(r'^\s*COPY\b',command,re.I):return False
    if any('AUTOCOMMIT' in arg for arg in args):return False
    if re.match(r'^\s*(DROP|ALTER)\s+DATABASE\b',command,re.I):return False
    return True


def native_format(args):
    short={c for arg in args if re.fullmatch(r'-[qAXt]+',arg) for c in arg[1:]}
    style='csv' if '--csv' in args else 'unaligned' if 'A' in short or '--no-align' in args else 'aligned'
    tuples='on' if 't' in short or '--tuples-only' in args else 'off'
    quiet='on' if 'q' in short or '--quiet' in args else 'off'
    return style,tuples,quiet


def format_neutral_args(args):
    result=[]
    for arg in args:
        if arg in ('--csv','--no-align','--tuples-only','--quiet'):continue
        if re.fullmatch(r'-[qAXt]+',arg):
            if 'X' in arg:result.append('-X')
        else:result.append(arg)
    return result


def close_sessions(service,report=None):
    state=service._expansion_timing_local
    sessions=getattr(state,'metadata_sessions',{})
    started=time.perf_counter()
    errors=[]
    try:
        for session in sessions.values():
            try:session.close()
            except Exception as error:
                # A lost server can break every cached backend. Still reap all
                # native clients and retain the primary operation failure.
                errors.append(type(error).__name__+':'+str(error))
    finally:
        state.metadata_sessions={}
        diag=getattr(state,'local_backend_diagnostics',None)
        if isinstance(diag,dict):diag.update({'metadata_reuse_hook_sha256':source_identity(),'metadata_statements_reexecuted':getattr(state,'metadata_reexecuted',0),'metadata_sessions_opened':getattr(state,'metadata_opened',0),'metadata_sessions_closed':all(s._closed for s in sessions.values()),'metadata_session_close_errors':errors,'results_cached':False})
        if report is not None:service._append_expansion_timing(report,'version','Close reused native metadata sessions',time.perf_counter()-started,'local_process_wall')


def install():
    server.install()
    import pipeline.local_diagnostics as diagnostic
    from pipeline.local_service import LocalService
    underlying=diagnostic.run_psql
    def hooked(args,text,*,cwd,timeout,cpu=None):
        service=server.SERVICE.get()
        if eligible(args,text,cwd) and service is not None and cpu is None:
            state=service._expansion_timing_local
            sessions=getattr(state,'metadata_sessions',None)
            if sessions is None:sessions={};state.metadata_sessions=sessions
            i=args.index('-c');command=args[i+1];base=args[:i]+args[i+2:];style,tuples,quiet=native_format(base)
            neutral=format_neutral_args(base);key=tuple(neutral)
            try:
                if key not in sessions:
                    sessions[key]=PsqlSession(neutral,timeout=timeout)
                    state.metadata_opened=getattr(state,'metadata_opened',0)+1
                session=sessions[key]
                session.timeout=timeout
                if getattr(session,'metadata_format',None)!=(style,tuples,quiet):
                    session.send('\\set QUIET on\n\\pset format '+style+'\n\\pset tuples_only '+tuples+'\n\\set QUIET '+quiet+'\n')
                    session.metadata_format=(style,tuples,quiet)
                # Constructor identity uses its own app name; restore the
                # original psql application's observable name for each query.
                session.send('SET application_name='+sql.Literal(os.environ.get('PGAPPNAME','psql')).as_string()+';')
                # One returned field contains the entire original -c string.
                # gexec executes it natively, without splitting SQL ourselves.
                output=session.send('SELECT '+sql.Literal(command).as_string()+'\n\\gexec\n')
                # Fresh psql closes (and rolls back) any transaction left open
                # by -c. Do the same before resetting the reused connection.
                session.send('SET client_min_messages=error; ROLLBACK;')
                session.send('DISCARD ALL;')
                state.metadata_reexecuted=getattr(state,'metadata_reexecuted',0)+1
                return subprocess.CompletedProcess(args,0,output,'')
            except BaseException:
                close_sessions(service)
                raise
        if isinstance(text,str) and MARKER in text:text='-- lab metadata session reuse source_sha256='+source_identity()+'\n'+text
        return underlying(args,text,cwd=cwd,timeout=timeout,cpu=cpu)
    diagnostic.run_psql=hooked
    register=LocalService.register_cross_table_expansion_versions
    def registered(self,*args,**kwargs):
        try:return register(self,*args,**kwargs)
        finally:close_sessions(self,kwargs.get('timing_report'))
    LocalService.register_cross_table_expansion_versions=registered
    execute=LocalService.execute_cross_table_expansion
    def executed(self,*args,**kwargs):
        try:return execute(self,*args,**kwargs)
        except BaseException:close_sessions(self);raise
    LocalService.execute_cross_table_expansion=executed


def main():
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
