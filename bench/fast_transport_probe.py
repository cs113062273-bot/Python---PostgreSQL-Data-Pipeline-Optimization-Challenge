"""Local server COPY plus fresh-execution single-statement metadata sessions.

Never caches query results or skips validation. Only one SELECT/WITH in native
unaligned tuple format can reuse a connection; DISCARD ALL after every command
matches fresh-session cleanup. Multi-command, DDL, CSV and the data transaction
retain their original transport/transaction boundaries. Session setup/close is
inside the full service timer. This is still an isolated research worker.
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
    if text is not None or cwd is not None or args.count('-c')!=1 or '--csv' in args:return False
    if not any(flag in args for flag in ('-At','-tA')):return False
    i=args.index('-c');command=args[i+1]
    if not re.match(r'^\s*(SELECT|WITH)\b',command,re.I) or re.search(r"\bE'",command,re.I):return False
    try:return len(PostgresAdminService._split_sql_statements(command))==1
    except Exception:return False


def close_sessions(service,report=None):
    state=service._expansion_timing_local
    sessions=getattr(state,'metadata_sessions',{})
    started=time.perf_counter()
    try:
        for session in sessions.values():session.close()
    finally:
        state.metadata_sessions={}
        diag=getattr(state,'local_backend_diagnostics',None)
        if isinstance(diag,dict):diag.update({'metadata_reuse_hook_sha256':source_identity(),'metadata_statements_reexecuted':getattr(state,'metadata_reexecuted',0),'metadata_sessions_opened':getattr(state,'metadata_opened',0),'metadata_sessions_closed':True,'results_cached':False})
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
            i=args.index('-c');command=args[i+1];base=args[:i]+args[i+2:];key=tuple(base)
            try:
                if key not in sessions:
                    sessions[key]=PsqlSession(base,timeout=timeout)
                    state.metadata_opened=getattr(state,'metadata_opened',0)+1
                session=sessions[key]
                # Constructor identity uses its own app name; restore the
                # original psql application's observable name for each query.
                session.send('SET application_name='+sql.Literal(os.environ.get('PGAPPNAME','psql')).as_string()+';')
                output=session.send(command+'\n;\n')
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
