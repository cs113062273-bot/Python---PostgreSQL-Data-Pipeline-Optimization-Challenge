"""Isolated local validation adapter for the native phased psql primitive.

The production LocalService/default is not patched persistently. Worker mode
substitutes only the protected data batch inside this dedicated process.
"""
import os
import subprocess
import sys

import psycopg

from pipeline.psql_session import PsqlSession


def run(args,stdin_text,*,cwd,timeout=None,cpu=None):
    if cpu is not None:raise RuntimeError('Phased transport CPU-affinity diagnostics are not qualified')
    def option(flag,default=None):
        indexes=[i for i,x in enumerate(args) if x==flag]
        return args[indexes[-1]+1] if indexes else default
    host=option('-h',os.environ.get('PGHOST','127.0.0.1'))
    db=option('-d')
    if host not in ('127.0.0.1','localhost','::1') or not db or not db.startswith('lab_'):
        raise RuntimeError('This phased validation adapter requires a local disposable lab database')
    connection={'host':host,'port':option('-p',os.environ.get('PGPORT','55432')),'dbname':db,'autocommit':True,'connect_timeout':5}
    user=option('-U',os.environ.get('PGUSER'))
    if user:connection['user']=user
    def cancel_backend(pid,name,epoch):
        with psycopg.connect(**connection) as c:
            row=c.execute('SELECT pid FROM pg_catalog.pg_stat_activity WHERE pid=%s AND datname=current_database() AND usename=current_user AND extract(epoch FROM backend_start)=%s::numeric',(pid,epoch)).fetchone()
            if row:c.execute('SELECT pg_catalog.pg_terminate_backend(%s)',(pid,))
    session=None
    try:
        session=PsqlSession(args,cwd=cwd,timeout=timeout,cancel_backend=cancel_backend)
        with session:output=session.send(stdin_text)
        result=subprocess.CompletedProcess(args,0,output,''.join(session._errors))
    except RuntimeError as error:
        code=session.process.returncode if session is not None else 1
        result=subprocess.CompletedProcess(args,code or 1,'',str(error))
    if session is not None:
        result.local_backend_diagnostics={'transport':'native_phased_psql_validation','backend_pid':session.backend_pid,'backend_start_epoch':session.backend_start_epoch,'protected_batch_phases':1,'scope':'psql interprets the unchanged protected batch and binary COPY; constructor identity/phase framing are outside that data program. No CPU pin.'}
    return result


def main():
    # Only this isolated worker process sees the substitution. Ordinary -c
    # metadata commands and every other transport call retain the original.
    import pipeline.local_diagnostics as module
    original=module.run_psql
    def hooked(args,stdin_text,*,cwd,timeout,cpu=None):
        if isinstance(stdin_text,str) and 'BEGIN ISOLATION LEVEL REPEATABLE READ;' in stdin_text and 'pgdm_cross_table_hash_manifest' in stdin_text:
            return run(args,stdin_text,cwd=cwd,timeout=timeout,cpu=cpu)
        return original(args,stdin_text,cwd=cwd,timeout=timeout,cpu=cpu)
    module.run_psql=hooked
    from bench.service_run import main as service_main
    service_main()


if __name__=='__main__':main()
