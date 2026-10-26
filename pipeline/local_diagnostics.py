"""Optional identical local backend affinity for controlled measurements.

No submission tuning: only the explicitly requested benchmark backend is pinned.
Its psql connection waits idle before receiving SQL; no timed query is suspended.
"""
import os
import subprocess
import time
import uuid


def run_psql(args,stdin_text,*,cwd,timeout,cpu=None):
    options=dict(capture_output=True,text=True,encoding='utf-8',errors='replace',timeout=timeout,shell=False,cwd=cwd)
    if cpu is None:return subprocess.run(args,input=stdin_text,**options)
    import psycopg
    import psutil
    host=args[args.index('-h')+1]
    if host not in ('127.0.0.1','localhost','::1'):raise RuntimeError('Diagnostic CPU affinity requires the local server')
    if '-c' in args:raise RuntimeError('Pin only the authored stdin batch, not command-line SQL')
    tag='pgdm_diag_'+uuid.uuid4().hex
    process=subprocess.Popen(args,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding='utf-8',errors='replace',cwd=cwd,env={**os.environ,'PGAPPNAME':tag},shell=False)
    started=time.perf_counter();diagnostics=None
    try:
        with psycopg.connect(host=host,port=args[args.index('-p')+1],user=args[args.index('-U')+1],dbname='postgres',autocommit=True,connect_timeout=10) as observer:
            deadline=time.monotonic()+10
            while process.poll() is None and time.monotonic()<deadline:
                row=observer.execute('SELECT pid,state FROM pg_stat_activity WHERE application_name=%s',(tag,)).fetchone()
                if row:
                    pid,state=row
                    backend=psutil.Process(pid)
                    if 'postgres' not in backend.name().lower():raise RuntimeError('Unexpected process for PostgreSQL backend')
                    original=backend.cpu_affinity()
                    backend.cpu_affinity([int(cpu)])
                    diagnostics={'backend_pid':pid,'requested_cpu':int(cpu),'original_affinity':original,'measured_affinity':backend.cpu_affinity(),'state_before_sql':state,'setup_wall_seconds':time.perf_counter()-started,'scope':'Leader backend only; ordinary parallel workers keep server affinity. Applied before SQL, equally to official and candidates; full operation wall includes setup.'}
                    break
                time.sleep(0.01)
        if diagnostics is None and process.poll() is None:raise RuntimeError('Local diagnostic backend did not become identifiable')
        stdout,stderr=process.communicate(input=stdin_text if process.poll() is None else None,timeout=timeout)
        value=subprocess.CompletedProcess(args,process.returncode,stdout,stderr)
        value.local_backend_diagnostics=diagnostics
        return value
    except BaseException:
        if process.poll() is None:process.kill()
        process.communicate()
        raise
