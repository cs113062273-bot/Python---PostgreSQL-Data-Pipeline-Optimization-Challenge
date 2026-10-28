"""Optional Windows leader CPU observation, with no affinity or SQL changes."""
import ctypes
from ctypes import wintypes
import hashlib
import os
from pathlib import Path
import subprocess
import time
import uuid


def source_identity():return hashlib.sha256(Path(__file__).read_text(encoding='utf8').encode()).hexdigest()


def run(args,text,*,cwd,timeout,cpu=None):
    if os.name!='nt' or cpu is not None:raise RuntimeError('CPU observation is a separate Windows diagnostic without affinity')
    import psycopg
    import psutil
    host=args[args.index('-h')+1]
    assert host in ('127.0.0.1','localhost','::1') and '-c' not in args
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.OpenProcess.argtypes=[wintypes.DWORD,wintypes.BOOL,wintypes.DWORD];kernel.OpenProcess.restype=wintypes.HANDLE
    kernel.GetProcessTimes.argtypes=[wintypes.HANDLE]+[ctypes.POINTER(wintypes.FILETIME)]*4;kernel.GetProcessTimes.restype=wintypes.BOOL
    kernel.CloseHandle.argtypes=[wintypes.HANDLE];kernel.CloseHandle.restype=wintypes.BOOL
    def times(handle):
        created=wintypes.FILETIME();exited=wintypes.FILETIME();system=wintypes.FILETIME();user=wintypes.FILETIME()
        if not kernel.GetProcessTimes(handle,ctypes.byref(created),ctypes.byref(exited),ctypes.byref(system),ctypes.byref(user)):raise ctypes.WinError(ctypes.get_last_error())
        number=lambda value:(value.dwHighDateTime<<32)|value.dwLowDateTime
        return number(created),number(system),number(user)
    tag='pgdm_cpu_'+uuid.uuid4().hex
    process=subprocess.Popen(args,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding='utf8',errors='replace',cwd=cwd,env={**os.environ,'PGAPPNAME':tag},shell=False,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    handle=None;started=time.perf_counter()
    try:
        with psycopg.connect(host=host,port=args[args.index('-p')+1],user=args[args.index('-U')+1],dbname='postgres',autocommit=True,connect_timeout=10) as observer:
            deadline=time.monotonic()+10;row=None
            while process.poll() is None and time.monotonic()<deadline:
                row=observer.execute('SELECT pid,state,extract(epoch FROM backend_start)::float8 FROM pg_stat_activity WHERE application_name=%s',(tag,)).fetchone()
                if row:break
                time.sleep(.01)
        if row is None or row[1]!='idle':raise RuntimeError('Could not identify an idle native SQL backend')
        pid,state,backend_start=row;backend=psutil.Process(pid)
        assert 'postgres' in backend.name().lower()
        affinity=backend.cpu_affinity()
        handle=kernel.OpenProcess(0x1000,False,pid)
        if not handle:raise ctypes.WinError(ctypes.get_last_error())
        before=times(handle)
        assert abs(before[0]/10000000-11644473600-backend_start)<2,'Backend PID was reused'
        setup=time.perf_counter()-started;query_started=time.perf_counter()
        stdout,stderr=process.communicate(input=text,timeout=timeout)
        wall=time.perf_counter()-query_started;after=times(handle)
        assert after[0]==before[0] and after[1]>=before[1] and after[2]>=before[2]
        value=subprocess.CompletedProcess(args,process.returncode,stdout,stderr)
        value.local_backend_diagnostics={'backend_cpu_observer':True,'hook_sha256':source_identity(),'backend_pid':pid,'backend_start_epoch':backend_start,'state_before_sql':state,'unchanged_affinity':affinity,'setup_wall_seconds':setup,'batch_transport_wall_seconds':wall,'leader_kernel_cpu_seconds':(after[1]-before[1])/10000000,'leader_user_cpu_seconds':(after[2]-before[2])/10000000,'leader_total_cpu_seconds':(after[1]+after[2]-before[1]-before[2])/10000000,'scope':'Windows GetProcessTimes on a retained native process handle, sampled before SQL and after psql completion. Leader only; parallel worker CPU is excluded. SQL, affinity, priority and power settings unchanged. Full processing timer includes observer setup.'}
        return value
    except BaseException:
        if process.poll() is None:process.kill()
        process.communicate();raise
    finally:
        if handle:kernel.CloseHandle(handle)


def install():
    import pipeline.local_diagnostics as diagnostic
    from bench import server_copy_probe as server
    original=server.ORIGINAL_RUN
    def observed(args,text,*,cwd,timeout,cpu=None):
        if isinstance(text,str) and 'BEGIN ISOLATION LEVEL REPEATABLE READ;' in text and 'pgdm_cross_table_hash_manifest' in text:
            return run(args,text,cwd=cwd,timeout=timeout,cpu=cpu)
        return original(args,text,cwd=cwd,timeout=timeout,cpu=cpu)
    server.ORIGINAL_RUN=observed
    if diagnostic.run_psql is original:diagnostic.run_psql=observed
