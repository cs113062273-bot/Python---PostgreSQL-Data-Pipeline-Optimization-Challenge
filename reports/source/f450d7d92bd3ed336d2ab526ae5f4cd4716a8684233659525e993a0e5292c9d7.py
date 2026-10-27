"""Native psql phases release each disposable binary file immediately.

All SQL, COPY command tags, counters, keys, validation and the one data
transaction remain native. A phase ends only after a compiler-owned complete
COPY timing block, outside every psql conditional. No intermediate commit.
Windows temporary-file attributes are only a hint for these six owned files;
final/source/counter/version durability is never weakened.
"""
import ctypes
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import psycopg
from bench.common import ROOT
from bench import refined_copy_probe as refined,fast_transport_probe as fast,server_copy_probe as server
from pipeline.native_copy import MARKER
from pipeline.psql_session import PsqlSession
from pipeline.recipes import ORDER


def source_identity():
    digest=hashlib.sha256(refined.source_identity().encode())
    digest.update(Path(__file__).read_text(encoding='utf-8').encode())
    return digest.hexdigest()


def sections(text):
    pattern=r'^[ \t]*UPDATE pg_temp\.pgdm_expansion_timings SET finished_at=clock_timestamp\(\) WHERE sequence_number=(20[1-6]);\r?$'
    matches=list(re.finditer(pattern,text,re.M))
    if len(matches)!=6 or {int(m[1])-200 for m in matches}!=set(range(1,7)):
        raise RuntimeError('Expected six unique complete compiler COPY boundaries')
    chunks=[];offset=0
    for match in matches:
        chunks.append((text[offset:match.end()]+'\n','table'+str(int(match[1])-200)))
        offset=match.end()
    chunks.append((text[offset:],None))
    return chunks


def temporary_files(directory):
    if os.name!='nt':return False
    kernel=ctypes.WinDLL('kernel32',use_last_error=True)
    kernel.CreateFileW.argtypes=[ctypes.c_wchar_p,ctypes.c_uint32,ctypes.c_uint32,ctypes.c_void_p,ctypes.c_uint32,ctypes.c_uint32,ctypes.c_void_p]
    kernel.CreateFileW.restype=ctypes.c_void_p
    kernel.CloseHandle.argtypes=[ctypes.c_void_p];kernel.CloseHandle.restype=ctypes.c_int
    for table in ORDER:
        path=directory/(table+'.bin')
        if path.exists():raise RuntimeError('Temporary hint refuses an existing binary file')
    for table in ORDER:
        handle=kernel.CreateFileW(str(directory/(table+'.bin')),0x40000000,7,None,1,0x100,None)
        if handle==ctypes.c_void_p(-1).value:raise ctypes.WinError(ctypes.get_last_error())
        if not kernel.CloseHandle(handle):raise ctypes.WinError(ctypes.get_last_error())
    return True


def phased(args,text,*,cwd,timeout=None,cpu=None):
    if cpu is not None:raise RuntimeError('Early-file phase worker has no affinity override')
    directory=Path(cwd).resolve()
    allowed=[(ROOT/'.runtime'/name).resolve() for name in ('native_copy','native_copy_fuzz','server_copy_tests')]
    if directory.parent not in allowed:raise RuntimeError('Only the fresh owned COPY workspace is eligible')
    def option(flag,default=None):
        indexes=[i for i,x in enumerate(args) if x==flag]
        return args[indexes[-1]+1] if indexes else default
    host=option('-h',os.environ.get('PGHOST','127.0.0.1'));db=option('-d')
    if host not in ('127.0.0.1','localhost','::1') or not db or not db.startswith('lab_'):raise RuntimeError('Local disposable lab database required')
    options={'host':host,'port':option('-p',os.environ.get('PGPORT','55432')),'user':option('-U',os.environ.get('PGUSER')),'dbname':db,'autocommit':True,'connect_timeout':5}
    def cancel(pid,name,epoch):
        with psycopg.connect(**options) as c:
            c.execute('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid=%s AND datname=current_database() AND usename=current_user AND extract(epoch FROM backend_start)=%s::numeric',(pid,epoch))
    chunks=sections(text)
    deadline=float('inf') if timeout is None else time.monotonic()+timeout
    setup_started=time.perf_counter();hinted=temporary_files(directory);hint_setup_seconds=time.perf_counter()-setup_started
    released=[];cleanup_seconds=0.;peak=0;retained_hint_files=0
    session=None;outputs=[]
    try:
        session=PsqlSession(args,cwd=directory,timeout=timeout,cancel_backend=cancel)
        with session:
            for script,table in chunks:
                remaining=None if timeout is None else deadline-time.monotonic()
                if remaining is not None and remaining<=0:raise TimeoutError('Native COPY program timed out')
                outputs.append(session.send(script,timeout=remaining))
                if table is not None:
                    started=time.perf_counter()
                    files=[directory/(t+'.bin') for t in ORDER]
                    peak=max(peak,sum(p.stat().st_size for p in files if p.exists()))
                    path=directory/(table+'.bin')
                    if path.exists():
                        size=path.stat().st_size
                        if size:
                            with path.open('rb') as file:assert file.read(11)==b'PGCOPY\n\xff\r\n\x00'
                            released.append({'table':table,'binary_bytes':size})
                            if os.name=='nt':
                                attribute_fn=ctypes.windll.kernel32.GetFileAttributesW
                                attribute_fn.argtypes=[ctypes.c_wchar_p];attribute_fn.restype=ctypes.c_uint32
                                attributes=attribute_fn(str(path))
                                retained_hint_files+=int(attributes!=0xffffffff and bool(attributes&0x100))
                        path.unlink()
                    cleanup_seconds+=time.perf_counter()-started
        result=subprocess.CompletedProcess(args,0,''.join(outputs),''.join(session._errors))
    except RuntimeError as error:
        result=subprocess.CompletedProcess(args,(session.process.returncode if session is not None else 1) or 1,''.join(outputs),str(error))
    result.early_file_diagnostics={'transport':'native_phases_with_early_owned_file_release','hook_sha256':source_identity(),'windows_temporary_hint_requested':hinted,'temporary_hint_setup_wall_seconds':hint_setup_seconds,'hint_retained_written_files':retained_hint_files,'released_binary_files':released,'binary_bytes_peak_at_completed_copy':peak,'early_cleanup_wall_seconds':cleanup_seconds,'data_transaction_commits_added':0,'phases':len(chunks)}
    return result


def install():
    refined.install();fast.install()
    native=server.ORIGINAL_RUN;original=server.run
    def native_hook(args,text,*,cwd,timeout=None,cpu=None):
        if isinstance(text,str) and MARKER in text:return phased(args,text,cwd=cwd,timeout=timeout,cpu=cpu)
        return native(args,text,cwd=cwd,timeout=timeout,cpu=cpu)
    server.ORIGINAL_RUN=native_hook
    def observed(args,text,*,cwd,timeout=None,cpu=None):
        result=original(args,'-- lab eager COPY release source_sha256='+source_identity()+'\n'+text,cwd=cwd,timeout=timeout,cpu=cpu)
        details=result.early_file_diagnostics
        diag=result.local_backend_diagnostics
        written=len(details['released_binary_files'])
        diag.update(details)
        diag.update({'server_files_used':bool(diag['server_file_privileges_available'] and written),'actual_binary_files':written})
        service=server.SERVICE.get()
        if service is not None:
            context=getattr(service._expansion_timing_local,'context',None)
            if context:
                service._append_expansion_timing(context['report'],'bulk_load','Hint private temporary binary COPY files',details['temporary_hint_setup_wall_seconds'],'local_filesystem_wall')
                service._append_expansion_timing(context['report'],'bulk_load','Release completed native binary COPY files',details['early_cleanup_wall_seconds'],'local_filesystem_wall')
        return result
    server.run=observed


def main():
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
