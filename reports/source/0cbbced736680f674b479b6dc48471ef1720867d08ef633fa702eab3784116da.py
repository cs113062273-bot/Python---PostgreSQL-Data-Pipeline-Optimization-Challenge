"""Isolated private-loopback server-file COPY, with client COPY fallback.

No role grant, PROGRAM, FREEZE, manual identity or durability change. The six
files are restricted to the caller's freshly created task-owned workspace.
Requires the test PostgreSQL OS process to share access to that directory.
Ordinary SSH transport is not qualified by this local experiment.
"""
from contextvars import ContextVar
import hashlib
import os
from pathlib import Path
import re
import sys

from bench.common import ROOT
from pipeline.native_copy import MARKER
from pipeline.recipes import ORDER

SERVICE=ContextVar('server_copy_service',default=None)


def source_identity():return hashlib.sha256(Path(__file__).read_text(encoding='utf-8').encode()).hexdigest()


def rewrite(text,cwd):
    directory=Path(cwd).resolve()
    allowed=[(ROOT/'.runtime'/name).resolve() for name in ('native_copy','native_copy_fuzz','server_copy_tests')]
    if not any(directory.parent==parent for parent in allowed):raise RuntimeError('Server COPY requires a new owned workspace')
    if any((directory/(t+'.bin')).exists() for t in ORDER):raise RuntimeError('Refuse to overwrite an existing COPY file')
    lines=text.splitlines(keepends=True);counts={t:0 for t in ORDER};result=[];inserted=False
    for line in lines:
        if line.strip()==MARKER and not inserted:
            result.append("-- lab server-file COPY source_sha256="+source_identity()+"\nSELECT pg_catalog.pg_has_role(current_user,'pg_read_server_files','USAGE') AND pg_catalog.pg_has_role(current_user,'pg_write_server_files','USAGE') AS pipeline_server_file_copy\n\\gset\n\\echo PGDM_SERVER_FILE_COPY=:pipeline_server_file_copy\n")
            inserted=True
        match=re.fullmatch(r"\\copy (.*) (TO|FROM) '(table[1-6])\.bin' \(FORMAT BINARY\)\s*",line)
        if not match:result.append(line);continue
        body,direction,table=match.groups()
        if table not in counts:raise RuntimeError('Unknown protected COPY target')
        counts[table]+=1
        path=(directory/(table+'.bin')).as_posix().replace("'","''")
        server='COPY '+body+' '+direction+" '"+path+"' (FORMAT BINARY);\n"
        result.extend(('\\if :pipeline_server_file_copy\n',server,'\\else\n',line if line.endswith('\n') else line+'\n','\\endif\n'))
    if not inserted or counts!={t:2 for t in ORDER}:raise RuntimeError('Expected exactly six export/import COPY pairs')
    return ''.join(result)


def run(args,text,*,cwd,timeout=None,cpu=None):
    # This helper is also used by the independent semantic replays, where no
    # service/context exists. The official control stays on original transport.
    import pipeline.local_diagnostics as diagnostic
    host=args[args.index('-h')+1] if '-h' in args else os.environ.get('PGHOST','127.0.0.1')
    if host not in ('127.0.0.1','localhost','::1'):raise RuntimeError('Server-file experiment requires loopback')
    transformed=rewrite(text,cwd)
    digest=hashlib.sha256(transformed.encode()).hexdigest()
    service=SERVICE.get()
    if service is not None:
        service._expansion_timing_local.guarded_batch_sha256=digest
        with (ROOT/'reports/sql'/(digest+'.sql')).open('w',encoding='utf-8',newline='\n') as f:f.write(transformed)
    result=ORIGINAL_RUN(args,transformed,cwd=cwd,timeout=timeout,cpu=cpu)
    choices=re.findall(r'^PGDM_SERVER_FILE_COPY=(t|f)\s*$',result.stdout,re.M)
    if result.returncode==0 and len(choices)!=1:raise RuntimeError('Server/client choice was not observed')
    result.stdout=re.sub(r'^PGDM_SERVER_FILE_COPY=(?:t|f)\r?\n','',result.stdout,flags=re.M)
    files=sum((Path(cwd)/(t+'.bin')).exists() for t in ORDER)
    upstream_diagnostics=getattr(result,'local_backend_diagnostics',None)
    result.local_backend_diagnostics={'transport':'private_server_file_copy_probe','server_files_used':choices==['t'] and files>0,'server_file_privileges_available':choices==['t'],'privilege_fallback_to_client':choices==['f'],'actual_binary_files':files,'hook_sha256':source_identity(),'actual_batch_sql_sha256':digest,'scope':'Private local same-OS-user workspace; native command tags/default identities and original validation. No privilege grant. Not SSH qualification.'}
    if upstream_diagnostics is not None:
        key='backend_cpu_diagnostics' if upstream_diagnostics.get('backend_cpu_observer') else 'leader_affinity_diagnostic'
        result.local_backend_diagnostics[key]=upstream_diagnostics
    return result


import pipeline.local_diagnostics as _diagnostic
ORIGINAL_RUN=_diagnostic.run_psql


def install():
    from pipeline.local_service import LocalService
    original=LocalService.run_remote_command
    def bound(self,*args,**kwargs):
        token=SERVICE.set(self)
        try:return original(self,*args,**kwargs)
        finally:SERVICE.reset(token)
    LocalService.run_remote_command=bound
    def hooked(args,text,*,cwd,timeout,cpu=None):
        if isinstance(text,str) and MARKER in text:return run(args,text,cwd=cwd,timeout=timeout,cpu=cpu)
        return ORIGINAL_RUN(args,text,cwd=cwd,timeout=timeout,cpu=cpu)
    _diagnostic.run_psql=hooked


def main():
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
