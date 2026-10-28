"""Single native data batch with hints only on disposable binary scratch files.

No phases or added commits. Windows may avoid unnecessary scratch-file writeback.
All source/final/counter/version data and WAL retain their original durability.
The original native COPY files/receipts, default IDs and full validation remain.
"""
from contextvars import ContextVar
import ctypes
import hashlib
import os
from pathlib import Path
import time
from bench import combined_metadata_probe as combined,server_copy_probe as server,early_copy_probe as early
from pipeline.recipes import ORDER

HINT_INFO=ContextVar('single_batch_temporary_hint_information',default=None)


def source_identity():
    return hashlib.sha256((combined.source_identity()+early.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def install():
    combined.install()
    rewrite=server.rewrite;run=server.run
    def hinted_rewrite(text,cwd):
        body=rewrite(text,cwd)
        # Original rewrite has verified the private fresh directory and refused
        # every existing binary file. CREATE_NEW creates only those six files.
        info=HINT_INFO.get();assert isinstance(info,dict)
        started=time.perf_counter();info['requested']=early.temporary_files(Path(cwd).resolve());info['setup_seconds']=time.perf_counter()-started
        return body
    server.rewrite=hinted_rewrite
    def observed(args,text,*,cwd,timeout=None,cpu=None):
        info={};token=HINT_INFO.set(info)
        try:
            result=run(args,'-- lab disposable file hint source_sha256='+source_identity()+'\n'+text,cwd=cwd,timeout=timeout,cpu=cpu)
            files=[Path(cwd)/(table+'.bin') for table in ORDER]
            written=[path for path in files if path.exists() and path.stat().st_size>0]
            retained=0
            if result.returncode==0:
                for path in written:
                    with path.open('rb') as f:assert f.read(11)==b'PGCOPY\n\xff\r\n\x00'
            if os.name=='nt':
                attributes=ctypes.windll.kernel32.GetFileAttributesW;attributes.argtypes=[ctypes.c_wchar_p];attributes.restype=ctypes.c_uint32
                retained=sum(1 for path in written if attributes(str(path))!=0xffffffff and attributes(str(path))&0x100)
            diagnostic=result.local_backend_diagnostics
            diagnostic.update({'temporary_hint_hook_sha256':source_identity(),'temporary_hint_requested':info['requested'],'temporary_hint_setup_seconds':info['setup_seconds'],'temporary_hint_retained_written_files':retained,'actual_binary_files':len(written),'server_files_used':diagnostic['server_file_privileges_available'] and bool(written),'scratch_binary_bytes':sum(path.stat().st_size for path in written),'extra_data_commits':0,'single_native_data_program':True})
            service=server.SERVICE.get()
            if service is not None:
                context=getattr(service._expansion_timing_local,'context',None)
                if context:service._append_expansion_timing(context['report'],'bulk_load','Hint only disposable binary COPY files',info['setup_seconds'],'local_filesystem_wall')
            return result
        finally:HINT_INFO.reset(token)
    server.run=observed


def main():
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
