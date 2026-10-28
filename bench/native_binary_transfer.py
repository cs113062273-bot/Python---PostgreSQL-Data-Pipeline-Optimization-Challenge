"""Native binary psql-to-psql transfer for private archive reconstruction.

This is not processing SQL or a benchmark shortcut. Two native psql COPY
streams share a binary OS pipe without Python rebuffering each source row.
Archive callers independently verify every restored byte/catalog/sequence.
"""
import hashlib
from pathlib import Path
import subprocess


def source_identity():return hashlib.sha256(Path(__file__).read_text(encoding='utf8').encode()).hexdigest()


def transfer_raw(psql,source,target,*,timeout=600):
    assert source.info.host in ('127.0.0.1','localhost','::1') and target.info.host==source.info.host
    assert source.info.port==target.info.port and source.info.dbname.startswith('lab_') and target.info.dbname.startswith('lab_') and source.info.dbname!=target.info.dbname
    def args(connection,command):
        info=connection.info
        return [str(psql),'-h',info.host,'-p',str(info.port),'-U',info.user,'-d',info.dbname,'-X','-q','-v','ON_ERROR_STOP=1','-c',command]
    reader=subprocess.Popen(args(source,r'\copy (SELECT * FROM public.raw_data ORDER BY raw_id) TO STDOUT (FORMAT BINARY)'),stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=False,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    writer=None
    try:
        writer=subprocess.Popen(args(target,r'\copy public.raw_data FROM STDIN (FORMAT BINARY)'),stdin=reader.stdout,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=False,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        reader.stdout.close();reader.stdout=None
        stdout,writer_error=writer.communicate(timeout=timeout)
        reader.wait(timeout=timeout);reader_error=reader.stderr.read()
        if reader.returncode or writer.returncode:
            raise RuntimeError('Native archive transfer failed: '+repr({'reader_exit':reader.returncode,'writer_exit':writer.returncode,'reader_stderr':reader_error.decode('utf8',errors='replace'),'writer_stderr':writer_error.decode('utf8',errors='replace')}))
        return {'transport':'two native psql binary COPY streams over one OS pipe','source_exit':reader.returncode,'target_exit':writer.returncode,'stdout':stdout.decode('utf8',errors='replace'),'reader_stderr':reader_error.decode('utf8',errors='replace'),'writer_stderr':writer_error.decode('utf8',errors='replace'),'hook_sha256':source_identity(),'python_per_row_buffering':False,'scope':'Untimed private archive reconstruction only; actual restored bytes and final FKs are independently checked by the caller.'}
    except BaseException:
        for process in (reader,writer):
            if process is not None and process.poll() is None:process.kill()
        for process in (reader,writer):
            if process is not None:
                try:process.communicate(timeout=10)
                except (OSError,subprocess.TimeoutExpired):pass
        raise
    finally:reader.stderr.close()
