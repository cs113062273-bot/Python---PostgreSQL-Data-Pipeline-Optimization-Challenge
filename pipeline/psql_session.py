"""Phased native psql pipe for bounded pipeline orchestration.

psql interprets SQL and its own metacommands, including binary-aware \\copy.
Python handles only phase boundaries and output. This primitive is not yet the
submission transport or a durable chunk protocol. Callers send complete trusted
SQL phases and may supply an own-backend cancel callback for prompt timeouts.
"""
import os
import queue
import subprocess
import threading
import time
import uuid


class PsqlSession:
    def __init__(self,args,*,cwd=None,env=None,timeout=600,cancel_backend=None):
        if '-c' in args:raise ValueError('Persistent psql requires pipe input, not -c')
        if '-f' in args and (args.index('-f')+1>=len(args) or args[args.index('-f')+1]!='-'):
            raise ValueError('Persistent psql accepts -f - for stdin, not an external script file')
        if timeout is not None and timeout<=0:raise ValueError('Positive timeout or None required')
        self.timeout=timeout;self.cancel_backend=cancel_backend
        self.application_name='pgdm_phase_'+uuid.uuid4().hex
        self.backend_pid=None;self.backend_start_epoch=None;self._closed=False
        self._queue=queue.Queue();self._errors=[];self._eof=set()
        self._phase_lock=threading.Lock();self._writer=None
        self.abort_errors=[]
        self.process=subprocess.Popen(list(args)+['-X','-q','-A','-t','-v','ON_ERROR_STOP=1'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,encoding='utf-8',errors='replace',bufsize=1,cwd=cwd,env={**os.environ,**(env or {}),'PGAPPNAME':self.application_name},shell=False,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        def read(stream,name):
            try:
                for line in stream:self._queue.put((name,line))
            finally:self._queue.put((name,None))
        self._readers=[threading.Thread(target=read,args=(self.process.stdout,'out'),daemon=True),threading.Thread(target=read,args=(self.process.stderr,'err'),daemon=True)]
        for reader in self._readers:reader.start()
        try:
            identity=self.send("SELECT pid::text||'|'||extract(epoch FROM backend_start)::text FROM pg_catalog.pg_stat_activity WHERE pid=pg_catalog.pg_backend_pid();",timeout=15 if timeout is None else min(timeout,15)).strip().split('|')
            if len(identity)!=2:raise RuntimeError('Native psql backend identity was not returned')
            self.backend_pid=int(identity[0]);self.backend_start_epoch=identity[1]
        except BaseException:self.abort();raise

    def _record(self,event):
        stream,line=event
        if line is None:self._eof.add(stream)
        elif stream=='err':self._errors.append(line)
        return stream,line

    def _finish_readers(self,timeout=2):
        deadline=time.monotonic()+timeout
        while len(self._eof)<2 and time.monotonic()<deadline:
            try:self._record(self._queue.get(timeout=.05))
            except queue.Empty:pass
        for reader in self._readers:reader.join(.2)
        if self.process.poll() is not None and all(not r.is_alive() for r in self._readers):
            self.process.stdout.close();self.process.stderr.close()

    def _error(self):
        self._finish_readers()
        return RuntimeError(''.join(self._errors).strip() or 'Native psql ended before the phase marker')

    def send(self,script,*,timeout=None):
        if self._closed:raise RuntimeError('Native psql session is closed')
        if not isinstance(script,str):raise TypeError('Native phase must be UTF-8 SQL text')
        limit=self.timeout if timeout is None else timeout
        if limit is not None and limit<=0:raise ValueError('Positive phase timeout required')
        if not self._phase_lock.acquire(blocking=False):raise RuntimeError('Another native psql phase is active')
        marker='__PGDM_PHASE_END_'+uuid.uuid4().hex+'__'
        stdout=[];deadline=float('inf') if limit is None else time.monotonic()+limit
        input_errors=[]
        def write():
            try:
                self.process.stdin.write(script+('' if script.endswith('\n') else '\n')+'\\echo '+marker+'\n')
                self.process.stdin.flush()
            except (BrokenPipeError,OSError,ValueError) as error:input_errors.append(error)
        try:
            # A phase can exceed pipe capacity while psql is busy executing an
            # early query. Keep the deadline and stdout/stderr drains live.
            self._writer=threading.Thread(target=write,daemon=True);self._writer.start()
            while True:
                remaining=deadline-time.monotonic()
                if remaining<=0:raise TimeoutError('Native psql phase timed out')
                try:event=self._queue.get(timeout=min(.1,remaining))
                except queue.Empty:
                    if self.process.poll() is not None:raise self._error()
                    if input_errors:raise RuntimeError('Native psql input pipe failed') from input_errors[0]
                    continue
                stream,line=self._record(event)
                if stream=='out' and line is not None:
                    if line.rstrip('\r\n')==marker:
                        self._writer.join(1)
                        if self._writer.is_alive():raise TimeoutError('Native psql input writer did not finish')
                        if input_errors:raise self._error()
                        return ''.join(stdout)
                    stdout.append(line)
                if len(self._eof)==2:raise self._error()
        except BaseException:
            self.abort()
            raise
        finally:self._phase_lock.release()

    def abort(self):
        if self._closed:return
        if self.process.poll() is None:
            if self.cancel_backend is not None and self.backend_pid is not None:
                try:self.cancel_backend(self.backend_pid,self.application_name,self.backend_start_epoch)
                except Exception as error:self.abort_errors.append(type(error).__name__)
            try:self.process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                self.process.kill();self.process.wait(timeout=5)
        try:self.process.stdin.close()
        except (BrokenPipeError,OSError):pass
        if self._writer is not None:self._writer.join(1)
        self._finish_readers();self._closed=True

    def close(self):
        if self._closed:return
        try:
            self.process.stdin.close()
            try:self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:self.abort();raise TimeoutError('Native psql did not exit after EOF')
            self._finish_readers();self._closed=True
            if self.process.returncode:raise self._error()
        except BaseException:
            self.abort();raise

    def __enter__(self):return self

    def __exit__(self,kind,error,traceback):
        if kind is None:self.close()
        else:self.abort()
