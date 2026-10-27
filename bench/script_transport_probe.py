"""Reuse native psql for complete metadata stdin programs, never result rows.

Stdin programs retain native statement-by-statement transaction behavior;
-c programs retain one-request gexec behavior. Protected data SQL stays on
the original server-file transport. Incomplete/metacommand programs fall back
to the original process, and every reused backend is reset between requests.
"""
import hashlib
import os
from pathlib import Path
import re
import subprocess
import sys
from psycopg import sql
from bench import refined_copy_probe as refined,fast_transport_probe as fast,server_copy_probe as server
from pipeline.native_copy import MARKER
from pipeline.psql_session import PsqlSession
from pipeline.upstream import PostgresAdminService


def source_identity():
    digest=hashlib.sha256(refined.source_identity().encode());digest.update(Path(__file__).read_text(encoding='utf-8').encode());return digest.hexdigest()


def complete_program(text):
    # Eligibility framing only: native psql still parses and executes the
    # unchanged program. Reject backslashes conservatively to cover both
    # client metacommands and non-default SQL backslash-escape settings.
    if not isinstance(text,str) or '\\' in text:return False
    quote=None;dollar=None;block=0;line=False;depth=0;last='';i=0
    while i<len(text):
        ch=text[i];pair=text[i:i+2]
        if line:
            if ch=='\n':line=False
        elif block:
            if pair=='/*':block+=1;i+=1
            elif pair=='*/':block-=1;i+=1
        elif dollar:
            if text.startswith(dollar,i):i+=len(dollar)-1;dollar=None
        elif quote:
            if ch==quote:
                if i+1<len(text) and text[i+1]==quote:i+=1
                else:quote=None
        elif pair=='--':line=True;i+=1
        elif pair=='/*':block=1;i+=1
        elif ch in ("'",'"'):quote=ch;last='literal'
        elif ch=='$':
            dollar=PostgresAdminService._read_dollar_quote_tag(text,i)
            if dollar is None:return False
            i+=len(dollar)-1;last='literal'
        elif ch=='(':depth+=1;last=ch
        elif ch==')':
            depth-=1;last=ch
            if depth<0:return False
        elif not ch.isspace():last=ch
        i+=1
    return quote is None and dollar is None and block==0 and depth==0 and last==';'


def eligible(args,text,cwd):
    if cwd is not None or not isinstance(text,str) or MARKER in text or 'pgdm_cross_table_hash_manifest' in text:return False
    if '-c' in args or args.count('-f')>1:return False
    if '-f' in args and args[args.index('-f')+1:] [:1]!=['-']:return False
    if 'ON_ERROR_STOP=1' not in args or '-X' not in args:return False
    if any(flag in args for flag in ('-a','-e','-E','-x','-H','-o','-z','-0','-1','--single-transaction','--echo-all','--echo-queries','--expanded','--html')):return False
    if any('AUTOCOMMIT' in arg or arg.startswith('--output') for arg in args):return False
    if re.search(r'\b(DROP|ALTER)\s+DATABASE\b|\bCOPY\b',text,re.I):return False
    return complete_program(text)


def install(strategy='refined'):
    if strategy=='refined':refined.install()
    elif strategy=='scaled':
        from bench.scaled_int_probe import install as data_install
        data_install()
    elif strategy=='projectset':
        from bench.projectset_probe import install as data_install
        data_install()
    else:raise ValueError(strategy)
    fast.install()
    # PsqlSession always has ON_ERROR_STOP=1. A single -c request has native
    # server error-stop semantics too; all eligible stdin programs explicitly
    # requested ON_ERROR_STOP=1. These connection profiles can share a pool.
    previous_neutral=fast.format_neutral_args
    def neutral(args):
        args=previous_neutral(args);result=[];i=0
        while i<len(args):
            if args[i]=='-v' and args[i+1:i+2]==['ON_ERROR_STOP=1']:i+=2;continue
            result.append(args[i]);i+=1
        return result
    fast.format_neutral_args=neutral
    import pipeline.local_diagnostics as diagnostic
    from pipeline.local_service import LocalService
    underlying=diagnostic.run_psql
    def hooked(args,text,*,cwd,timeout,cpu=None):
        service=server.SERVICE.get()
        if service is not None and cpu is None and eligible(args,text,cwd):
            state=service._expansion_timing_local
            sessions=getattr(state,'metadata_sessions',None)
            if sessions is None:sessions={};state.metadata_sessions=sessions
            base=list(args)
            if '-f' in base:
                index=base.index('-f');del base[index:index+2]
            style,tuples,quiet=fast.native_format(base);base=neutral(base);key=tuple(base)
            try:
                if key not in sessions:
                    sessions[key]=PsqlSession(base,timeout=timeout);state.metadata_opened=getattr(state,'metadata_opened',0)+1
                session=sessions[key];session.timeout=timeout
                if getattr(session,'metadata_format',None)!=(style,tuples,quiet):
                    session.send('\\set QUIET on\n\\pset format '+style+'\n\\pset tuples_only '+tuples+'\n\\set QUIET '+quiet+'\n');session.metadata_format=(style,tuples,quiet)
                session.send('SET application_name='+sql.Literal(os.environ.get('PGAPPNAME','psql')).as_string()+';')
                output=session.send(text)
                session.send('SET client_min_messages=error; ROLLBACK;');session.send('DISCARD ALL;')
                state.metadata_reexecuted=getattr(state,'metadata_reexecuted',0)+1
                state.metadata_stdin_reexecuted=getattr(state,'metadata_stdin_reexecuted',0)+1
                return subprocess.CompletedProcess(args,0,output,'')
            except BaseException:fast.close_sessions(service);raise
        if isinstance(text,str) and MARKER in text:text='-- lab native metadata stdin reuse source_sha256='+source_identity()+'\n'+text
        return underlying(args,text,cwd=cwd,timeout=timeout,cpu=cpu)
    diagnostic.run_psql=hooked
    register=LocalService.register_cross_table_expansion_versions
    def registered(self,*args,**kwargs):
        try:return register(self,*args,**kwargs)
        finally:
            state=self._expansion_timing_local;diag=getattr(state,'local_backend_diagnostics',None)
            if isinstance(diag,dict):diag.update({'metadata_stdin_hook_sha256':source_identity(),'metadata_stdin_programs_reexecuted':getattr(state,'metadata_stdin_reexecuted',0),'data_strategy':strategy})
    LocalService.register_cross_table_expansion_versions=registered


def main():
    strategy='refined'
    if len(sys.argv)>1 and sys.argv[1] in ('scaled','projectset'):strategy=sys.argv.pop(1)
    install(strategy)
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
