"""Same-query native metadata replay; no data-pipeline percentage."""
import argparse
import os
import statistics
import time
import uuid
from bench.common import connect,write_report
from bench.fixture import create_fixture
from bench import combined_metadata_probe as combined,fast_transport_probe as fast,server_copy_probe as server
from pipeline.local_service import LocalService


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--commands',type=int,default=32)
    p.add_argument('--output',default='reports/metadata-framing-components.json')
    args=p.parse_args();frozen=combined.source_identity()
    db='lab_metadata_framing_'+uuid.uuid4().hex[:8];create_fixture(db)
    combined.install()
    import pipeline.local_diagnostics as diagnostic
    with connect(db,autocommit=True) as c:
        assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'Do not overlap processing'
    report={'kind':'native_metadata_same_query_framing_components','official_score':False,'database':db,'combined_hook_sha256':frozen,'runs':[],'scope':'Fresh session pools for every replay; original SQL rerun each time, 1/4 stdin and 3/4 -c. Pool start/reset/close included. No transform/COPY/index/FK/version/commit score. Functional formatting/transaction equality is a separate 19-case suite.'}
    for repeat in range(args.repeats):
        for mode in (('separate','combined') if repeat%2==0 else ('combined','separate')):
            assert combined.source_identity()==frozen
            service=LocalService()
            base=[service.psql,'-h',os.environ['PGHOST'],'-p',os.environ['PGPORT'],'-U',os.environ['PGUSER'],'-d',db,'-X','-v','ON_ERROR_STOP=1','-qAt']
            context=server.SERVICE.set(service);enabled=combined.ENABLED.set(mode=='combined');outputs=[]
            try:
                started=time.perf_counter()
                for i in range(args.commands):
                    command='SELECT '+str(i)+'::integer;'
                    result=diagnostic.run_psql(base+(['-f','-'] if i%4==0 else ['-c',command]),command if i%4==0 else None,cwd=None,timeout=15)
                    assert result.returncode==0
                    outputs.append(result.stdout)
                fast.close_sessions(service);seconds=time.perf_counter()-started
                assert outputs==[str(i)+'\n' for i in range(args.commands)]
                state=service._expansion_timing_local
                assert state.metadata_reexecuted==args.commands and state.metadata_stdin_reexecuted==len(range(0,args.commands,4)) and not state.metadata_sessions
                assert getattr(state,'metadata_combined_phases',0)==(args.commands if mode=='combined' else 0)
                report['runs'].append({'repeat':repeat,'mode':mode,'seconds':seconds,'native_queries_reexecuted':args.commands,'stdin_queries_reexecuted':state.metadata_stdin_reexecuted,'sessions_opened':state.metadata_opened,'closed_and_all_results_correct':True});write_report(args.output,report)
                print(repeat,mode,round(seconds,4),flush=True)
            finally:
                fast.close_sessions(service);combined.ENABLED.reset(enabled);server.SERVICE.reset(context)
    report['summary']={mode:statistics.median(r['seconds'] for r in report['runs'] if r['mode']==mode) for mode in ('separate','combined')};write_report(args.output,report)


if __name__=='__main__':main()
