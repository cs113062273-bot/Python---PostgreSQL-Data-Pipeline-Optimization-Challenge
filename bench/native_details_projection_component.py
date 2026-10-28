"""Worker choices for the actual compact cache and guarded details setup."""
import argparse
import hashlib
from pathlib import Path
import statistics
import time
from bench import native_c_lab as lab,native_details_probe as candidate
from bench.common import ROOT,write_report,require_lab
from pipeline.normalization import source_sql
from tests.equivalence.oracle import protected_schema_signature,sequence_signature


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--database',required=True);p.add_argument('--repeats',type=int,default=3);p.add_argument('--output',required=True);args=p.parse_args()
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    frozen=candidate.source_identity();candidate.install()
    from pipeline.workspace import prepare_sql,RELATION
    report={'kind':'native_details_projection_worker_component','database':args.database,'candidate_hook_sha256':frozen,
            'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'runs':[],
            'scope':'Actual work namespace, trusted extension catalog checks, compact normalization CTAS, global proof and typed-details eligibility. Shared warmed source/reference. Every raw_id/12 values/proof compared outside timers. All private setup rolls back. Retained populated destinations make final empty-target decision false; no final ingest/keys/FK/commit/versions or complete-service percentage.'}
    with lab.connect(args.database,autocommit=True) as c:
        require_lab(c);before=protected_schema_signature(c);sequences=sequence_signature(c)
        with c.transaction(force_rollback=True):
            c.execute("SET LOCAL work_mem='128MB'; SET LOCAL statement_timeout='180s'")
            normalize,axes,build=lab.install_compact(c);report['normalizer_build']=build
            scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
            c.execute('CREATE TEMP TABLE projection_reference ON COMMIT DROP AS SELECT s.raw_id,n.* FROM '+source_sql(scopes)+" s CROSS JOIN LATERAL "+normalize+"(s.raw->'values') n("+lab.compact_definition()+')')
            assert c.execute('SELECT count(*) FROM pg_temp.projection_reference').fetchone()==(1000000,)
            columns=['raw_id']+[name for name,kind in lab.COMPACT_FIELDS]
            row=lambda alias:'ROW('+','.join(alias+'.'+field for field in columns)+')'
            for repeat in range(args.repeats):
                modes=[8,0,2,4];shift=repeat%4
                for workers in modes[shift:]+modes[:shift]:
                    with c.transaction(force_rollback=True):
                        program=prepare_sql(scopes)
                        assert program.count('SET LOCAL max_parallel_workers_per_gather=8;')==1
                        program=program.replace('SET LOCAL max_parallel_workers_per_gather=8;','SET LOCAL max_parallel_workers_per_gather='+str(workers)+';',1)
                        start=time.perf_counter();c.execute(program);seconds=time.perf_counter()-start
                        assert c.execute('SELECT usable FROM pg_temp.pipeline_details_state').fetchone()==(True,)
                        assert not c.execute('SELECT EXISTS(SELECT 1 FROM pg_temp.projection_reference a FULL JOIN '+RELATION+' b USING(raw_id) WHERE '+row('a')+' IS DISTINCT FROM '+row('b')+')').fetchone()[0]
                        count=c.execute('SELECT count(*) FROM '+RELATION).fetchone()[0];assert count==1000000
                        report['runs'].append({'repeat':repeat,'workers':workers,'seconds':seconds,'exact_all_values_and_proof':True,'rows':count,'private_setup_rolled_back':True});write_report(output,report)
                        print(repeat,workers,round(seconds,6),'all source/cache fields exact',flush=True)
                    assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()==(None,)
            assert candidate.source_identity()==frozen
        assert protected_schema_signature(c)==before and sequence_signature(c)==sequences
    report['summary']={str(w):statistics.median(r['seconds'] for r in report['runs'] if r['workers']==w) for w in (0,2,4,8)}
    report['passed']=True;write_report(output,report);print(report['summary'],flush=True)


if __name__=='__main__':main()
