"""Compare six/eight workers over the same proven inline SQL kernels."""
import argparse
import hashlib
from pathlib import Path
import re
import statistics
import tempfile
import time
import uuid
from psycopg import sql
from bench.common import ROOT,connect,require_lab,environment,write_report
from bench import t1_inline_probe as inline,calendar_proof_probe as calendar
from bench.run import prepared
from pipeline.workspace import RELATION
from pipeline.parallel_normalization import settings_sql as original_settings
from tests.equivalence.oracle import protected_schema_signature,sequence_signature
from tests.recovery.bulk_dump import sequence_state


def source_identity():
    return hashlib.sha256((inline.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def settings(workers=6):
    if workers not in (6,8):raise ValueError(workers)
    body=original_settings(8);assert body.count('max_parallel_workers_per_gather=8;')==1
    return body.replace('max_parallel_workers_per_gather=8;','max_parallel_workers_per_gather='+str(workers)+';',1)


def component():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_native_variant_d58abcf90d')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/six-eight-workers-inline-kernels-1m.json')
    args=p.parse_args();frozen=source_identity()
    report={'kind':'rollback_only_six_eight_workers_same_inline_sql','official_score':False,'source_sha256':frozen,'runs':[],'scope':'Complete extraction/proof/ANALYZE and six real binary COPY TO transforms; no final COPY FROM/index/FK/versions/commit. Full typed transform fingerprints outside timers. Identical SQL/setting budget except worker degree.'}
    parent=(ROOT/'.runtime/six_worker').resolve();parent.mkdir(exist_ok=True)
    directory=Path(tempfile.mkdtemp(prefix='component_',dir=parent)).resolve();assert directory.parent==parent
    path=directory/'transform.bin';expected={}
    inline.install()
    import pipeline.guarded_normalization as guarded
    try:
        with connect(args.database,autocommit=True) as c:
            require_lab(c)
            assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'Do not overlap processing'
            report['environment']=environment(c);before=protected_schema_signature(c);sequences=sequence_signature(c);values=sequence_state(c)
            scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
            recipes=prepared('guarded_copy',scopes)
            for repeat in range(args.repeats):
                for workers in ((8,6) if repeat%2==0 else (6,8)):
                    assert source_identity()==frozen
                    with c.transaction(force_rollback=True):
                        c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+settings(workers)+" SET LOCAL work_mem='64MB'; SET LOCAL statement_timeout='90s';")
                        schema='six_worker_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                        query=calendar.projection(scopes,logged=True).replace('challenge_lab.pipeline_normalized_input',schema+'.input')
                        started=time.perf_counter();c.execute(query);c.execute('ANALYZE '+schema+'.input(raw_id,text_column_1,text_column_2,text_column_3)')
                        assert c.execute('SELECT count(*),bool_and(pipeline_domain_safe) FROM '+schema+'.input').fetchone()==(1000000,True)
                        projection_seconds=time.perf_counter()-started
                        row={'repeat':repeat,'workers':workers,'projection_proof_analyze_seconds':projection_seconds,'transforms':{}}
                        print(repeat,workers,'projection',round(projection_seconds,4),flush=True)
                        for recipe in recipes:
                            table=recipe['pure_table_name'];body=guarded.fast_statement(recipe['statements'][0],scopes,'guarded_copy').replace(RELATION,schema+'.input')
                            select=re.fullmatch(r'INSERT INTO public\.table[1-6]\s*\(.*?\)\s*(SELECT\s.*?);?',body,re.S)[1].rstrip(';')
                            c.execute('SET LOCAL enable_parallel_hash='+('off' if table=='table1' else 'on'))
                            assert not path.exists();started=time.perf_counter()
                            cur=c.execute(sql.SQL('COPY ({}) TO {} (FORMAT BINARY)').format(sql.SQL(select),sql.Literal(path.as_posix())));seconds=time.perf_counter()-started
                            size=path.stat().st_size;path.unlink()
                            fp=c.execute('SELECT count(*),sum(hash_record_extended(q,17)::numeric)::text,sum(hash_record_extended(q,97)::numeric)::text FROM ('+select+') q').fetchone()
                            if table not in expected:expected[table]=fp
                            assert fp==expected[table]
                            row['transforms'][table]={'seconds':seconds,'binary_bytes':size,'command_tag':cur.statusmessage,'full_typed_fingerprint':fp}
                            print(repeat,workers,table,round(seconds,4),flush=True)
                            if repeat==0 and table=='table1':row['actual_table1_plan']=c.execute('EXPLAIN (ANALYZE,BUFFERS,TIMING OFF,FORMAT JSON) '+select).fetchone()[0]
                        row['kernel_seconds']=projection_seconds+sum(v['seconds'] for v in row['transforms'].values())
                        report['runs'].append(row);write_report(args.output,report)
                    assert protected_schema_signature(c)==before and sequence_signature(c)==sequences and sequence_state(c)==values
            report['summary']={str(n):statistics.median(r['kernel_seconds'] for r in report['runs'] if r['workers']==n) for n in (6,8)}
            report['all_private_work_rolled_back_native_sequences_unchanged']=True;write_report(args.output,report)
    finally:path.unlink(missing_ok=True);directory.rmdir()


def install():
    inline.install()
    import pipeline.parallel_normalization as parallel
    import pipeline.native_copy as native_copy
    parallel.settings_sql=settings;native_copy.settings_sql=settings
    projection=parallel.prepare_input_sql
    parallel.prepare_input_sql=lambda scopes,logged=False:'-- lab six-worker inline source_sha256='+source_identity()+'\n'+projection(scopes,logged=logged)


def main():
    import sys
    if len(sys.argv)>1 and sys.argv[1]=='component':sys.argv.pop(1);component();return
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
