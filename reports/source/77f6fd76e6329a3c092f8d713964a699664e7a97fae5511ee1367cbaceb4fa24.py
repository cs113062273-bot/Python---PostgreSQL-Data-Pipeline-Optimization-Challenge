"""Isolated native C versus SQL projection, exact every-row comparison."""
import argparse
import hashlib
from pathlib import Path
import statistics
import time
import uuid
from bench import native_c_lab as lab,calendar_proof_probe as calendar,null_cache_probe as null_cache
from bench.common import ROOT,environment,write_report
from pipeline.normalization import source_sql
from pipeline.parallel_normalization import settings_sql
from pipeline.recipes import EXTRACT,destinations


def native_projection(scopes,function,target):
    values=[]
    for field in lab.FIELDS:
        value="NULLIF(n."+field+",'')"
        if field.startswith('text_column_'):
            value='pg_catalog.btrim('+value+')'
            if field!='text_column_1':value='pg_catalog.upper('+value+')'
        values.append(value+' AS '+field)
    return 'CREATE UNLOGGED TABLE '+target+' AS SELECT s.raw_id,'+','.join(values)+',n.pipeline_domain_safe FROM '+source_sql(scopes)+" s CROSS JOIN LATERAL "+function+"(s.raw->'values') n("+lab.definition()+');'


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows',type=int,default=100000)
    parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--database')
    parser.add_argument('--output',default='reports/native-c-projection-100k.json')
    args=parser.parse_args();assert args.rows>0 and args.repeats>0
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    database=args.database or 'lab_native_c_component_'+uuid.uuid4().hex[:8]
    if args.database is None:lab.fixture(database,rows=args.rows)
    report={'kind':'isolated_unqualified_native_c_projection_pair','official_score':False,'application_integration':False,'application_function_guard_parity':False,'eligibility_unproven':True,'database':database,'source_rows':args.rows,'runs':[],
            'scope':'Selected normalization/proof CTAS only, shared warmed source, three rotating pairs. Every resulting raw_id plus all 64 normalized values and proof are compared exactly outside timers. No final native COPY/index/FK/commit/versions, full-service percentage or official eligibility claim.'}
    source_sha=hashlib.sha256((ROOT/'native/challenge_probe.c').read_bytes()).hexdigest();driver_sha=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    with lab.connect(database,autocommit=True) as c:
        function,build=lab.install(c);report['native_build']=build;report['driver_sha256']=driver_sha;report['environment']=environment(c)
        assert c.execute('SELECT count(*) FROM public.raw_data').fetchone()[0]==args.rows
        scopes=[row[0] for row in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        columns=['raw_id',*lab.FIELDS,'pipeline_domain_safe'];record=lambda prefix:'ROW('+','.join(prefix+'.'+field for field in columns)+')'
        for repeat in range(args.repeats):
            for mode in (('sql','native_c') if repeat%2==0 else ('native_c','sql')):
                assert hashlib.sha256((ROOT/'native/challenge_probe.c').read_bytes()).hexdigest()==source_sha
                with c.transaction(force_rollback=True):
                    c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB'; SET LOCAL statement_timeout='90s';")
                    schema='native_component_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                    # Reference creation warms exactly the same source before
                    # either timed mode and provides actual rows for equality.
                    reference=null_cache.projection(calendar.projection(scopes,logged=True)).replace('challenge_lab.pipeline_normalized_input',schema+'.reference')
                    c.execute(reference)
                    query=reference.replace(schema+'.reference',schema+'.candidate') if mode=='sql' else native_projection(scopes,function,schema+'.candidate')
                    query_sha=hashlib.sha256(query.encode()).hexdigest();archive=ROOT/'reports/sql'/(query_sha+'.sql')
                    with archive.open('x',encoding='utf8',newline='\n') as writer:writer.write(query)
                    started=time.perf_counter();c.execute(query);seconds=time.perf_counter()-started
                    mismatch=c.execute('SELECT EXISTS(SELECT 1 FROM '+schema+'.reference a FULL JOIN '+schema+'.candidate b USING(raw_id) WHERE '+record('a')+' IS DISTINCT FROM '+record('b')+')').fetchone()[0]
                    assert not mismatch,'Native output or proof differs from SQL for an actual source row'
                    count,safe=c.execute('SELECT count(*),bool_and(pipeline_domain_safe) FROM '+schema+'.candidate').fetchone();assert count==args.rows
                    size=c.execute('SELECT pg_total_relation_size(%s::regclass)',(schema+'.candidate',)).fetchone()[0]
                    report['runs'].append({'repeat':repeat,'mode':mode,'projection_seconds':seconds,'exact_all_rows_64_values_and_proof':True,'rows':count,'all_safe':safe,'stage_bytes':size,'sql_sha256':query_sha});write_report(output,report)
                    print(repeat,mode,round(seconds,5),'exact rows',count,flush=True)
        report['summary']={mode:{'median_projection_seconds':statistics.median(row['projection_seconds'] for row in report['runs'] if row['mode']==mode)} for mode in ('sql','native_c')}
        report['all_private_projection_work_rolled_back']=True;report['all_passed']=True;write_report(output,report)
        print(report['summary'],flush=True)


if __name__=='__main__':main()
