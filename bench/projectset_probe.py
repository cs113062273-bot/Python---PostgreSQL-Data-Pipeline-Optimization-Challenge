"""Native ProjectSet expression barriers, without per-row FunctionScan stores."""
import argparse
import hashlib
from pathlib import Path
import statistics
import sys
import time
import uuid
from bench.common import ROOT,connect,require_lab,environment,write_report
from bench import refined_copy_probe as refined,text_stage_probe as text
from bench.proof_pattern_probe import negative_condition
from pipeline.parallel_normalization import settings_sql
from pipeline.normalization import source_sql
from pipeline.recipes import EXTRACT,destinations
from pipeline.safe_domain import _bundle


def source_identity():
    digest=hashlib.sha256(refined.source_identity().encode());digest.update(Path(__file__).read_text(encoding='utf-8').encode());return digest.hexdigest()


def projection(scopes,mode='payload_bundle',*,logged=False):
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    values=[]
    for field in fields:
        expression="p.payload->>'"+field+"'"
        if field.startswith('text_column_'):
            expression='pg_catalog.btrim('+expression+')'
            if field!='text_column_1':expression='pg_catalog.upper('+expression+')'
        values.append(expression)
    definitions=','.join(f'{field} text' for field in fields)
    bundles=[_bundle('integer_column',15).replace('c.','n.'),_bundle('numeric_column',30).replace('c.','n.'),"coalesce(n.id_column_1,'')","coalesce(n.datetime_column_1,'')"]
    payload='(SELECT s.raw_id,pg_catalog.unnest(ARRAY[s.raw->\'values\']) AS payload FROM '+source_sql(scopes)+' s) p'
    extracted='SELECT p.raw_id,n.* FROM '+payload+' CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW('+','.join(values)+')]) n('+definitions+')'
    target='CREATE UNLOGGED TABLE challenge_lab.pipeline_normalized_input AS' if logged else 'CREATE TEMP TABLE pipeline_normalized_input ON COMMIT DROP AS'
    if mode=='payload':
        # Retain the existing proof bundle barrier, changing only payload.
        body=target+' SELECT n.raw_id,'+','.join('n.'+f for f in fields)+','+negative_condition()+' AS pipeline_domain_safe FROM ('+extracted+') n CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW('+','.join(bundles)+')]) c(integers text,numerics text,identifier text,stamp text);'
    elif mode=='payload_bundle':
        kind='pgdm_pipeline_work.pipeline_proof_bundle'
        body='CREATE TYPE '+kind+' AS (integers text,numerics text,identifier text,stamp text);\n'
        projected='SELECT n.*,pg_catalog.unnest(ARRAY[ROW('+','.join(bundles)+')::'+kind+']) AS proof FROM ('+extracted+') n'
        condition=negative_condition()
        for field in ('integers','numerics','identifier','stamp'):condition=condition.replace('c.'+field,'(b.proof).'+field)
        body+=target+' SELECT b.raw_id,'+','.join('b.'+f for f in fields)+','+condition+' AS pipeline_domain_safe FROM ('+projected+') b;'
    else:raise ValueError(mode)
    return '-- lab native ProjectSet source_sha256='+source_identity()+'\n'+body


def nodes(plan):
    yield plan
    for child in plan.get('Plans',[]):yield from nodes(child)


def component():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_100k')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/projectset-components-100k.json')
    args=p.parse_args();report={'kind':'rollback_only_projectset_projection_components','official_score':False,'source_sha256':source_identity(),'runs':[]}
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])});record='ROW('+','.join(['raw_id']+fields+['pipeline_domain_safe'])+')'
    with connect(args.database,autocommit=True) as c:
        require_lab(c);report['environment']=environment(c)
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')];expected=None
        for repeat in range(args.repeats):
            modes=['original','payload','payload_bundle'];shift=repeat%len(modes)
            for mode in modes[shift:]+modes[:shift]:
                with c.transaction(force_rollback=True):
                    c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB'; SET LOCAL statement_timeout='45s'; CREATE SCHEMA pgdm_pipeline_work;")
                    if mode=='original':
                        query=text.projection(scopes,logged=True)
                        from pipeline.safe_domain import condition_sql
                        query=query.replace(condition_sql(),negative_condition()).replace('CREATE TABLE challenge_lab.pipeline_normalized_input AS','CREATE UNLOGGED TABLE challenge_lab.pipeline_normalized_input AS')
                    else:query=projection(scopes,mode,logged=True)
                    query=query.replace('challenge_lab.pipeline_normalized_input','pgdm_pipeline_work.input')
                    if mode=='payload_bundle':
                        prefix,query=query.split(';\n',1);c.execute(prefix+';')
                    digest=hashlib.sha256(query.encode()).hexdigest()
                    with (ROOT/'reports/sql'/(digest+'.sql')).open('w',encoding='utf-8',newline='\n') as f:f.write(query)
                    started=time.perf_counter();plan=c.execute('EXPLAIN (ANALYZE,BUFFERS,WAL,TIMING OFF,FORMAT JSON) '+query).fetchone()[0][0];seconds=time.perf_counter()-started
                    fp=c.execute(f'SELECT count(*),sum(hash_record_extended({record},17)::numeric)::text,sum(hash_record_extended({record},97)::numeric)::text FROM pgdm_pipeline_work.input').fetchone()
                    if expected is None:expected=fp
                    assert fp==expected
                    report['runs'].append({'repeat':repeat,'mode':mode,'seconds':seconds,'sql_sha256':digest,'full_projection_fingerprint':fp,'workers_launched':max(n.get('Workers Launched',0) for n in nodes(plan['Plan'])),'plan':plan})
                    print(repeat,mode,round(seconds,4),flush=True)
                    write_report(args.output,report)
        report['summary']={mode:statistics.median(r['seconds'] for r in report['runs'] if r['mode']==mode) for mode in modes};report['all_scratch_transactionally_rolled_back']=True;write_report(args.output,report)


def install():
    refined.install()
    import pipeline.parallel_normalization as parallel
    import pipeline.workspace as work
    parallel.prepare_input_sql=lambda scopes,logged=False:projection(scopes,'payload_bundle',logged=logged)
    original=work.cleanup_sql
    def cleanup():
        body=original();marker='DROP TABLE pgdm_pipeline_work.pipeline_normalized_input RESTRICT;'
        assert body.count(marker)==1
        return body.replace(marker,marker+'\nDROP TYPE pgdm_pipeline_work.pipeline_proof_bundle RESTRICT;',1)
    work.cleanup_sql=cleanup


def main():
    if len(sys.argv)<2 or sys.argv[1] not in ('component','service','semantics'):raise SystemExit('Use component, service or semantics with driver arguments')
    action=sys.argv.pop(1)
    if action=='component':component();return
    install()
    if action=='service':
        from bench.fast_transport_probe import install as transport
        transport()
        from bench.service_run import main as driver
    else:
        from tests.semantics.native_copy import main as driver
    driver()


if __name__=='__main__':main()
