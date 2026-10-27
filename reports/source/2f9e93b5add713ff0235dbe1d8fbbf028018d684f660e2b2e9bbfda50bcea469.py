"""Ephemeral unlogged cache research, never unlogged final data/checkpoints.

Only the transaction-created normalized stage changes persistence; all six
destinations, counters and control versions keep their original durability.
The cache is dropped before commit. A pre-commit failure must roll back all
public changes and recreate the cache on retry. Default compiler is untouched.
"""
import argparse
import hashlib
from pathlib import Path
import statistics
import sys
import time
import uuid

from bench.common import ROOT,connect,require_lab,environment,write_report
from pipeline.parallel_normalization import prepare_input_sql as original_projection,settings_sql
from pipeline.recipes import EXTRACT,destinations


def source_identity():return hashlib.sha256(Path(__file__).read_text(encoding='utf-8').encode()).hexdigest()


def unlogged_projection(scopes,*,logged=False):
    body=original_projection(scopes,logged=logged)
    if not logged:return body
    marker='CREATE TABLE challenge_lab.pipeline_normalized_input AS'
    if body.count(marker)!=1:raise RuntimeError('Unknown stage persistence boundary')
    return '-- lab ephemeral unlogged stage source_sha256='+source_identity()+'\n'+body.replace(marker,'CREATE UNLOGGED TABLE challenge_lab.pipeline_normalized_input AS',1)


def install():
    import pipeline.parallel_normalization as module
    module.prepare_input_sql=unlogged_projection


def component():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_template_1m')
    p.add_argument('--repeats',type=int,default=2)
    p.add_argument('--output',default='reports/unlogged-stage-component-1m.json')
    args=p.parse_args()
    if args.repeats<1:p.error('Positive repeats required')
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    value='ROW('+','.join(['raw_id']+fields+['pipeline_domain_safe'])+')'
    report={'kind':'rollback_only_ephemeral_stage_persistence_components','official_score':False,'hook_sha256':source_identity(),'runs':[],'scope':'Alternating instrumented CTAS components, not full-service timing. Count/two typed hashes compare cache contents; WAL insert-LSN delta is cluster-wide and may include background work. No durable final/checkpoint relation is changed.'}
    with connect(args.database,autocommit=True) as c:
        require_lab(c);report['environment']=environment(c)
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        expected=None
        for repeat in range(args.repeats):
            for mode in (('logged','unlogged') if repeat%2==0 else ('unlogged','logged')):
                with c.transaction(force_rollback=True):
                    c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB';")
                    schema='persistence_probe_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                    query=(original_projection if mode=='logged' else unlogged_projection)(scopes,logged=True).replace('challenge_lab.pipeline_normalized_input',schema+'.input',1)
                    digest=hashlib.sha256(query.encode()).hexdigest()
                    with (ROOT/'reports/sql'/(digest+'.sql')).open('w',encoding='utf-8',newline='\n') as f:f.write(query)
                    start_lsn=c.execute('SELECT pg_current_wal_insert_lsn()').fetchone()[0]
                    started=time.perf_counter();plan=c.execute('EXPLAIN (ANALYZE,BUFFERS,WAL,TIMING OFF,FORMAT JSON) '+query).fetchone()[0][0];seconds=time.perf_counter()-started
                    wal=c.execute('SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s::pg_lsn)',(start_lsn,)).fetchone()[0]
                    persistence,size=c.execute('SELECT relpersistence,pg_total_relation_size(oid) FROM pg_class WHERE oid=%s::regclass',(schema+'.input',)).fetchone()
                    assert persistence==('p' if mode=='logged' else 'u')
                    fp=c.execute(f'SELECT count(*),sum(hash_record_extended({value},17)::numeric)::text,sum(hash_record_extended({value},97)::numeric)::text FROM {schema}.input').fetchone()
                    if expected is None:expected=fp
                    assert fp==expected,'Cache contents differ'
                    report['runs'].append({'repeat':repeat,'mode':mode,'projection_seconds':seconds,'relpersistence':persistence,'stage_bytes':size,'wal_insert_lsn_delta_bytes_clusterwide':int(wal),'content_fingerprint':fp,'sql_sha256':digest,'plan':plan})
                    print(repeat+1,mode,round(seconds,4),'seconds;',int(wal),'WAL bytes; cache fingerprint identical',flush=True)
        report['all_scratch_transactionally_rolled_back']=True
        report['summary']={mode:{'median_projection_seconds':statistics.median(r['projection_seconds'] for r in report['runs'] if r['mode']==mode),'median_cluster_wal_delta_bytes':statistics.median(r['wal_insert_lsn_delta_bytes_clusterwide'] for r in report['runs'] if r['mode']==mode)} for mode in ('logged','unlogged')}
        write_report(args.output,report)


def main():
    if len(sys.argv)<2 or sys.argv[1] not in ('component','service','semantics','atomic'):
        raise SystemExit('Use component, service, semantics or atomic with driver arguments')
    action=sys.argv.pop(1)
    if action=='component':component();return
    install()
    if action=='service':
        from bench.service_run import main as driver
    elif action=='semantics':
        from tests.semantics.native_copy import main as driver
    else:
        import tests.recovery.native_copy_atomic as module
        output='reports/recovery-unlogged-stage-atomic.json'
        if '--output' in sys.argv:
            i=sys.argv.index('--output');output=sys.argv[i+1];del sys.argv[i:i+2]
        module.write_report=lambda ignored,value:write_report(output,{**value,'research_hook_sha256':source_identity()})
        driver=module.main
    driver()


if __name__=='__main__':main()
