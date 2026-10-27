"""Compare native extraction barriers under identical parallel settings."""
import argparse
import hashlib
from pathlib import Path
import statistics
import sys
import time
import uuid

from bench.common import connect,require_lab,environment,write_report
from pipeline.parallel_normalization import prepare_input_sql as original_projection,settings_sql
from pipeline.recipes import EXTRACT,destinations
from pipeline.safe_domain import _bundle


def source_identity():return hashlib.sha256(Path(__file__).read_text(encoding='utf-8').encode()).hexdigest()


def projection(scopes,mode,*,logged=False):
    body=original_projection(scopes,logged=logged)
    if mode=='array':return body
    if mode not in ('offset_fields','offset_all'):raise ValueError(mode)
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    values=','.join(f"p.payload->>'{key}'" for key in fields)
    defs=','.join(f'{key} text' for key in fields)
    marker='CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW('+values+')]) n('+defs+')'
    assert body.count(marker)==1
    body=body.replace(marker,'CROSS JOIN LATERAL (SELECT '+','.join(f"p.payload->>'{key}' AS {key}" for key in fields)+' OFFSET 0) n',1)
    if mode=='offset_all':
        marker="CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[s.raw->'values']) p(payload)"
        assert body.count(marker)==1
        body=body.replace(marker,"CROSS JOIN LATERAL (SELECT s.raw->'values' AS payload OFFSET 0) p",1)
        bundles=[_bundle('integer_column',15).replace('c.','n.'),_bundle('numeric_column',30).replace('c.','n.'),"coalesce(n.id_column_1,'')","coalesce(n.datetime_column_1,'')"]
        marker='CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW('+','.join(bundles)+')]) c(integers text,numerics text,identifier text,stamp text)'
        assert body.count(marker)==1
        body=body.replace(marker,'CROSS JOIN LATERAL (SELECT '+','.join(expr+' AS '+name for expr,name in zip(bundles,('integers','numerics','identifier','stamp')))+' OFFSET 0) c',1)
    return '-- lab offset projection source_sha256='+source_identity()+'\n'+body


def nodes(node):
    yield node
    for child in node.get('Plans',[]):yield from nodes(child)


def component():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_100k')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/offset-projection-100k.json')
    args=p.parse_args();report={'kind':'rollback_only_parallel_projection_barriers','official_score':False,'source_sha256':source_identity(),'runs':[],'scope':'Instrumented projection only under identical parallel settings, content verified by count and two typed hashes. Not a full-pipeline score.'}
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])});value='ROW('+','.join(['raw_id']+fields+['pipeline_domain_safe'])+')'
    with connect(args.database,autocommit=True) as c:
        require_lab(c);report['environment']=environment(c);scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')];expected=None
        for repeat in range(args.repeats):
            modes=['array','offset_fields','offset_all'];shift=repeat%3
            for mode in modes[shift:]+modes[:shift]:
                with c.transaction(force_rollback=True):
                    c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB';")
                    schema='offset_probe_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                    query=projection(scopes,mode,logged=True).replace('challenge_lab.pipeline_normalized_input',schema+'.input',1)
                    started=time.perf_counter();plan=c.execute('EXPLAIN (ANALYZE,BUFFERS,WAL,TIMING OFF,FORMAT JSON) '+query).fetchone()[0][0];seconds=time.perf_counter()-started
                    fp=c.execute(f'SELECT count(*),sum(hash_record_extended({value},17)::numeric)::text,sum(hash_record_extended({value},97)::numeric)::text FROM {schema}.input').fetchone()
                    if expected is None:expected=fp
                    assert fp==expected
                    workers=max(n.get('Workers Launched',0) for n in nodes(plan['Plan']))
                    report['runs'].append({'repeat':repeat,'mode':mode,'projection_seconds':seconds,'workers_launched':workers,'fingerprint':fp,'plan':plan})
                    print(repeat+1,mode,round(seconds,4),'seconds;',workers,'workers',flush=True)
        report['all_scratch_transactionally_rolled_back']=True
        report['summary']={mode:{'median_projection_seconds':statistics.median(r['projection_seconds'] for r in report['runs'] if r['mode']==mode)} for mode in ('array','offset_fields','offset_all')}
        write_report(args.output,report)


def main():
    if len(sys.argv)<2 or sys.argv[1] not in ('component','service','semantics'):raise SystemExit('Use component, service or semantics with driver arguments')
    action=sys.argv.pop(1)
    if action=='component':component();return
    import pipeline.parallel_normalization as module
    module.prepare_input_sql=lambda scopes,logged=False:projection(scopes,'offset_all',logged=logged)
    if action=='service':
        from bench.server_copy_probe import install
        install()
        from bench.service_run import main as driver
    else:
        from tests.semantics.native_copy import main as driver
    driver()


if __name__=='__main__':main()
