"""Rollback-only PG17 JSON_TABLE extraction with typed TEXT fingerprints.

Strict root/member paths and LEFT JOIN retain one source row and avoid array
unwrapping. JSONB columns unquote via #>> '{}'; formatted TEXT columns preserve
objects/arrays and distinguish JSON null from the literal string "null".
No jsonb_to_record volatility/dependency exemption or user-defined function.
"""
import argparse
import hashlib
import statistics
import time
import uuid

from bench.common import ROOT,connect,require_lab,write_report,environment
from pipeline.parallel_normalization import prepare_input_sql,settings_sql
from pipeline.recipes import EXTRACT,destinations


def projection(scopes,mode,logged=True):
    query=prepare_input_sql(scopes,logged=logged)
    if mode=='original':return query
    if mode not in ('jsonb','formatted_text'):raise ValueError(mode)
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    values=','.join(f"p.payload->>'{key}'" for key in fields)
    defs=','.join(f'{key} text' for key in fields)
    marker='CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW('+values+')]) n('+defs+')\n'
    assert query.count(marker)==1
    columns=','.join(f"{key} {'jsonb' if mode=='jsonb' else 'text FORMAT JSON'} PATH 'strict $.{key}' "+('OMIT QUOTES ' if mode=='formatted_text' else '')+'NULL ON EMPTY NULL ON ERROR' for key in fields)
    converted=','.join(f"j.{key} #>> '{{}}'" if mode=='jsonb' else f"CASE WHEN j.{key}='null' AND p.payload->'{key}'='null'::jsonb THEN NULL ELSE j.{key} END" for key in fields)
    replacement="LEFT JOIN LATERAL JSON_TABLE(p.payload, 'strict $' COLUMNS ("+columns+")) j ON true\nCROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW("+converted+')]) n('+defs+')\n'
    return query.replace(marker,replacement,1)


def workers(node):
    return max([node.get('Workers Launched',0)]+[workers(p) for p in node.get('Plans',[])])


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_100k')
    p.add_argument('--repeats',type=int,default=1)
    p.add_argument('--output',default='reports/json-table-projection-100k.json')
    args=p.parse_args()
    if args.repeats<1:p.error('Positive repeats required')
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    value='ROW('+','.join(['raw_id']+fields+['pipeline_domain_safe'])+')'
    report={'kind':'rollback_only_json_table_extraction','official_score':False,'runs':[],'scope':'Instrumented projection only. Row count plus two independent typed TEXT/proof hash sums; no exact per-row comparison, full pipeline time, universal semantic proof or dependency-guard exemption.'}
    with connect(args.database,autocommit=True) as c:
        require_lab(c);report['environment']=environment(c)
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        expected=None
        for repeat in range(args.repeats):
            order=['original','jsonb','formatted_text'];shift=repeat%3
            for mode in order[shift:]+order[:shift]:
                with c.transaction(force_rollback=True):
                    c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB';")
                    schema='json_table_probe_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                    query=projection(scopes,mode).replace('challenge_lab.pipeline_normalized_input',schema+'.input',1)
                    digest=hashlib.sha256(query.encode()).hexdigest()
                    with (ROOT/'reports/sql'/(digest+'.sql')).open('w',encoding='utf-8',newline='\n') as f:f.write(query)
                    print(mode,'starting',flush=True)
                    started=time.perf_counter();plan=c.execute('EXPLAIN (ANALYZE,BUFFERS,WAL,TIMING OFF,FORMAT JSON) '+query).fetchone()[0][0];seconds=time.perf_counter()-started
                    result=c.execute(f'SELECT count(*),sum(hash_record_extended({value},17)::numeric)::text,sum(hash_record_extended({value},97)::numeric)::text FROM {schema}.input').fetchone()
                    if expected is None:expected=result
                    assert result==expected,'JSON_TABLE extraction differs'
                    size=c.execute('SELECT pg_total_relation_size(%s::regclass)',(schema+'.input',)).fetchone()[0]
                    report['runs'].append({'repeat':repeat,'mode':mode,'projection_seconds':seconds,'stage_bytes':size,'workers_launched':workers(plan['Plan']),'sql_sha256':digest,'text_projection_fingerprint':result,'plan':plan})
                    print(mode,round(seconds,4),'seconds;',size,'bytes; identical TEXT/proof fingerprint',flush=True)
        report['all_scratch_transactionally_rolled_back']=True
        report['summary']={mode:{'median_projection_seconds':statistics.median(r['projection_seconds'] for r in report['runs'] if r['mode']==mode)} for mode in ('original','jsonb','formatted_text')}
        write_report(args.output,report)


if __name__=='__main__':main()
