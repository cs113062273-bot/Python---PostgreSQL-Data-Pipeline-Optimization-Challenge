"""Rollback-only stage cost/typed-value equivalence, not a pipeline score."""
import argparse
import statistics
import time
import uuid

from bench.common import connect,write_report,environment,require_lab
from bench.typed_stage_probe import original_projection,typed_projection,source_identity
from pipeline.parallel_normalization import settings_sql
from pipeline.recipes import EXTRACT,destinations


def workers(node):
    return max([node.get('Workers Launched',0)]+[workers(p) for p in node.get('Plans',[])])


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_template_1m')
    p.add_argument('--repeats',type=int,default=1)
    p.add_argument('--output',default='reports/typed-stage-component-1m.json')
    args=p.parse_args()
    if args.repeats<1:p.error('Positive repeats required')
    report={'kind':'rollback_only_typed_stage_component','official_score':False,'hook_sha256':source_identity(),'runs':[],'scope':'Instrumented projection only; includes every safe conversion, excludes full-output transformation/COPY/indexes/FKs/counters/versions. Never a complete pipeline timing.'}
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    with connect(args.database,autocommit=True) as c:
        require_lab(c);report['environment']=environment(c)
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        expected=None
        for repeat in range(args.repeats):
            for mode in (('text','typed') if repeat%2==0 else ('typed','text')):
                with c.transaction(force_rollback=True):
                    c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB';")
                    schema='typed_probe_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                    query=(original_projection if mode=='text' else typed_projection)(scopes,logged=True).replace('challenge_lab.pipeline_normalized_input',schema+'.input',1)
                    print(mode,'projection starting',flush=True)
                    started=time.perf_counter();plan=c.execute('EXPLAIN (ANALYZE,BUFFERS,WAL,TIMING OFF,FORMAT JSON) '+query).fetchone()[0][0];seconds=time.perf_counter()-started
                    size=c.execute('SELECT pg_total_relation_size(%s::regclass)',(schema+'.input',)).fetchone()[0]
                    values=['raw_id']
                    for name in fields:
                        kind='smallint' if name.startswith('integer_column_') else 'numeric' if name.startswith('numeric_column_') else 'integer' if name=='id_column_1' else 'timestamptz' if name=='datetime_column_1' else None
                        values.append(f"CASE WHEN pipeline_domain_safe THEN NULLIF(BTRIM({name}),'')::{kind} END" if kind and mode=='text' else name)
                    values.append('pipeline_domain_safe');value='ROW('+','.join(values)+')'
                    proof=c.execute(f'SELECT count(*),bool_and(pipeline_domain_safe),sum(hash_record_extended({value},17)::numeric)::text,sum(hash_record_extended({value},97)::numeric)::text FROM {schema}.input').fetchone()
                    if expected is None:expected=proof
                    assert proof==expected,'Typed values differ from original native casts'
                    row={'mode':mode,'repeat':repeat,'projection_seconds':seconds,'stage_bytes':size,'workers_launched':workers(plan['Plan']),'typed_value_fingerprint':proof,'plan':plan}
                    report['runs'].append(row);print(mode,round(seconds,4),size,'bytes; typed values identical',flush=True)
        report['all_scratch_transactionally_rolled_back']=True
        report['summary']={mode:{'median_projection_seconds':statistics.median(r['projection_seconds'] for r in report['runs'] if r['mode']==mode),'stage_bytes':next(r['stage_bytes'] for r in report['runs'] if r['mode']==mode)} for mode in ('text','typed')}
        write_report(args.output,report)


if __name__=='__main__':main()
