"""Isolated native planner reuse of one unchanged safe-domain predicate."""
import argparse
import hashlib
from pathlib import Path
import statistics
import sys
import time
import uuid

from bench.common import ROOT,connect,require_lab,environment,write_report
from pipeline.parallel_normalization import prepare_input_sql as original_projection,settings_sql
from pipeline.safe_domain import condition_parts,condition_sql
from pipeline.recipes import EXTRACT,destinations


def source_identity():return hashlib.sha256(Path(__file__).read_text(encoding='utf-8').encode()).hexdigest()


def memo_projection(scopes,*,logged=False):
    body=original_projection(scopes,logged=logged);parts=condition_parts()
    marker=condition_sql()+' AS pipeline_domain_safe'
    if body.count(marker)!=1:raise RuntimeError('Unknown conservative proof boundary')
    body=body.replace(marker,'integer_proof.safe AND '+' AND '.join(parts[1:])+' AS pipeline_domain_safe',1)
    assert body.endswith(';')
    return '-- lab integer predicate barrier source_sha256='+source_identity()+'\n'+body[:-1]+'\nCROSS JOIN LATERAL pg_catalog.unnest(ARRAY['+parts[0]+']) integer_proof(safe);'


def install():
    import pipeline.parallel_normalization as module
    module.prepare_input_sql=memo_projection


def nodes(node):
    yield node
    for child in node.get('Plans',[]):yield from nodes(child)


def component():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_100k')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/proof-memoize-component-100k.json')
    args=p.parse_args()
    if args.repeats<1:p.error('Positive repeats required')
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    value='ROW('+','.join(['raw_id']+fields+['pipeline_domain_safe'])+')'
    report={'kind':'rollback_only_proof_planner_component','official_score':False,'hook_sha256':source_identity(),'runs':[],'scope':'Instrumented CTAS only. Count/two typed content hashes are diagnostics, not exact comparison. No input is accepted by a different proof, no new function or volatility exemption.'}
    with connect(args.database,autocommit=True) as c:
        require_lab(c);report['environment']=environment(c)
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')];expected=None
        for repeat in range(args.repeats):
            for mode in (('original','integer_barrier') if repeat%2==0 else ('integer_barrier','original')):
                with c.transaction(force_rollback=True):
                    c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ; LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB';")
                    schema='proof_probe_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                    query=(original_projection if mode=='original' else memo_projection)(scopes,logged=True).replace('challenge_lab.pipeline_normalized_input',schema+'.input',1)
                    started=time.perf_counter();plan=c.execute('EXPLAIN (ANALYZE,BUFFERS,WAL,TIMING OFF,FORMAT JSON) '+query).fetchone()[0][0];seconds=time.perf_counter()-started
                    fp=c.execute(f'SELECT count(*),sum(hash_record_extended({value},17)::numeric)::text,sum(hash_record_extended({value},97)::numeric)::text FROM {schema}.input').fetchone()
                    if expected is None:expected=fp
                    assert fp==expected
                    caches=[{k:n.get(k) for k in ('Cache Key','Cache Mode','Cache Hits','Cache Misses','Cache Evictions','Peak Memory Usage')} for n in nodes(plan['Plan']) if n.get('Node Type')=='Memoize']
                    report['runs'].append({'repeat':repeat,'mode':mode,'projection_seconds':seconds,'content_fingerprint':fp,'memoize_nodes':caches,'plan':plan})
                    print(repeat+1,mode,round(seconds,4),'seconds; memoize nodes',len(caches),flush=True)
        report['all_scratch_transactionally_rolled_back']=True
        report['summary']={mode:{'median_projection_seconds':statistics.median(r['projection_seconds'] for r in report['runs'] if r['mode']==mode)} for mode in ('original','integer_barrier')}
        write_report(args.output,report)


def main():
    if len(sys.argv)<2 or sys.argv[1] not in ('component','service','semantics'):raise SystemExit('Use component, service or semantics with driver arguments')
    action=sys.argv.pop(1)
    if action=='component':component();return
    install()
    if action=='service':
        from bench.service_run import main as driver
    else:
        from tests.semantics.native_copy import main as driver
    driver()


if __name__=='__main__':main()
