"""Rollback-only native parallel CTAS probe; not a pipeline speed score."""
import argparse
import time
import psutil
from bench.common import connect,require_lab,environment,write_report
from pipeline.normalization import prepare_sql
from pipeline.parallel_normalization import prepare_input_sql,settings_sql
from pipeline.recipes import EXTRACT,destinations
from pipeline.safe_domain import gate_sql


def unnest_projection(statement):
    original="CROSS JOIN LATERAL (SELECT source_row.raw -> 'values' AS payload OFFSET 0) projected"
    replacement="CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[source_row.raw -> 'values']) AS projected(payload)"
    if statement.count(original)!=1:raise ValueError('Unexpected projection')
    return statement.replace(original,replacement)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_template_1m')
    p.add_argument('--repeats',type=int,default=1)
    p.add_argument('--experiment',choices=['domain','extraction'],default='domain')
    p.add_argument('--output',default='reports/parallel-extraction-probe-1m.json')
    args=p.parse_args()
    report={'kind':'rollback_only_component_experiment','official_score':False,'runs':[]}
    with connect(args.database,autocommit=True) as c:
        require_lab(c)
        process=psutil.Process(c.info.backend_pid)
        original_affinity=process.cpu_affinity()
        report['environment']=environment(c)
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        for repeat in range(args.repeats):
            modes=[('offset_serial',0),('proof_serial',0),('proof_parallel4',4),('proof_parallel8',8)] if args.experiment=='domain' else [('offset_serial',0),('unnest_serial',0),('unnest_parallel2',2),('unnest_parallel4',4),('unnest_parallel8',8)]
            for mode,workers in modes:
                with c.transaction(force_rollback=True):
                    process.cpu_affinity([2] if not workers else original_affinity)
                    c.execute(settings_sql(workers))
                    q=prepare_sql(scopes,all_tables=True,defer_index=True)
                    if mode.startswith('proof_'):q=prepare_input_sql(scopes)
                    if mode.startswith('unnest_'):q=unnest_projection(q)
                    started=time.perf_counter()
                    plan=c.execute('EXPLAIN (ANALYZE,BUFFERS,VERBOSE,FORMAT JSON,TIMING OFF) '+q).fetchone()[0]
                    elapsed=time.perf_counter()-started
                    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
                    fingerprint=c.execute("SELECT count(*),sum(hash_record_extended(ROW(raw_id,"+','.join(fields)+"),17)::numeric)::text FROM pg_temp.pipeline_normalized_input t").fetchone()
                    gate_started=time.perf_counter()
                    safe=c.execute('SELECT bool_and(pipeline_domain_safe) FROM pg_temp.pipeline_normalized_input' if mode.startswith('proof_') else gate_sql()).fetchone()[0]
                    gate_seconds=time.perf_counter()-gate_started
                    row={'mode':mode,'repeat':repeat,'seconds_instrumented':elapsed,'global_gate_seconds':gate_seconds,'safe':safe,'fingerprint':fingerprint,'plan':plan}
                    report['runs'].append(row)
                    print(mode,elapsed,fingerprint,flush=True)
                write_report(args.output,report)
        report['fingerprints_equivalent']=len({tuple(r['fingerprint']) for r in report['runs']})==1
        write_report(args.output,report)


if __name__=='__main__':main()
