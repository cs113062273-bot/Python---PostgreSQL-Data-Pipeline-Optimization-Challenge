"""Original source manifest query with native parallel planner settings."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import time
from bench import native_c_lab as lab
from bench.common import ROOT,write_report,require_lab
from pipeline.upstream import PostgresAdminService


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--database',required=True);p.add_argument('--repeats',type=int,default=3);p.add_argument('--output',required=True);args=p.parse_args()
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    report={'kind':'native_original_manifest_parallel_component','database':args.database,'runs':[],
            'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'scope':'Actual unchanged original manifest SQL: scope/hash counts, missing-scope check, sorted full dependency JSON and source catalog columns. Planner settings only; temporary manifest rolled back each trial. No normalization/ingest/commit/versions or full-service percentage.'}
    with lab.connect(args.database,autocommit=True) as c:
        require_lab(c);scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        query=PostgresAdminService._build_expansion_manifest_sql('public','raw_data',scopes)
        with c.transaction(force_rollback=True):
            cursor=c.execute(query)
            while cursor.nextset():pass
            expected=json.loads(cursor.fetchone()[0])
        for repeat in range(args.repeats):
            modes=['original',2,4,8];shift=repeat%4
            for workers in modes[shift:]+modes[:shift]:
                with c.transaction(force_rollback=True):
                    c.execute("SET LOCAL statement_timeout='90s'")
                    if workers!='original':c.execute('SET LOCAL max_parallel_workers_per_gather='+str(workers)+'; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0')
                    start=time.perf_counter();cursor=c.execute(query)
                    while cursor.nextset():pass
                    actual=json.loads(cursor.fetchone()[0]);seconds=time.perf_counter()-start
                    assert actual==expected
                assert c.execute("SELECT to_regclass('pg_temp.pgdm_cross_table_hash_manifest')").fetchone()==(None,)
                report['runs'].append({'repeat':repeat,'workers':workers,'seconds':seconds,'entire_original_manifest_exact':True,'private_manifest_rolled_back':True});write_report(output,report)
                print(repeat,workers,round(seconds,6),'entire source manifest exact',flush=True)
    report['summary']={str(w):statistics.median(r['seconds'] for r in report['runs'] if r['workers']==w) for w in ['original',2,4,8]};report['passed']=True;write_report(output,report);print(report['summary'],flush=True)


if __name__=='__main__':main()
