"""Real complete FK validation under alternative native planner settings."""
import argparse
import hashlib
from pathlib import Path
import statistics
import time
from bench import native_c_lab as lab
from bench.common import ROOT,write_report,require_lab
from pipeline.bulk_fks import FOREIGN_KEYS
from tests.equivalence.oracle import protected_schema_signature,sequence_signature


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--database',required=True);p.add_argument('--repeats',type=int,default=3);p.add_argument('--output',required=True);p.add_argument('--hash-only-compare',action='store_true');args=p.parse_args()
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    modes=[('original',''),('merge_only','SET LOCAL enable_hashjoin=off; SET LOCAL enable_nestloop=off'),('index_cost','SET LOCAL random_page_cost=1.1; SET LOCAL effective_cache_size=\'4GB\'')]
    if args.hash_only_compare:modes=[('original',''),('hash_only','SET LOCAL enable_mergejoin=off; SET LOCAL enable_nestloop=off')]
    report={'kind':'native_full_fk_planner_component','database':args.database,'runs':[],
            'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'scope':'Six actual ALTER TABLE ADD validated FKs, rotating retained completed output. All DDL and settings roll back. Existing relation visibility/cache state may differ from freshly ingested rows. No full-service speedup claim.'}
    with lab.connect(args.database,autocommit=True) as c:
        c.execute('SET search_path=pg_catalog');require_lab(c)
        before=protected_schema_signature(c);sequences=sequence_signature(c)
        assert c.execute('SELECT count(*) FROM public.raw_data').fetchone()==(1000000,)
        report['relation_visibility']=c.execute("SELECT relname,reltuples,relpages,relallvisible FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind='r' ORDER BY relname").fetchall()
        for repeat in range(args.repeats):
            shift=repeat%len(modes)
            for name,settings in modes[shift:]+modes[:shift]:
                with c.transaction(force_rollback=True):
                    c.execute("SET LOCAL statement_timeout='90s'; SET LOCAL maintenance_work_mem='128MB'")
                    if settings:c.execute(settings)
                    for table,constraint,definition in FOREIGN_KEYS:
                        assert c.execute('SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=%s::regclass AND conname=%s AND convalidated',('public.'+table,constraint)).fetchone()==(definition,)
                        c.execute('ALTER TABLE public.'+table+' DROP CONSTRAINT '+constraint)
                    durations={}
                    for table,constraint,definition in FOREIGN_KEYS:
                        start=time.perf_counter();c.execute('ALTER TABLE public.'+table+' ADD CONSTRAINT '+constraint+' '+definition);durations[constraint]=time.perf_counter()-start
                    assert protected_schema_signature(c)==before and sequence_signature(c)==sequences
                assert protected_schema_signature(c)==before
                report['runs'].append({'repeat':repeat,'mode':name,'seconds':sum(durations.values()),'constraints':durations,'all_six_full_native_validations':True,'all_private_ddl_rolled_back':True});write_report(output,report)
                print(repeat,name,round(sum(durations.values()),5),durations,flush=True)
    report['summary']={name:statistics.median(r['seconds'] for r in report['runs'] if r['mode']==name) for name,_ in modes};report['passed']=True;write_report(output,report);print(report['summary'],flush=True)


if __name__=='__main__':main()
