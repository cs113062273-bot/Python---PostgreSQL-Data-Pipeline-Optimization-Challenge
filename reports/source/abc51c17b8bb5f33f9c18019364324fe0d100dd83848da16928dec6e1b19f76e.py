"""Rollback-only native index/FK rebuilds on retained completed output.

No source or destination rows change. This isolates real complete index builds
and full FK validation, not query-plan estimates or a whole-pipeline score.
"""
import argparse
from datetime import datetime,timezone
import hashlib
from pathlib import Path
import statistics
import time
from bench import native_c_lab as lab
from bench.common import ROOT,write_report,require_lab
from pipeline.bulk_fks import FOREIGN_KEYS,restore_sql as restore_fks
from pipeline.bulk_indexes import prepare_mode_sql,restore_sql as restore_indexes
from tests.equivalence.oracle import protected_schema_signature,sequence_signature


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',required=True);p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',required=True);args=p.parse_args()
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    modes=[('128m_workers2',128,2),('128m_workers0',128,0),('512m_workers0',512,0),('512m_workers2',512,2)]
    report={'kind':'native_maintenance_rollback_component','database':args.database,
            'timestamp_utc':datetime.now(timezone.utc).isoformat(),'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'runs':[],'whole_pipeline_score':False,'scope':'Real original four-table index rebuild and six complete FK validations on already populated retained output, warmed repeated rotating order. Every trial rolls back. No row DML, final ingest/commit/version cost or complete-processing improvement claimed.'}
    with lab.connect(args.database,autocommit=True) as c:
        c.execute('SET search_path=pg_catalog')
        require_lab(c);before=protected_schema_signature(c);sequences=sequence_signature(c)
        assert c.execute('SELECT count(*) FROM public.raw_data').fetchone()[0]==1000000
        for repeat in range(args.repeats):
            shift=repeat%len(modes)
            for name,memory,workers in modes[shift:]+modes[:shift]:
                with c.transaction(force_rollback=True):
                    c.execute("SET LOCAL statement_timeout='180s'; SET LOCAL work_mem='64MB'; SET LOCAL search_path=pg_catalog")
                    c.execute('SET LOCAL max_parallel_maintenance_workers='+str(workers))
                    c.execute("SET LOCAL maintenance_work_mem='"+str(memory)+"MB'")
                    c.execute('LOCK TABLE public.raw_data IN SHARE MODE')
                    c.execute('CREATE TEMP TABLE pipeline_bulk_foreign_keys(table_name text,constraint_name text,definition text) ON COMMIT DROP')
                    for table,constraint,definition in FOREIGN_KEYS:
                        actual=c.execute("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=%s::regclass AND conname=%s AND convalidated",('public.'+table,constraint)).fetchone()
                        assert actual==(definition,),(table,constraint,actual,definition)
                        c.execute('INSERT INTO pg_temp.pipeline_bulk_foreign_keys VALUES(%s,%s,%s)',(table,constraint,definition))
                        c.execute('ALTER TABLE public.'+table+' DROP CONSTRAINT '+constraint)
                    for table in ('table1','table2','table5','table6'):c.execute(prepare_mode_sql('guarded_copy',table))
                    durations={}
                    for table in ('table1','table2','table5','table6'):
                        query=restore_indexes(table,mode='guarded_copy').replace("'128MB'","'"+str(memory)+"MB'")
                        start=time.perf_counter();c.execute(query);durations[table]=time.perf_counter()-start
                    start=time.perf_counter();c.execute(restore_fks());durations['all_foreign_keys']=time.perf_counter()-start
                    assert protected_schema_signature(c)==before
                    assert sequence_signature(c)==sequences
                assert protected_schema_signature(c)==before and sequence_signature(c)==sequences
                row={'repeat':repeat,'mode':name,'seconds':durations,'total_component_seconds':sum(durations.values()),'catalog_sequence_options_restored':True,'all_private_ddl_rolled_back':True}
                report['runs'].append(row);write_report(output,report)
                print(repeat,name,round(row['total_component_seconds'],5),durations,flush=True)
    report['summary']={name:{'median_component_seconds':statistics.median(r['total_component_seconds'] for r in report['runs'] if r['mode']==name),
                            'median_by_stage':{stage:statistics.median(r['seconds'][stage] for r in report['runs'] if r['mode']==name) for stage in ('table1','table2','table5','table6','all_foreign_keys')}} for name,_,_ in modes}
    report['all_passed']=True;write_report(output,report);print(report['summary'],flush=True)


if __name__=='__main__':main()
