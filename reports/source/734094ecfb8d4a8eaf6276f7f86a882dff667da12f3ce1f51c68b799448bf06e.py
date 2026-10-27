"""Rollback-only rebuild/validation tuning on our verified 1M candidate clone."""
import json
import time
import argparse
from bench.common import connect,require_lab,ROOT,write_report,environment
from pipeline.bulk_indexes import prepare_mode_sql,restore_sql
from pipeline.bulk_fks import FOREIGN_KEYS,restore_sql as restore_fks
from pipeline.parallel_normalization import settings_sql
from tests.equivalence.oracle import schema_signature


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--phase',choices=['all','indexes','fk'],default='all')
    args=p.parse_args()
    receipt=json.loads((ROOT/'reports/fresh-parallel-ctas-bulk-1m.json').read_text())
    run=next(r for r in receipt['runs'] if r['mode']=='lab_parallel_bulk')
    assert run['exact_except_all'] and run['schema_equivalent']
    db=run['database']
    report={'kind':'rollback_only_maintenance_component_not_ranked','database':db,'runs':[]}
    with connect(db,autocommit=True) as c:
        require_lab(c)
        report['environment']=environment(c)
        before=schema_signature(c)
        variants=[(2,128),(0,128),(0,256),(2,256),(4,512)]
        for repeat in range(2) if args.phase!='fk' else []:
            for workers,memory in variants if repeat==0 else list(reversed(variants)):
                with c.transaction(force_rollback=True):
                    c.execute('SET LOCAL max_parallel_maintenance_workers='+str(workers))
                    start=time.perf_counter()
                    c.execute(prepare_mode_sql('lab_parallel_bulk','table2'))
                    restore=restore_sql('table2',mode='lab_parallel_bulk')
                    assert restore.count("'128MB'")==1
                    c.execute(restore.replace("'128MB'","'"+str(memory)+"MB'"))
                    elapsed=time.perf_counter()-start
                assert schema_signature(c)==before
                row={'phase':'table2_two_unique_indexes','repeat':repeat,'max_workers':workers,'maintenance_memory_mb':memory,'wall_seconds':elapsed}
                report['runs'].append(row);print(row,flush=True)
                write_report('reports/maintenance-components-1m.json',report)
        for repeat in range(2) if args.phase!='indexes' else []:
            for workers in [0,2,4,8] if repeat==0 else [8,4,2,0]:
                with c.transaction(force_rollback=True):
                    c.execute('SET LOCAL search_path=pg_catalog')
                    c.execute('CREATE TEMP TABLE pipeline_bulk_foreign_keys(table_name text,constraint_name text,definition text) ON COMMIT DROP')
                    for table,name,definition in FOREIGN_KEYS:
                        actual=c.execute('SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=%s::regclass AND conname=%s',('public.'+table,name)).fetchone()[0]
                        assert actual==definition
                        c.execute('INSERT INTO pg_temp.pipeline_bulk_foreign_keys VALUES(%s,%s,%s)',(table,name,definition))
                        from psycopg import sql
                        c.execute(sql.SQL('ALTER TABLE {} DROP CONSTRAINT {}').format(sql.Identifier('public',table),sql.Identifier(name)))
                    c.execute(settings_sql(workers))
                    start=time.perf_counter()
                    c.execute(restore_fks())
                    elapsed=time.perf_counter()-start
                assert schema_signature(c)==before
                row={'phase':'six_fk_validation','repeat':repeat,'max_workers':workers,'wall_seconds':elapsed}
                report['runs'].append(row);print(row,flush=True)
                write_report('reports/fk-validation-components-1m.json' if args.phase=='fk' else 'reports/maintenance-components-1m.json',report)


if __name__=='__main__':main()
