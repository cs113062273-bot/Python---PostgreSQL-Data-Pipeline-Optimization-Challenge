"""Fresh, unranked CTAS worker plans; all data/DDL is rolled back."""
import argparse
import uuid
from psycopg import sql
from bench.common import connect,write_report,environment
from bench.run import prepared
from pipeline.recipes import CTAS_MODES,bulk_unique_tables
from pipeline.parallel_normalization import materialize_statement,settings_sql,restore_settings_sql
from pipeline.guarded_normalization import prepare_sql,fast_statement
from pipeline.bulk_fks import prepare_sql as fk_prepare,restore_sql as fk_restore
from pipeline.bulk_indexes import prepare_mode_sql,restore_sql


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--template',default='lab_generated_100k')
    p.add_argument('--mode',choices=CTAS_MODES,default='lab_parallel_bulk')
    p.add_argument('--output',default='reports/plans/parallel-ctas-100k.json')
    args=p.parse_args()
    if not args.template.startswith('lab_'):p.error('Use a preserved lab_ template')
    db='lab_ctas_profile_'+uuid.uuid4().hex[:8]
    with connect('postgres',autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {} STRATEGY FILE_COPY').format(sql.Identifier(db),sql.Identifier(args.template)))
    report={'kind':'instrumented_rollback_ctas_plans_not_ranked','mode':args.mode,'database':db,'plans':{}}
    with connect(db,autocommit=True) as c:
        report['environment']=environment(c)
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        with c.transaction(force_rollback=True):
            c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            c.execute('LOCK TABLE public.raw_data IN SHARE MODE')
            c.execute(prepare_sql(scopes,args.mode))
            assert c.execute('SELECT safe FROM pg_temp.pipeline_normalization_domain').fetchone()[0]
            c.execute(fk_prepare())
            for d in prepared(args.mode,scopes):
                table=d['pure_table_name']
                if table in bulk_unique_tables(args.mode):c.execute(prepare_mode_sql(args.mode,table))
                stage,insert,cleanup=materialize_statement(fast_statement(d['statements'][0],scopes,args.mode),mode=args.mode)
                ctas=stage[stage.index('CREATE TEMP TABLE'):stage.rindex(';\nDO $restore_parallel$')]
                c.execute(settings_sql())
                if args.mode=='lab_parallel_cache' and table=='table1':c.execute('SET LOCAL enable_parallel_hash=off')
                plan=c.execute('EXPLAIN (ANALYZE,BUFFERS,WAL,SETTINGS,FORMAT JSON,TIMING OFF) '+ctas).fetchone()[0]
                c.execute(restore_settings_sql())
                report['plans'][table]=plan
                c.execute(insert)
                c.execute(cleanup)
                if table in bulk_unique_tables(args.mode):c.execute(restore_sql(table,mode=args.mode))
                if table in ('table3','table4'):c.execute('ANALYZE public.'+table)
                print(table,'CTAS',plan[0]['Execution Time'],'ms',flush=True)
            c.execute(fk_restore())
    write_report(args.output,report)


if __name__=='__main__':main()
