"""Unranked EXPLAIN-only join alternatives in our retained profile clone."""
import json
import re
import argparse
from bench.common import connect,write_report,ROOT
from bench.run import prepared
from pipeline.guarded_normalization import prepare_sql,fast_statement
from pipeline.parallel_normalization import materialize_statement,settings_sql


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--analyze',action='store_true')
    args=parser.parse_args()
    previous=json.loads((ROOT/'reports/plans/parallel-ctas-100k.json').read_text())
    db=previous['database']
    if not db.startswith('lab_ctas_profile_'):raise RuntimeError('Expected our rollback-only profile clone')
    report={'kind':'unranked_join_tuning','analyze':args.analyze,'database':db,'plans':{}}
    explain='EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON,TIMING OFF) ' if args.analyze else 'EXPLAIN (FORMAT JSON) '
    with connect(db,autocommit=True) as c:
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data')]
        with c.transaction(force_rollback=True):
            c.execute(prepare_sql(scopes,'lab_parallel_bulk'))
            assert c.execute('SELECT safe FROM pg_temp.pipeline_normalization_domain').fetchone()[0]
            recipes=prepared('lab_parallel_bulk',scopes)
            for d in recipes[:2]:
                c.execute(fast_statement(d['statements'][0],scopes,'lab_parallel_bulk'))
                c.execute('ANALYZE public.'+d['pure_table_name'])
            statement=fast_statement(recipes[2]['statements'][0],scopes,'lab_parallel_bulk')
            c.execute(settings_sql())
            for name,settings in [('default',''),('serial_hash','SET LOCAL enable_parallel_hash=off'),('no_nested_loop','SET LOCAL enable_nestloop=off'),('dimension_worker_hint','ALTER TABLE public.table3 SET (parallel_workers=8)')]:
                with c.transaction(force_rollback=True):
                    if settings:c.execute(settings)
                    stage,_,_=materialize_statement(statement)
                    q=stage[stage.index('CREATE TEMP TABLE'):stage.rindex(';\nDO $restore_parallel$')]
                    report['plans'][name]=c.execute(explain+q).fetchone()[0]
            c.execute('ALTER TABLE challenge_lab.pipeline_normalized_input ADD COLUMN pipeline_dim text,ADD COLUMN pipeline_left text,ADD COLUMN pipeline_right text')
            c.execute("UPDATE challenge_lab.pipeline_normalized_input SET pipeline_dim=btrim(text_column_1),pipeline_left=left(upper(btrim(text_column_2)),1),pipeline_right=right(upper(btrim(text_column_2)),1)")
            c.execute('ANALYZE challenge_lab.pipeline_normalized_input(pipeline_dim,pipeline_left,pipeline_right)')
            statement=statement.replace('BTRIM(source_row.text_column_1)','source_row.pipeline_dim')
            for fn,col in [('LEFT','pipeline_left'),('RIGHT','pipeline_right')]:
                pattern=fn+r'\(\s*UPPER\(BTRIM\(source_row\.text_column_2\)\),\s*1\s*\)'
                statement,n=re.subn(pattern,'source_row.'+col,statement)
                assert n==1
            stage,_,_=materialize_statement(statement)
            q=stage[stage.index('CREATE TEMP TABLE'):stage.rindex(';\nDO $restore_parallel$')]
            report['plans']['normalized_join_statistics']=c.execute(explain+q).fetchone()[0]
    write_report('reports/plans/parallel-join-tuning'+('-analyze' if args.analyze else '')+'.json',report)
    def nodes(n):
        yield n
        for x in n.get('Plans',[]):yield from nodes(x)
    for name,plan in report['plans'].items():
        print(name,plan[0].get('Execution Time'),[(n['Node Type'],n.get('Workers Planned'),n.get('Workers Launched'),n.get('Plan Rows')) for n in nodes(plan[0]['Plan'])],flush=True)


if __name__=='__main__':main()
