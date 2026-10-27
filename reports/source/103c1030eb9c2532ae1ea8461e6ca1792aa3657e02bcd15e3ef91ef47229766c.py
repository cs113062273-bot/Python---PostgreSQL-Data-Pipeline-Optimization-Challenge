"""Paired rollback-only existing-data key rebuild costs; not service timing."""
import argparse
import statistics
import time
from psycopg import sql

from bench.common import connect,write_report,require_lab,environment
from bench.index_tuning_probe import install,source_identity
import pipeline.bulk_indexes as indexes
from pipeline.recipes import ORDER
from tests.equivalence.oracle import protected_schema_signature,sequence_signature


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',required=True)
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/index-tuning-components-1m.json')
    args=p.parse_args()
    if args.repeats<1:p.error('Positive repeats required')
    original=indexes.restore_sql;install();tuned=indexes.restore_sql;indexes.restore_sql=original
    report={'kind':'paired_rollback_index_rebuild_component','official_score':False,'database':args.database,'hook_sha256':source_identity(),'runs':[],'scope':'Existing loaded 1M results, no insert/transform/COPY or source processing. FK drop/restore outside key-build timer. All DDL/settings roll back. Not full-service timing or a new reduction versus official.'}
    tables=('table1','table5','table6','table2')
    with connect(args.database,autocommit=True) as c:
        require_lab(c);report['environment']=environment(c)
        before=protected_schema_signature(c);seq_before=sequence_signature(c)
        seq_values=c.execute("SELECT schemaname,sequencename,last_value FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename").fetchall()
        with c.transaction():
            c.execute('SET LOCAL search_path=pg_catalog')
            foreign_keys=c.execute("SELECT c.relname,k.conname,pg_get_constraintdef(k.oid) FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid WHERE c.relnamespace='public'::regnamespace AND c.relname=ANY(%s) AND k.contype='f' ORDER BY c.relname,k.conname",(list(ORDER),)).fetchall()
        for repeat in range(args.repeats):
            for mode in (('original','tuned') if repeat%2==0 else ('tuned','original')):
                with c.transaction(force_rollback=True):
                    c.execute('LOCK TABLE public.raw_data IN SHARE MODE')
                    # This existing-data research component is not the native
                    # empty-target bulk mode. Its guard remains unmodified.
                    for table,name,definition in foreign_keys:
                        c.execute(sql.SQL('ALTER TABLE {} DROP CONSTRAINT {}').format(sql.Identifier('public',table),sql.Identifier(name)))
                    for table in tables:c.execute(indexes.prepare_mode_sql('guarded_copy',table))
                    total=0;costs={}
                    for table in tables:
                        started=time.perf_counter();c.execute((original if mode=='original' else tuned)(table,mode='guarded_copy'));seconds=time.perf_counter()-started
                        costs[table]=seconds;total+=seconds
                    for table,name,definition in foreign_keys:
                        c.execute(sql.SQL('ALTER TABLE {} ADD CONSTRAINT {} ').format(sql.Identifier('public',table),sql.Identifier(name))+sql.SQL(definition))
                    assert protected_schema_signature(c)==before and sequence_signature(c)==seq_before
                    report['runs'].append({'repeat':repeat,'mode':mode,'four_tables_key_rebuild_seconds':total,'per_table_seconds':costs})
                    print(repeat+1,mode,round(total,4),'seconds; full restored schema identical',flush=True)
                assert protected_schema_signature(c)==before and sequence_signature(c)==seq_before
                assert c.execute("SELECT schemaname,sequencename,last_value FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename").fetchall()==seq_values
        report['all_changes_transactionally_rolled_back']=True
        report['summary']={mode:{'median_four_tables_key_rebuild_seconds':statistics.median(r['four_tables_key_rebuild_seconds'] for r in report['runs'] if r['mode']==mode)} for mode in ('original','tuned')}
        write_report(args.output,report)


if __name__=='__main__':main()
