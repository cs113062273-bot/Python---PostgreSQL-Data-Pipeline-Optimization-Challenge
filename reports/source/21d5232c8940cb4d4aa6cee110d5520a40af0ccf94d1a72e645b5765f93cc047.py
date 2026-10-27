"""Native metadata fallbacks for the two additional bulk uniqueness proofs."""
import uuid
from bench.fixture import create_fixture
from bench.common import connect,write_report
from bench.run import execute
from pipeline.bulk_indexes import prepare_partial_sql,prepare_sql,restore_sql
from tests.equivalence.oracle import schema_signature,fingerprints


def main():
    db='lab_parallel_index_'+uuid.uuid4().hex[:8]
    create_fixture(db)
    results=[]
    partial='public.table5_id_column_2_when_id_column_3_null_key'
    unique='public.table6_id_column_2_id_column_3_key'
    with connect(db,autocommit=True) as c:
        execute(c,'baseline',['group001'],reset_destinations=False)
        schema=schema_signature(c);data=fingerprints(c)
        cases=[
            ('native_partial_removed_and_restored','',partial,prepare_partial_sql(),True),
            ('partial_storage_option_retained',f'ALTER INDEX {partial} SET (fillfactor=80)',partial,prepare_partial_sql(),False),
            ('partial_descending_order_retained',f'DROP INDEX {partial}; CREATE UNIQUE INDEX table5_id_column_2_when_id_column_3_null_key ON public.table5(id_column_2 DESC) WHERE id_column_3 IS NULL',partial,prepare_partial_sql(),False),
            ('different_predicate_retained',f'DROP INDEX {partial}; CREATE UNIQUE INDEX table5_id_column_2_when_id_column_3_null_key ON public.table5(id_column_2) WHERE id_column_3 IS NULL AND numeric_column_1>0',partial,prepare_partial_sql(),False),
            ('include_column_retained',f'DROP INDEX {partial}; CREATE UNIQUE INDEX table5_id_column_2_when_id_column_3_null_key ON public.table5(id_column_2) INCLUDE(numeric_column_1) WHERE id_column_3 IS NULL',partial,prepare_partial_sql(),False),
            ('native_table6_unique_removed_and_restored','',unique,prepare_sql('table6'),True),
            ('external_reference_retains_table6_unique','CREATE TABLE public.external_reference(a bigint,b smallint,FOREIGN KEY(a,b) REFERENCES public.table6(id_column_2,id_column_3))',unique,prepare_sql('table6'),False),
        ]
        for name,setup,index,prepare,removed in cases:
            with c.transaction(force_rollback=True):
                if setup:c.execute(setup)
                before=schema_signature(c)
                c.execute(prepare)
                actual=c.execute('SELECT to_regclass(%s) IS NULL',(index,)).fetchone()[0]
                assert actual is removed,(name,actual)
                if removed:
                    c.execute(restore_sql('table5' if index==partial else 'table6',mode='lab_parallel_bulk'))
                    assert schema_signature(c)==before,name
                assert fingerprints(c)==data,name
            assert schema_signature(c)==schema,name
            results.append({'name':name,'index_removed':removed,'passed':True})
        # Interrupted index removal must restore both the definition and data.
        for table,prepare in [('table5',prepare_partial_sql()),('table6',prepare_sql('table6'))]:
            try:
                with c.transaction():
                    c.execute(prepare)
                    raise RuntimeError('failure after index drop')
            except RuntimeError:pass
            assert schema_signature(c)==schema and fingerprints(c)==data
            results.append({'name':'exception_rollback_'+table,'passed':True})
    write_report('reports/recovery-parallel-indexes.json',{'database':db,'all_passed':True,'cases':results})
    print('Additional index fallbacks/restoration:',len(results),'PASS',flush=True)


if __name__=='__main__':main()
