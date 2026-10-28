"""Native FK completion: actual gaps, NULLs, fallback, later RI and rollback."""
import hashlib
import json
import uuid
from pathlib import Path

import psycopg
from psycopg import sql
from bench import native_c_lab as lab,native_fk_probe as candidate
from bench.native_fk_scan_component import FKS,TABLES
from bench.common import ROOT,write_report
from bench.run import execute
from tests.equivalence.oracle import protected_schema_signature,sequence_signature


def main():
    output=ROOT/'reports/native-fk-validator-semantics.json';assert not output.exists()
    db='lab_fk_validation_'+uuid.uuid4().hex[:8];lab.fixture(db)
    info=candidate.package();checks=[]
    call='SELECT fk_lab.fk_validate('+','.join("'public."+t+"'::regclass" for t in TABLES)+')'
    report={'kind':'full_fk_native_validator_semantics','database':db,'source_sha256':info['source_sha256'],'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'checks':checks,'passed':False}
    with lab.connect(db,autocommit=True) as c:
        execute(c,'baseline',['group001'])
        original=[]
        for table,column in FKS:
            name=table+'_'+column+'_fkey'
            definition=c.execute('SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=%s::regclass AND conname=%s',('public.'+table,name)).fetchone()[0]
            original.append((table,column,name,definition))
        before=protected_schema_signature(c);sequences=sequence_signature(c)
        def flags():return c.execute("SELECT count(*) FROM pg_constraint WHERE connamespace='public'::regnamespace AND contype='f' AND convalidated").fetchone()[0]
        def unvalidated():
            for table,column,name,definition in original:
                c.execute(sql.SQL('ALTER TABLE {} DROP CONSTRAINT {}').format(sql.Identifier('public',table),sql.Identifier(name)))
                c.execute(sql.SQL('ALTER TABLE {} ADD CONSTRAINT {} '+definition+' NOT VALID').format(sql.Identifier('public',table),sql.Identifier(name)))
            assert flags()==0
        def error_state(query):
            try:
                with c.transaction(force_rollback=True):c.execute(query)
            except psycopg.Error as error:return error.sqlstate
            raise AssertionError('Expected failure')
        with c.transaction(force_rollback=True):
            c.execute('CREATE SCHEMA fk_lab; CREATE EXTENSION '+info['extension']+' WITH SCHEMA fk_lab')
            assert c.execute('SELECT fk_lab.fk_identity()').fetchone()[0]==info['source_sha256']
            with c.transaction(force_rollback=True):
                unvalidated();assert c.execute(call).fetchone()[0] is not None and flags()==6
                for table,column in FKS:
                    query=sql.SQL('UPDATE {} SET {}=-1 WHERE ctid=(SELECT ctid FROM {} LIMIT 1)').format(sql.Identifier('public',table),sql.Identifier(column),sql.Identifier('public',table))
                    assert error_state(query)=='23503';checks.append({'case':'later_native_RI_rejects_orphan','table':table,'column':column})
            with c.transaction(force_rollback=True):
                unvalidated()
                c.execute('ALTER TABLE public.table1 DROP CONSTRAINT table1_id_column_3_fkey')
                # Delete an actual interior parent key: range membership alone
                # would incorrectly accept the child reference in this test.
                key=c.execute('SELECT raw_id FROM public.raw_data ORDER BY raw_id OFFSET 10 LIMIT 1').fetchone()[0]
                c.execute('DELETE FROM public.raw_data WHERE raw_id=%s',(key,))
                definition=original[0][3]
                c.execute('ALTER TABLE public.table1 ADD CONSTRAINT table1_id_column_3_fkey '+definition+' NOT VALID')
                assert error_state(call)=='23503' and flags()==0
                checks.append({'case':'interior_missing_actual_parent_rejected_no_flags_completed'})
            with c.transaction(force_rollback=True):
                unvalidated();c.execute('ALTER TABLE public.table1 DROP CONSTRAINT table1_id_column_3_fkey')
                c.execute('UPDATE public.raw_data SET raw_id=9223372036854775807 WHERE raw_id=(SELECT min(raw_id) FROM public.raw_data)')
                c.execute('ALTER TABLE public.table1 ADD CONSTRAINT table1_id_column_3_fkey '+original[0][3]+' NOT VALID')
                assert c.execute(call).fetchone()[0] is None and flags()==0
                assert error_state('ALTER TABLE public.table1 VALIDATE CONSTRAINT table1_id_column_3_fkey')=='23503'
                checks.append({'case':'wide_range_fallback_preserves_invalid_FK_for_native_validation'})
            for table in ('raw_data','table1','table3','table4'):
                with c.transaction(force_rollback=True):
                    unvalidated();c.execute('ALTER TABLE public.'+table+' ENABLE ROW LEVEL SECURITY')
                    assert c.execute(call).fetchone()[0] is None and flags()==0
                    checks.append({'case':'RLS_fallback_no_catalog_completion','table':table})
            with c.transaction(force_rollback=True):
                unvalidated()
                # Explicit nullable-column fixture: official table1 raw link
                # is NOT NULL, whereas MATCH SIMPLE must still be correct if
                # the standalone validator is called on nullable FK columns.
                for table,column in FKS:c.execute(sql.SQL('ALTER TABLE {} ALTER COLUMN {} DROP NOT NULL').format(sql.Identifier('public',table),sql.Identifier(column)))
                c.execute('UPDATE public.table1 SET id_column_3=NULL,id_column_4=NULL')
                c.execute('UPDATE public.table2 SET id_column_2=NULL');c.execute('UPDATE public.table5 SET id_column_2=NULL')
                c.execute('UPDATE public.table6 SET id_column_2=NULL,id_column_3=NULL')
                assert c.execute(call).fetchone()[0]==[0]*6 and flags()==6
                checks.append({'case':'MATCH_SIMPLE_NULL_references'})
            with c.transaction(force_rollback=True):
                unvalidated();c.execute('TRUNCATE '+','.join('public.'+t for t in TABLES)+' CONTINUE IDENTITY RESTRICT')
                assert c.execute(call).fetchone()[0]==[0]*6 and flags()==6
                checks.append({'case':'empty_actual_parent_and_child_sets'})
            for parent,key in [('raw_data','raw_id'),('table1','id_column_1'),('table3','id_column_1'),('table4','id_column_1')]:
                results=[]
                for mode in ('native','shared'):
                    with c.transaction(force_rollback=True):
                        unvalidated()
                        if mode=='shared':assert c.execute(call).fetchone()[0] is not None
                        else:
                            for table,column,name,definition in original:c.execute(sql.SQL('ALTER TABLE {} VALIDATE CONSTRAINT {}').format(sql.Identifier('public',table),sql.Identifier(name)))
                        c.execute(sql.SQL('UPDATE {} SET {}=-7 WHERE {}=(SELECT min({}) FROM {})').format(sql.Identifier('public',parent),sql.Identifier(key),sql.Identifier(key),sql.Identifier(key),sql.Identifier('public',parent)))
                        c.execute(sql.SQL('DELETE FROM {} WHERE {}=-7').format(sql.Identifier('public',parent),sql.Identifier(key)))
                        results.append([c.execute('SELECT to_jsonb(t)::text FROM public.'+t+' t ORDER BY 1').fetchall() for t in TABLES])
                assert results[0]==results[1];checks.append({'case':'native_update_delete_CASCADE_exact','parent':parent})
            with c.transaction(force_rollback=True):
                unvalidated()
                with c.transaction(force_rollback=True):assert c.execute(call).fetchone()[0] is not None and flags()==6
                assert flags()==0;checks.append({'case':'completion_subtransaction_rollback_restores_all_six_flags'})
            with c.transaction(force_rollback=True):
                unvalidated();c.execute('SET LOCAL transaction_read_only=on')
                assert error_state(call)=='25006'
                checks.append({'case':'read_only_transaction_rejects_completion'})
        assert protected_schema_signature(c)==before and sequence_signature(c)==sequences
    report['passed']=True;report['all_mutations_rolled_back']=True;write_report(output,report);print(len(checks),'FK completion boundary cases PASS',flush=True)


if __name__=='__main__':main()
