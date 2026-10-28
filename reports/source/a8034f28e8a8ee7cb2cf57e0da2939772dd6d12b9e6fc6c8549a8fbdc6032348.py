"""A stale repeatable-read snapshot must not hide a newly committed orphan.

The service already locks the complete graph before its first snapshot. This
test additionally protects standalone extension use by matching PostgreSQL's
RI_Initial_Check current-snapshot behavior.
"""
from pathlib import Path
import uuid
import psycopg
from bench import native_c_lab as lab,native_fk_stream_probe as old,native_fk_current_probe as current
from bench.common import ROOT,write_report
from bench.native_fk_scan_component import TABLES
from bench.run import execute
from tests.equivalence.oracle import protected_schema_signature,sequence_signature


def main():
    output=ROOT/'reports/native-fk-current-snapshot-concurrency.json';assert not output.exists()
    db='lab_fk_snapshot_'+uuid.uuid4().hex[:8];lab.fixture(db)
    args=','.join("'public."+t+"'::regclass" for t in TABLES)
    results={}
    with lab.connect(db,autocommit=True) as setup:
        execute(setup,'baseline',['group001']);before=protected_schema_signature(setup);seq=sequence_signature(setup)
        parent,raw=setup.execute('SELECT id_column_1,id_column_3 FROM public.table1 ORDER BY id_column_1 LIMIT 1').fetchone()
        definition=setup.execute("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='public.table1'::regclass AND conname='table1_id_column_3_fkey'").fetchone()[0]
        setup.execute('ALTER TABLE public.table1 DROP CONSTRAINT table1_id_column_3_fkey')
        for schema,module in [('fk_old',old),('fk_current',current)]:setup.execute('CREATE SCHEMA '+schema+'; CREATE EXTENSION '+module.package()['extension']+' WITH SCHEMA '+schema)
        for mode in ('old_snapshot_negative_control','native','current_snapshot'):
            setup.execute('UPDATE public.table1 SET id_column_3=%s WHERE id_column_1=%s',(raw,parent))
            with lab.connect(db,autocommit=True) as a,lab.connect(db,autocommit=True) as writer:
                try:
                    with a.transaction(force_rollback=True):
                        a.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                        assert a.execute('SELECT id_column_3 FROM public.table1 WHERE id_column_1=%s',(parent,)).fetchone()==(raw,)
                        writer.execute('UPDATE public.table1 SET id_column_3=-1 WHERE id_column_1=%s',(parent,))
                        a.execute('ALTER TABLE public.table1 ADD CONSTRAINT table1_id_column_3_fkey '+definition+' NOT VALID')
                        if mode=='native':a.execute('ALTER TABLE public.table1 VALIDATE CONSTRAINT table1_id_column_3_fkey')
                        else:
                            schema='fk_old' if mode=='old_snapshot_negative_control' else 'fk_current'
                            assert a.execute('SELECT '+schema+'.fk_validate('+args+')').fetchone()[0] is not None
                        results[mode]={'sqlstate':'00000','orphan_accepted':True}
                except psycopg.Error as error:
                    results[mode]={'sqlstate':error.sqlstate,'orphan_accepted':False,'primary':error.diag.message_primary,'detail':error.diag.message_detail}
            print(mode,results[mode],flush=True)
        assert results['old_snapshot_negative_control']['orphan_accepted'] is True
        assert results['native']==results['current_snapshot'] and results['native']['sqlstate']=='23503'
        setup.execute('UPDATE public.table1 SET id_column_3=%s WHERE id_column_1=%s',(raw,parent))
        setup.execute('ALTER TABLE public.table1 ADD CONSTRAINT table1_id_column_3_fkey '+definition)
        for schema,module in [('fk_old',old),('fk_current',current)]:setup.execute('DROP EXTENSION '+module.package()['extension']+'; DROP SCHEMA '+schema)
        assert protected_schema_signature(setup)==before and sequence_signature(setup)==seq
    write_report(output,{'kind':'foreign_key_stale_RR_snapshot_concurrency_regression','passed':True,'database':db,'results':results,'current_source_sha256':current.package()['source_sha256'],'native_source_reference':'https://github.com/postgres/postgres/blob/REL_17_11/src/backend/utils/adt/ri_triggers.c#L1465','service_stable_lock_before_snapshot_contract_retained':True,'all_test_mutations_restored':True})
    print('Old-snapshot negative control reproduced; current snapshot matches native 23503 PASS',flush=True)


if __name__=='__main__':main()
