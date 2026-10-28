"""Dump stream verifier must preserve escaped fields and detect wrong data."""
import json
import os
from pathlib import Path
import subprocess
import uuid
from psycopg.types.json import Jsonb
from bench.common import ROOT,connect,write_report
from bench.fixture import create_fixture
from bench.run import execute
from bench.archive_native_clone import snapshot,digest
from bench.archive_stream_verified import verify_data


def main():
    db='lab_archive_stream_'+uuid.uuid4().hex[:8];create_fixture(db)
    directory=ROOT/'.runtime/native_archives';directory.mkdir(exist_ok=True)
    archive=directory/(db+'-verifier-control.dump')
    executable=ROOT/'.runtime/postgresql/pgsql/bin'
    special='native\n\r\t\b\f\v\\.\\N中文\x1a'
    with connect(db,autocommit=True) as c:
        execute(c,'baseline',['group001'])
        c.execute('UPDATE public.table3 SET text_column_1=%s WHERE id_column_1=(SELECT min(id_column_1) FROM public.table3)',(special,))
        c.execute("UPDATE public.raw_data SET raw=jsonb_set(raw,'{values,text_column_1}',%s) WHERE raw_id=1",(Jsonb(special),))
        c.execute("SET TimeZone='UTC'; SET DateStyle='ISO, MDY'; SET IntervalStyle='postgres'; SET extra_float_digits=3; SET bytea_output='hex'")
        before=snapshot(db)
        subprocess.run([str(executable/'pg_dump.exe'),'-Fc','-f',str(archive),db],env={**os.environ,'PGOPTIONS':'-c timezone=UTC','PGCLIENTENCODING':'UTF8'},check=True)
        tables=before['tables_owners_acl_persistence']
        accepted=verify_data(c,archive,executable/'pg_restore.exe',tables)
        assert snapshot(db)==before
        with c.transaction(force_rollback=True):
            c.execute('UPDATE public.table3 SET text_column_1=%s WHERE id_column_1=(SELECT min(id_column_1) FROM public.table3)',('deliberately edited archive source',))
            try:verify_data(c,archive,executable/'pg_restore.exe',tables)
            except AssertionError as error:assert str(error)=='public.table3',str(error)
            else:raise AssertionError('Archive verifier accepted a same-count edited field')
        assert snapshot(db)==before
        write_report('reports/archive-stream-verifier-controls.json',{'kind':'native_dump_copy_stream_verifier_controls','database':db,'all_passed':True,'escaped_unicode_control_bytes_preserved':True,'same_count_field_change_rejected':True,'fault_edit_rolled_back_and_sequences_unchanged':True,'all_table_copy_text':accepted,'dump_sha256':digest(archive),'verifier_source_sha256':__import__('hashlib').sha256(Path(ROOT/'bench/archive_stream_verified.py').read_text(encoding='utf8').encode()).hexdigest(),'scope':'64-row native fixture plus actual official SQL outputs. No pipeline performance score. Data stream verification is not a full populated restore claim.'})
    print('Archive stream controls: complete native rows/escapes/Unicode/control bytes and negative edit rejection PASS',flush=True)


if __name__=='__main__':main()
