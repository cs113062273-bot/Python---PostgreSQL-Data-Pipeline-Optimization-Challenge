"""Byte-exact native archive pipe, including Unicode and binary-looking text."""
import uuid
from psycopg.types.json import Jsonb
from bench.common import ROOT,connect,write_report
from bench.fixture import create_fixture
from bench.native_binary_transfer import transfer_raw,source_identity
from bench.retire_failed_initialization import raw_fingerprint
from tests.recovery.bulk_dump import sequence_state


def main():
    source_db='lab_pipe_source_'+uuid.uuid4().hex[:8];target_db='lab_pipe_target_'+uuid.uuid4().hex[:8]
    create_fixture(source_db);create_fixture(target_db)
    with connect(source_db,autocommit=True) as source,connect(target_db,autocommit=True) as target:
        source.execute("UPDATE public.raw_data SET raw=jsonb_set(raw,'{values,text_column_1}',%s) WHERE raw_id=1",(Jsonb('native binary\n\\.\x01\x02中文'),))
        target.execute('TRUNCATE public.raw_data CASCADE')
        before=raw_fingerprint(source);source_sequences=sequence_state(source);target_sequences=sequence_state(target)
        result=transfer_raw(ROOT/'.runtime/postgresql/pgsql/bin/psql.exe',source,target,timeout=30)
        assert raw_fingerprint(target)==before and raw_fingerprint(source)==before
        assert sequence_state(source)==source_sequences and sequence_state(target)==target_sequences
        assert target.execute('SELECT count(*) FROM public.raw_data').fetchone()[0]==64
        write_report('reports/native-archive-binary-pipe-64.json',{'kind':'private_archive_native_binary_pipe_functional_test','all_passed':True,'source_database':source_db,'target_database':target_db,'actual_native_raw_binary_sha256':before,'source_and_target_sequences_untouched':True,'source_identity':source_identity(),'transfer':result,'official_score':False})
    print('Native archive pipe: 64 exact raw rows/Unicode/control bytes/sequences PASS',flush=True)


if __name__=='__main__':main()
