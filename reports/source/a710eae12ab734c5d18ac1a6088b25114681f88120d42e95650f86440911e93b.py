"""Exercise index fast-path fallbacks and fail-closed schema checks."""
import uuid
import psycopg
from bench.common import connect,write_report
from bench.fixture import create_fixture
from pipeline.bulk_fks import prepare_sql,restore_sql
from pipeline.bulk_indexes import prepare_mode_sql,restore_sql as restore_indexes
from tests.equivalence.oracle import schema_signature


def main():
    database="lab_bulk_guards_"+uuid.uuid4().hex[:8]
    create_fixture(database)
    results=[]
    cases=[
        ("cyclic identity retains PK","ALTER SEQUENCE public.table1_id_column_1_seq CYCLE","bulk_full","table1","table1_pkey"),
        ("custom index storage retained","ALTER INDEX public.table2_id_column_2_integer_column_1_key SET(fillfactor=80)","bulk_indexes","table2","table2_id_column_2_integer_column_1_key"),
        ("bounded dimension strings retain key","ALTER TABLE public.table3 ALTER COLUMN text_column_1 TYPE varchar(100)","bulk_indexes","table1","table1_id_column_3_key"),
        ("external FK retains referenced unique","CREATE TABLE public.extra_child(parent bigint REFERENCES public.table1(id_column_3))","bulk_indexes","table1","table1_id_column_3_key"),
        ("external FK retains referenced PK","CREATE TABLE public.extra_child(parent bigint REFERENCES public.table1(id_column_1))","bulk_full","table1","table1_pkey"),
    ]
    with connect(database,autocommit=True) as conn:
        for name,mutation,mode,table,key in cases:
            with conn.transaction(force_rollback=True):
                conn.execute(mutation)
                before=schema_signature(conn)
                path=conn.execute("SHOW search_path").fetchone()[0]
                conn.execute(prepare_sql())
                conn.execute(prepare_mode_sql(mode,table))
                assert conn.execute("SELECT to_regclass(%s)",("public."+key,)).fetchone()[0],name
                assert conn.execute("SHOW search_path").fetchone()[0]==path,name
                conn.execute(restore_indexes(table));conn.execute(restore_sql())
                assert schema_signature(conn)==before,name
            results.append({"case":name,"passed":True});print(name,": PASS",flush=True)
        for name,mutation in [
            ("unknown CHECK refused","ALTER TABLE public.table1 ADD CONSTRAINT unknown_check CHECK(id_column_3>0)"),
            ("source user trigger refused","""CREATE FUNCTION challenge_lab.stub() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;
               CREATE TRIGGER unknown_trigger BEFORE INSERT ON public.raw_data FOR EACH ROW EXECUTE FUNCTION challenge_lab.stub()"""),
        ]:
            with conn.transaction(force_rollback=True):
                conn.execute(mutation);before=schema_signature(conn)
                try:
                    with conn.transaction():conn.execute(prepare_sql())
                except psycopg.Error as error:
                    assert error.sqlstate=='P0001'
                else:raise AssertionError(name)
                assert schema_signature(conn)==before,name
            results.append({"case":name,"passed":True});print(name,": PASS",flush=True)
    write_report("reports/bulk-guard-fallbacks.json",{"kind":"bulk_index_metadata_fallbacks","database":database,"tests":results,"all_passed":True})


if __name__=="__main__":main()
