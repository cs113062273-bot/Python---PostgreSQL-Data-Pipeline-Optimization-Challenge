"""Detect content edits preserving stored hashes, schema edits and receipt edits."""
import argparse
import uuid
from bench.common import connect,write_report
from pipeline.checkpoints import ChunkRunner
from tests.recovery.run import Service,fixture


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',default='reports/recovery-tamper.json')
    args=parser.parse_args()
    records=[]
    for kind in ("source_same_raw_hash","destination_same_count","schema_rename","version_dependency","row_counter"):
        database=fixture(); job=str(uuid.uuid4()); service=Service(); runner=ChunkRunner(service,connect)
        runner.run(database,["group001"],job,chunk_rows=16)
        if kind!="source_same_raw_hash":
            service.finalize_checkpoint_job(runner,database,job)
        with connect(database,autocommit=True) as conn:
            if kind=="source_same_raw_hash":
                # Privileged bypass simulates an external edit or damaged restore.
                # The stored raw_hash is deliberately left untouched.
                conn.execute("ALTER TABLE public.raw_data DISABLE TRIGGER pipeline_write_fence")
                conn.execute("UPDATE public.raw_data SET raw=jsonb_set(raw,'{values,text_column_1}',to_jsonb('edited'::text)) WHERE raw_id=1")
                conn.execute("ALTER TABLE public.raw_data ENABLE TRIGGER pipeline_write_fence")
            elif kind=="destination_same_count":
                conn.execute("UPDATE public.table2 SET integer_column_2=integer_column_2+1 WHERE id_column_1=(SELECT min(id_column_1) FROM public.table2)")
            elif kind=="schema_rename":
                conn.execute("ALTER TABLE public.table1 RENAME COLUMN text_column_1 TO edited_column")
            elif kind=="row_counter":
                conn.execute("UPDATE public.pgdm_table_row_counts SET row_count=row_count+1 WHERE table_name='table2'")
        if kind=="version_dependency":
            with connect("postgres_data_manager",autocommit=True) as control:
                control.execute("UPDATE app_control.table_version_dependencies SET dependency_payload='{}'::jsonb WHERE source_database_name=%s",(database,))
        try:
            if kind=="version_dependency":
                service.finalize_checkpoint_job(runner,database,job)
            else:
                runner.run(database,["group001"],job,chunk_rows=16)
        except RuntimeError as error:
            records.append({"kind":kind,"database":database,"passed":True,"rejection":str(error)})
            print(kind,"PASS",flush=True)
        else:
            raise AssertionError("Tampering accepted: "+kind)
    write_report(args.output,{"tests":records})


if __name__=="__main__":main()
