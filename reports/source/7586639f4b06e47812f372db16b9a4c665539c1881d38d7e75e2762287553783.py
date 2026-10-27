"""Exercise committed-prefix reuse, transaction rollback, fences and version saga."""
import argparse
from datetime import datetime,timezone
import json
import subprocess
import sys
import uuid

from bench.common import connect,write_report
from bench.control import seed_initial_versions
from bench.fixture import create_fixture
from bench.run import execute
from pipeline.checkpoints import ChunkRunner
from pipeline.local_service import LocalService
from pipeline.version_receipts import VersionReceiptMixin
from tests.equivalence.oracle import assert_exact,assert_relationships,snapshot


class Service(VersionReceiptMixin,LocalService):
    pass


class InjectedFailure(RuntimeError):
    pass


def fixture():
    database="lab_recovery_"+uuid.uuid4().hex[:10]
    create_fixture(database,64)
    seed_initial_versions(database)
    with connect(database,autocommit=True) as conn:
        execute(conn,"baseline",["group001"])
        snapshot(conn)
        conn.execute("TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY")
    return database


def check(database,job):
    with connect(database,autocommit=True) as conn:
        assert_exact(conn)
        assert_relationships(conn)
        chunks=conn.execute("SELECT lo,hi,completed_at FROM pipeline_recovery.chunks WHERE job_id=%s ORDER BY lo",(job,)).fetchall()
        assert [(r[0],r[1]) for r in chunks]==[(0,16),(16,32),(32,48),(48,64)]
        for table in ("table3","table4","table1","table5","table6","table2"):
            actual=conn.execute(f"SELECT count(*) FROM public.{table}").fetchone()[0]
            counter=conn.execute("SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s",(table,)).fetchone()[0]
            assert actual==counter,(table,actual,counter)
        assert conn.execute("SELECT count(*) FROM pipeline_recovery.active").fetchone()[0]==0
    return chunks


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output",default="reports/recovery.json")
    args=parser.parse_args()
    report={"kind":"isolated_functional_recovery","timestamp_utc":datetime.now(timezone.utc).isoformat(),"tests":[],
            "not_tested":["real SSH disconnect","Linux server restart","GUI process termination","power loss"]}
    events=["before_start","during_table3","during_table1","during_table2","before_chunk_commit",
            "after_chunk_commit","after_data_commit","during_versions","after_versions_commit","after_complete_commit","backend_termination","client_termination"]
    for target in events:
        database=fixture(); job=str(uuid.uuid4()); service=Service(); hit=[]
        def failpoint(event,detail):
            if event==target and not hit:
                hit.append(event)
                raise InjectedFailure(event)
            if target=="backend_termination" and event=="during_table2" and not hit:
                hit.append(target)
                with connect(database,autocommit=True) as killer:
                    assert killer.execute("SELECT pg_terminate_backend(%s)",(detail["backend_pid"],)).fetchone()[0]
        runner=ChunkRunner(service,connect,failpoint)
        if target=="during_versions":
            service._version_failpoint_sql="DO $fail$ BEGIN RAISE EXCEPTION 'injected version rollback'; END $fail$;"
        if target=="client_termination":
            process=subprocess.Popen([sys.executable,"-m","tests.recovery.worker",database,job,"after_chunk_commit"],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
            message=process.stdout.readline()
            assert json.loads(message)["ready"]
            process.kill(); process.communicate(timeout=20); hit.append(target)
        else:
            try:
                runner.run(database,["group001"],job,chunk_rows=16)
                service.finalize_checkpoint_job(runner,database,job)
            except Exception as error:
                if not hit and target!="during_versions":
                    raise
                if target=="during_versions":
                    assert "injected version rollback" in str(error)
                    hit.append(target)
            else:
                raise AssertionError("Failpoint did not interrupt: "+target)
        with connect(database,autocommit=True) as conn:
            exists=conn.execute("SELECT to_regclass('pipeline_recovery.chunks')").fetchone()[0]
            before=conn.execute("SELECT lo,hi,completed_at FROM pipeline_recovery.chunks WHERE job_id=%s ORDER BY lo",(job,)).fetchall() if exists else []
            active=conn.execute("SELECT count(*) FROM pipeline_recovery.active").fetchone()[0] if exists else 0
            fence_checks=[]
            if active:
                for statement in ["UPDATE public.raw_data SET raw=raw", "TRUNCATE public.raw_data CASCADE", "DELETE FROM public.table1"]:
                    try:
                        conn.execute(statement)
                    except Exception as error:
                        assert getattr(error,"sqlstate",None)=="55000"
                        fence_checks.append(True)
                    else:
                        raise AssertionError("Write fence failed")
        service=Service(); runner=ChunkRunner(service,connect)
        runner.run(database,["group001"],job,chunk_rows=16)
        versions=service.finalize_checkpoint_job(runner,database,job)
        assert len(versions)==6
        assert service.finalize_checkpoint_job(runner,database,job)==versions
        runner.run(database,["group001"],job,chunk_rows=16)
        chunks=check(database,job)
        assert chunks[:len(before)]==before,"A committed chunk was rerun"
        try:
            runner.run(database,["group001"],job,chunk_rows=17)
        except RuntimeError as error:
            assert "different request" in str(error)
        else:
            raise AssertionError("Changed request accepted")
        report["tests"].append({"event":target,"passed":True,"database":database,"job":job,
                                "committed_chunks_reused":len(before),"write_fence_checks":len(fence_checks),
                                "exact_six_tables":True,"row_counters":True,"versions":versions})
        write_report(args.output,report)
        print(target,"PASS; reused",len(before),"chunks",flush=True)
    print("All interruption and idempotency checks passed",flush=True)


if __name__=="__main__": main()
