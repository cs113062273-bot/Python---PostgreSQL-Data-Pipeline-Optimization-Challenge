"""Crash/restart a deliberately selected project-local cluster, then resume."""
import argparse
from pathlib import Path
import subprocess
import uuid
from bench.common import ROOT,connect,write_report
from pipeline.checkpoints import ChunkRunner
from tests.recovery.run import Service,fixture,InjectedFailure,check


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pg-ctl",required=True)
    parser.add_argument("--data-dir",required=True)
    parser.add_argument("--port",type=int,default=55432)
    args=parser.parse_args()
    directory=Path(args.data_dir).resolve()
    runtime=(ROOT/".runtime").resolve()
    if runtime not in directory.parents or not (directory/"PG_VERSION").exists():
        parser.error("Only a verified cluster under this project's .runtime may be restarted")
    database=fixture();job=str(uuid.uuid4());service=Service();hit=[]
    def fail(event,detail):
        if event=="after_chunk_commit" and not hit:
            hit.append(True);raise InjectedFailure(event)
    try:ChunkRunner(service,connect,fail).run(database,["group001"],job,chunk_rows=16)
    except InjectedFailure:pass
    with connect(database,autocommit=True) as conn:
        before=conn.execute("SELECT lo,hi,completed_at FROM pipeline_recovery.chunks WHERE job_id=%s ORDER BY lo",(job,)).fetchall()
        assert len(before)==1
    subprocess.run([args.pg_ctl,"-D",str(directory),"-m","immediate","-w","stop"],check=True)
    subprocess.run([args.pg_ctl,"-D",str(directory),"-l",str(runtime/"postgres.log"),"-o",f"-h 127.0.0.1 -p {args.port}","-w","start"],check=True)
    service=Service();runner=ChunkRunner(service,connect)
    runner.run(database,["group001"],job,chunk_rows=16)
    service.finalize_checkpoint_job(runner,database,job)
    after=check(database,job)
    assert after[:1]==before
    write_report("reports/recovery-restart.json",{"kind":"local_immediate_postgres_stop_and_wal_recovery","passed":True,
        "database":database,"job":job,"committed_chunks_reused":1,"exact_six_tables":True,"versions_idempotent":True,
        "not_equivalent_to":["Linux restart","power loss","storage failure"]})
    print("Immediate server stop/start: PASS, committed chunk reused",flush=True)


if __name__=="__main__":main()
