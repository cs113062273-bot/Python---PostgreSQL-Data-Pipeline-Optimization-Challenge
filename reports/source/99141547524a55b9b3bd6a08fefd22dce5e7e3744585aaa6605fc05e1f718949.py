"""Actual pg_dump/pg_restore with same-name identity and intact job/version state."""
import argparse
from pathlib import Path
import subprocess
import uuid
from psycopg import sql
from bench.common import ROOT,connect,write_report
from pipeline.checkpoints import ChunkRunner
from tests.recovery.run import Service,fixture,InjectedFailure,check


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pg-dump",required=True)
    parser.add_argument("--pg-restore",required=True)
    args=parser.parse_args()
    database=fixture();job=str(uuid.uuid4());hit=[]
    def fail(event,detail):
        if event=="after_chunk_commit" and not hit:
            hit.append(True);raise InjectedFailure(event)
    try:ChunkRunner(Service(),connect,fail).run(database,["group001"],job,chunk_rows=16)
    except InjectedFailure:pass
    archive=ROOT/".runtime"/(database+".dump")
    subprocess.run([args.pg_dump,"-Fc","-f",str(archive),database],check=True)
    # Preserve the old DB under a new name, restoring into the exact original
    # name to retain the signature/control-lineage identity. Nothing is dropped.
    with connect("postgres",autocommit=True) as admin:
        admin.execute(sql.SQL("ALTER DATABASE {} RENAME TO {}").format(sql.Identifier(database),sql.Identifier(database+"_before_restore")))
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    subprocess.run([args.pg_restore,"--exit-on-error","-d",database,str(archive)],check=True)
    service=Service();runner=ChunkRunner(service,connect)
    runner.run(database,["group001"],job,chunk_rows=16)
    versions=service.finalize_checkpoint_job(runner,database,job)
    assert service.finalize_checkpoint_job(runner,database,job)==versions
    check(database,job)
    write_report("reports/recovery-dump.json",{"kind":"actual_custom_format_dump_restore_of_partial_job","passed":True,
        "database":database,"job":job,"preserved_original":database+"_before_restore","dump_bytes":archive.stat().st_size,
        "exact_six_tables":True,"versions":versions,"limitations":["same PostgreSQL version, role and database name","control database retained","GUI raw-snapshot filesystem replay not tested"]})
    print("Actual dump/restore of partial job: PASS",flush=True)


if __name__=="__main__":main()
