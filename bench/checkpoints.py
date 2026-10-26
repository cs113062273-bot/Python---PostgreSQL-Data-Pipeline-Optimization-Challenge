"""Measure the full LOCAL chunk prototype, including integrity and version costs."""
import argparse
from datetime import datetime,timezone
import time
import uuid
from bench.common import connect,environment,require_lab,write_report
from bench.control import seed_initial_versions
from bench.run import execute
from pipeline.checkpoints import ChunkRunner
from pipeline.local_service import LocalService
from pipeline.version_receipts import VersionReceiptMixin
from tests.equivalence.oracle import snapshot,assert_exact


class Service(VersionReceiptMixin,LocalService):pass


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database",required=True)
    parser.add_argument("--job",default=None,help="Repeat this UUID and identical arguments to resume")
    parser.add_argument("--chunk-rows",type=int,default=100000)
    parser.add_argument("--mode",choices=["baseline","path_once"],default="baseline")
    parser.add_argument("--output",default="reports/chunk-benchmark.json")
    args=parser.parse_args();job=args.job or str(uuid.uuid4())
    with connect(args.database,autocommit=True) as conn:
        require_lab(conn);env=environment(conn)
        scopes=[r[0] for r in conn.execute("SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1")]
        if not conn.execute("SELECT to_regclass('pipeline_recovery.jobs')").fetchone()[0]:
            execute(conn,"baseline",scopes);snapshot(conn)
            conn.execute("TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY")
        before=conn.execute("SELECT pg_database_size(current_database()),pg_current_wal_insert_lsn()").fetchone()
    seed_initial_versions(args.database)
    write_report(args.output,{"job":job,"state":"started","database":args.database,"chunk_rows":args.chunk_rows,"mode":args.mode})
    print("Durable job",job,flush=True)
    service=Service();runner=ChunkRunner(service,connect)
    started=time.perf_counter()
    runner.run(args.database,scopes,job,args.chunk_rows,args.mode)
    versions=service.finalize_checkpoint_job(runner,args.database,job)
    seconds=time.perf_counter()-started
    with connect(args.database,autocommit=True) as conn:
        assert_exact(conn)
        chunks=conn.execute("SELECT lo,hi,rows_inserted FROM pipeline_recovery.chunks WHERE job_id=%s ORDER BY lo",(job,)).fetchall()
        size,wal=conn.execute("SELECT pg_database_size(current_database()),pg_wal_lsn_diff(pg_current_wal_insert_lsn(),%s)::bigint",(before[1],)).fetchone()
    report={"kind":"local_chunk_end_to_end_wall","official_score":False,"job":job,"database":args.database,
            "timestamp_utc":datetime.now(timezone.utc).isoformat(),"environment":env,"chunk_rows":args.chunk_rows,"mode":args.mode,
            "seconds":seconds,"includes":["preflight","two full actual-content source fingerprints","checkpoint commits","output fingerprints","version saga","final integrity verification"],
            "excludes":["diagnostic reference construction","post-run EXCEPT ALL oracle","GUI refresh","SSH"],
            "chunks":chunks,"versions":versions,"exact_six_tables":True,"database_growth_bytes":size-before[0],
            "wal_cluster_delta_bytes":wal,"peak_ram_bytes":None,"peak_intermediate_bytes":None}
    write_report(args.output,report)
    print(f"{seconds:.3f}s including integrity/checkpoint/version costs; six outputs exact",flush=True)


if __name__=="__main__":main()
