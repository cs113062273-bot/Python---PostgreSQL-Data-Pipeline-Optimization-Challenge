"""Fresh database for EVERY run, preserving the organizer's empty-table stats.

Canonical output is copied only to a separate verification schema after timing.
The benchmark database never runs reference/warmup destination DML first.
"""
import argparse
from datetime import datetime,timezone
import json
import statistics
import uuid
import hashlib
import psutil
from psycopg import sql

from bench.common import connect,environment,require_lab,write_report
from bench.run import execute
from pipeline.recipes import MODES,ORDER,compiler_sha256,ROOT
from tests.equivalence.oracle import canonical,fingerprints,schema_signature,assert_relationships


def expected_fingerprints(conn):
    return {table:conn.execute(f"""SELECT count(*),coalesce(sum(hash_record_extended(ROW(value),17)::numeric),0)::text,
        coalesce(sum(hash_record_extended(ROW(value),97)::numeric),0)::text FROM challenge_reference.{table}""").fetchone() for table in ORDER}


def exact_to_reference(conn,reference):
    schema="verify_"+uuid.uuid4().hex[:10]
    reference.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    for table in ORDER:
        target=sql.Identifier(schema,table)
        reference.execute(sql.SQL("CREATE TABLE {}(value jsonb) WITH(autovacuum_enabled=false,toast.autovacuum_enabled=false)").format(target))
        with conn.cursor().copy("COPY ("+canonical(table)+") TO STDOUT (FORMAT BINARY)") as reader:
            with reference.cursor().copy(sql.SQL("COPY {} FROM STDIN (FORMAT BINARY)").format(target)) as writer:
                pending=bytearray()
                for block in reader:
                    pending.extend(block)
                    if len(pending)>=1024*1024:
                        writer.write(bytes(pending));pending.clear()
                if pending:writer.write(bytes(pending))
        with reference.transaction():
            reference.execute("SET LOCAL work_mem='256MB'")
            different=reference.execute(sql.SQL("""SELECT EXISTS(
              (SELECT value FROM {} EXCEPT ALL SELECT value FROM {}) UNION ALL
              (SELECT value FROM {} EXCEPT ALL SELECT value FROM {}))""").format(
                target,sql.Identifier("challenge_reference",table),sql.Identifier("challenge_reference",table),target)).fetchone()[0]
            if different:raise AssertionError("Fresh-run EXCEPT ALL mismatch: "+table)
    return schema


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--template",default="lab_generated_100k")
    parser.add_argument("--reference",default="lab_100k")
    parser.add_argument("--modes",nargs="+",choices=MODES,default=["baseline","qf_lazy","hash_array64","analyze_base"])
    parser.add_argument("--repeats",type=int,default=3)
    parser.add_argument("--output",default="reports/fresh-100k.json")
    parser.add_argument("--clone-strategy",choices=["WAL_LOG","FILE_COPY"],default="FILE_COPY")
    parser.add_argument("--cpu-affinity",type=int,help="Optional local-only measured-backend CPU; applies identically to all modes")
    parser.add_argument("--order",choices=['latin','alternating'],default='latin')
    args=parser.parse_args()
    if not args.template.startswith("lab_"):parser.error("Use a preserved lab_ source template")
    def implementation_identity():
        return {'compiler_sha256':compiler_sha256(),**{name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in ('bench/run.py','bench/fresh.py')}}
    implementation=implementation_identity()
    report={"kind":"fresh_database_per_run_sql_diagnostic","official_score":False,"template":args.template,
            "clone_strategy":args.clone_strategy,"measured_backend_cpu_affinity":args.cpu_affinity,"order":args.order,
            "reference":args.reference,"timestamp_utc":datetime.now(timezone.utc).isoformat(),"runs":[],
            "implementation_identity":implementation,
            "method":"new database clone for each run; no destination warmup/TRUNCATE; typed fingerprints each run; central bidirectional EXCEPT ALL once per mode after timing",
            "exclusions":["clone","oracle","full preflight/counters/versions/GUI/SSH","locks","commit latency (reported separately)"]}
    with connect(args.reference,autocommit=True) as reference:
        require_lab(reference)
        expected=expected_fingerprints(reference)
        checked=set()
        with connect(args.template,autocommit=True) as template:
            report["environment"]=environment(template)
            report["source_rows"]=template.execute("SELECT count(*) FROM public.raw_data").fetchone()[0]
        for repeat in range(args.repeats):
            shift=repeat%len(args.modes)
            modes=args.modes[shift:]+args.modes[:shift] if args.order=='latin' else args.modes if repeat%2==0 else list(reversed(args.modes))
            for mode in modes:
                if implementation_identity()!=implementation:raise RuntimeError('Implementation changed during a benchmark cohort; stop and rerun with fixed source')
                database="lab_fresh_"+uuid.uuid4().hex[:10]
                with connect("postgres",autocommit=True) as admin:
                    admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE {} STRATEGY "+args.clone_strategy).format(sql.Identifier(database),sql.Identifier(args.template)))
                with connect(database,autocommit=True) as conn:
                    if args.cpu_affinity is not None:
                        if conn.info.host not in ('127.0.0.1','localhost','::1'):raise RuntimeError('CPU affinity requires a local server')
                        backend=psutil.Process(conn.info.backend_pid)
                        if 'postgres' not in backend.name().lower():raise RuntimeError('Backend process does not match PostgreSQL')
                        backend.cpu_affinity([args.cpu_affinity])
                    scopes=[r[0] for r in conn.execute("SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1")]
                    before=schema_signature(conn)
                    stats=conn.execute("SELECT c.relname,c.reltuples,(SELECT count(*) FROM pg_stats s WHERE s.schemaname='public' AND s.tablename=c.relname) FROM pg_class c WHERE c.relnamespace='public'::regnamespace AND c.relname=ANY(%s) ORDER BY c.relname",(list(ORDER),)).fetchall()
                    if any(n!=-1 or count!=0 for _,n,count in stats):
                        raise RuntimeError("Template destinations are not in original unanalyzed empty state")
                    result=execute(conn,mode,scopes,reset_destinations=False)
                    if implementation_identity()!=implementation:raise RuntimeError('Implementation changed during timed execution; discard this cohort')
                    print(f"fresh {repeat+1}: {mode}: SQL stages {result['sql_seconds']:.4f}s; verification pending",flush=True)
                    assert schema_signature(conn)==before
                    assert_relationships(conn)
                    if fingerprints(conn)!=expected:raise AssertionError("Fresh output fingerprints differ: "+mode)
                    if mode not in checked:
                        result["verification_schema"]=exact_to_reference(conn,reference)
                        result["exact_except_all"]=True
                        checked.add(mode)
                    result.update(database=database,repeat=repeat,initial_statistics=stats,fingerprints_equivalent=True,schema_equivalent=True)
                    report["runs"].append(result)
                    write_report(args.output,report)
                    print(f"fresh {repeat+1}: {mode}: {result['sql_seconds']:.4f}s; six outputs equivalent",flush=True)
        baseline=statistics.median(r["sql_seconds"] for r in report["runs"] if r["mode"]=="baseline")
        report["summary"]={}
        for mode in args.modes:
            values=[r["sql_seconds"] for r in report["runs"] if r["mode"]==mode]
            median=statistics.median(values)
            report["summary"][mode]={"individual_seconds":values,"median":median,"min":min(values),"max":max(values),"reduction_percent":100*(1-median/baseline)}
        write_report(args.output,report)
        print(json.dumps(report["summary"],indent=2),flush=True)


if __name__=="__main__":main()
