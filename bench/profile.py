"""Store real EXPLAIN ANALYZE plans separately from benchmark timings."""
import argparse
from bench.common import connect,require_lab,environment,write_report
from bench.run import execute
from pipeline.recipes import MODES


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database",default="lab_100k")
    parser.add_argument("--modes",nargs="+",choices=MODES,default=["baseline","path_array","lazy_keys"])
    args=parser.parse_args()
    with connect(args.database,autocommit=True) as conn:
        require_lab(conn)
        scopes=[r[0] for r in conn.execute("SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1")]
        report={"kind":"instrumented_diagnostic_plans","official_score":False,"environment":environment(conn),"modes":args.modes,"runs":[]}
        for mode in args.modes:
            result=execute(conn,mode,scopes,profile=True)
            result.pop("sql_seconds")
            result.pop("stages")
            report["runs"].append(result)
            write_report("reports/plans/profile.json",report)
            print(mode,"plans saved; no benchmark total assigned",flush=True)


if __name__=="__main__":main()
