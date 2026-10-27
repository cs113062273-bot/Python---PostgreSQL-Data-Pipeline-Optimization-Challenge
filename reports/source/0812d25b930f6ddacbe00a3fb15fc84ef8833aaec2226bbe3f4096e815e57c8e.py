"""Build the exact six-table oracle in its own disposable database."""
import argparse
from bench.common import connect,require_lab,write_report,environment
from bench.run import execute
from tests.equivalence.oracle import snapshot,fingerprints,assert_relationships


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database",required=True)
    parser.add_argument("--output",default="reports/reference-setup.json")
    args=parser.parse_args()
    with connect(args.database,autocommit=True) as conn:
        require_lab(conn)
        scopes=[r[0] for r in conn.execute("SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1")]
        env=environment(conn)
        result=execute(conn,"baseline",scopes,reset_destinations=False)
        assert_relationships(conn);snapshot(conn)
        write_report(args.output,{"kind":"independent_oracle_setup","official_score":False,"environment":env,
            "database":args.database,"source_rows":conn.execute("SELECT count(*) FROM public.raw_data").fetchone()[0],
            "fingerprints":fingerprints(conn),"baseline_sql_run":result})
    print("Independent original SQL oracle created",flush=True)


if __name__=="__main__":main()
