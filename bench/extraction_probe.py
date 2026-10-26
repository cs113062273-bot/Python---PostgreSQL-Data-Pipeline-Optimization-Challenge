"""Unranked, rollback-only comparison of native text extraction primitives."""
import argparse
import time
import psutil
from bench.common import connect, require_lab, environment, write_report
from pipeline.normalization import prepare_sql
from pipeline.recipes import EXTRACT, destinations


def json_table_sql(scopes):
    from pipeline.normalization import source_sql
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    columns=','.join(f"{key} text FORMAT JSON PATH 'strict $.{key}' OMIT QUOTES NULL ON EMPTY NULL ON ERROR" for key in fields)
    # The outer lateral join retains source rows whose values member is SQL NULL.
    return "CREATE TEMP TABLE pipeline_normalized_input ON COMMIT DROP AS SELECT s.raw_id,j.* FROM "+source_sql(scopes)+" s LEFT JOIN LATERAL JSON_TABLE(s.raw->'values','strict $' COLUMNS("+columns+")) j ON true;"


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',default='lab_generated_100k')
    parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--output',default='reports/extraction-probe.json')
    args=parser.parse_args()
    report={'kind':'rollback_only_component_experiment','official_score':False,'runs':[]}
    with connect(args.database,autocommit=True) as conn:
        require_lab(conn)
        psutil.Process(conn.info.backend_pid).cpu_affinity([2])
        scopes=[r[0] for r in conn.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        report['environment']=environment(conn)
        # Probe every JSON kind and null/missing keys before any speed claim.
        probes=['{}','null','[]','[1,2]','1','true','"x"',
                '{"x":null}','{"x":{}}','{"x":[1,2]}','{"x":"a\\nb"}',
                '{"x":0.0100}','{"x":true}','{"x":[{"a":1}]}']
        report['shape_probes']=[]
        for value in probes:
            row=conn.execute("SELECT p->>'x',j.x FROM (SELECT %s::jsonb p) s LEFT JOIN LATERAL JSON_TABLE(p,'strict $' COLUMNS(x text FORMAT JSON PATH 'strict $.x' OMIT QUOTES NULL ON EMPTY NULL ON ERROR)) j ON true",(value,)).fetchone()
            report['shape_probes'].append({'json':value,'operator':row[0],'json_table':row[1]})
        report['shape_equivalent']=all(r['operator']==r['json_table'] for r in report['shape_probes'])
        for repeat in range(args.repeats):
            modes=['operators','json_table'] if repeat%2==0 else ['json_table','operators']
            for mode in modes:
                with conn.transaction(force_rollback=True):
                    body=prepare_sql(scopes,all_tables=True,defer_index=True) if mode=='operators' else json_table_sql(scopes)
                    started=time.perf_counter()
                    conn.execute(body)
                    elapsed=time.perf_counter()-started
                    fingerprint=conn.execute("SELECT count(*),sum(hash_record_extended(t,17)::numeric)::text FROM pg_temp.pipeline_normalized_input t").fetchone()
                    row={'repeat':repeat,'mode':mode,'seconds':elapsed,'fingerprint':fingerprint}
                    report['runs'].append(row)
                    print(row,flush=True)
                write_report(args.output,report)
        report['generated_data_equivalent']=len({tuple(r['fingerprint']) for r in report['runs']})==1
        write_report(args.output,report)


if __name__=='__main__':main()
