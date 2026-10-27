"""Rollback-only local binary COPY vs INSERT, including export and cleanup.

Uses one exact ready projection from the retained profile clone. Server paths
are restricted to this project's newly created runtime directory. No PROGRAM,
FREEZE, durability changes, stored outputs or sequence rewinds are used.
"""
import argparse
import json
from pathlib import Path
import re
import time
import uuid
import psutil
from bench.common import connect,ROOT,write_report,environment
from bench.run import prepared
from pipeline.guarded_normalization import prepare_sql,fast_statement
from pipeline.parallel_normalization import materialize_statement,settings_sql,restore_settings_sql
from pipeline.bulk_fks import prepare_sql as prepare_fks,restore_sql as restore_fks
from pipeline.bulk_indexes import prepare_mode_sql,restore_sql
from pipeline.recipes import ORDER
from tests.equivalence.oracle import canonical


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--table',choices=['table1','table2','table5','table6'],default='table2')
    p.add_argument('--database',help='An explicitly retained lab_ctas_profile_ clone only')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--cache',type=int,default=1)
    p.add_argument('--direct',action='store_true',help='Also bypass the ready heap: native COPY of the exact transformed SELECT')
    p.add_argument('--output',default='reports/copy-component-100k.json')
    args=p.parse_args()
    if not 1<=args.cache<=10000:p.error('cache must be 1..10000')
    db=args.database or json.loads((ROOT/'reports/plans/parallel-ctas-100k.json').read_text())['database']
    if not db.startswith('lab_ctas_profile_'):p.error('Only our rollback-only profile clones are permitted')
    directory=(ROOT/'.runtime'/'copy_probe'/uuid.uuid4().hex).resolve()
    directory.mkdir(parents=True)
    report={'kind':'rollback_only_local_copy_component_not_ranked','database':db,'table':args.table,'runs':[]}
    def fingerprint(c):
        return c.execute('SELECT count(*),sum(hash_record_extended(ROW(value),17)::numeric)::text FROM ('+canonical(args.table)+') t').fetchone()
    with connect(db,autocommit=True) as c:
        if c.info.host not in ('localhost','127.0.0.1','::1'):raise RuntimeError('File cleanup requires our local server')
        psutil.Process(c.info.backend_pid).cpu_affinity([2])
        report['environment']=environment(c)
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data')]
        with c.transaction(force_rollback=True):
            c.execute(prepare_sql(scopes,'lab_parallel_bulk'))
            assert c.execute('SELECT safe FROM pg_temp.pipeline_normalization_domain').fetchone()[0]
            c.execute(prepare_fks())
            recipes=prepared('lab_parallel_bulk',scopes)
            for d in recipes:
                table=d['pure_table_name']
                if table==args.table:
                    c.execute(prepare_mode_sql('lab_parallel_bulk',table))
                    original=fast_statement(d['statements'][0],scopes,'lab_parallel_bulk')
                    original_header=re.fullmatch(r'INSERT INTO public\.'+table+r'\s*\((.*?)\)\s*(SELECT.*)',original,re.S)
                    assert original_header
                    direct_query=original_header[2].rstrip(';')
                    prep,insert,_=materialize_statement(original)
                    projection_start=time.perf_counter()
                    c.execute(prep)
                    report['ready_projection_wall_seconds']=time.perf_counter()-projection_start
                    header=re.fullmatch(r'INSERT INTO public\.'+table+r'\s*\((.*?)\)\s*(SELECT.*)',insert,re.S)
                    assert header
                    columns,query=header.groups()
                    sequence=c.execute('SELECT pg_get_serial_sequence(%s,%s)',('public.'+table,'id_column_1')).fetchone()[0]
                    cache_before=c.execute('SELECT seqcache FROM pg_sequence WHERE seqrelid=%s::regclass',(sequence,)).fetchone()[0]
                    for repeat in range(args.repeats):
                        modes=['insert','copy'] if args.cache==1 else ['insert','cached_insert','copy','cached_copy']
                        if args.direct:modes+=['direct_copy','cached_direct_copy'] if args.cache>1 else ['direct_copy']
                        if repeat%2:modes=list(reversed(modes))
                        for mode in modes:
                            path=directory/(uuid.uuid4().hex+'.bin')
                            try:
                                with c.transaction(force_rollback=True):
                                    start=time.perf_counter()
                                    from psycopg import sql
                                    cached=mode.startswith('cached_')
                                    is_copy=mode.endswith('copy')
                                    if cached:c.execute(sql.SQL('ALTER SEQUENCE {} CACHE '+str(args.cache)).format(sql.Identifier(*sequence.split('.'))))
                                    if not is_copy:
                                        count=c.execute(insert).rowcount
                                        export_seconds=0
                                    else:
                                        # psycopg renders a proper SQL literal, never shell text.
                                        direct=mode.endswith('direct_copy')
                                        if direct:c.execute(settings_sql())
                                        c.execute(sql.SQL('COPY ('+(direct_query if direct else query)+') TO {} (FORMAT BINARY)').format(sql.Literal(path.as_posix())))
                                        if direct:c.execute(restore_settings_sql())
                                        export_seconds=time.perf_counter()-start
                                        count=c.execute(sql.SQL('COPY public.'+table+' ('+columns+') FROM {} (FORMAT BINARY)').format(sql.Literal(path.as_posix()))).rowcount
                                    if cached:c.execute(sql.SQL('ALTER SEQUENCE {} CACHE '+str(cache_before)).format(sql.Identifier(*sequence.split('.'))))
                                    seconds=time.perf_counter()-start
                                    file_bytes=path.stat().st_size if is_copy else 0
                                    cleanup=time.perf_counter()
                                    if is_copy:path.unlink()
                                    cleanup_seconds=time.perf_counter()-cleanup
                                    row={'repeat':repeat,'mode':mode,'affected':count,'total_wall_seconds':seconds+cleanup_seconds,'projection_plus_load_wall_seconds':seconds+cleanup_seconds+(0 if mode.endswith('direct_copy') else report['ready_projection_wall_seconds']),'export_wall_seconds':export_seconds,'cleanup_wall_seconds':cleanup_seconds,'file_bytes':file_bytes,'fingerprint':fingerprint(c)}
                                    report['runs'].append(row)
                                    print({k:v for k,v in row.items() if k!='fingerprint'},flush=True)
                            finally:
                                if path.exists():path.unlink()
                    break
                c.execute(prepare_mode_sql('lab_parallel_bulk',table) if table in ('table1','table2','table5','table6') else 'SELECT 1')
                c.execute(fast_statement(d['statements'][0],scopes,'lab_parallel_bulk'))
                if table in ('table1','table2','table5','table6'):c.execute(restore_sql(table,mode='lab_parallel_bulk'))
                if table in ('table3','table4'):c.execute('ANALYZE public.'+table)
    directory.rmdir()
    report['fingerprints_equivalent']=len({tuple(r['fingerprint']) for r in report['runs']})==1
    assert report['fingerprints_equivalent']
    write_report(args.output,report)


if __name__=='__main__':main()
