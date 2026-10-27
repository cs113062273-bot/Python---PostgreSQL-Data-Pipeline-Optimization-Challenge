"""Rollback-only worker degree measurements; never a complete-service score."""
import argparse
import hashlib
from pathlib import Path
import re
import statistics
import tempfile
import time
import uuid
from psycopg import sql
from bench.common import ROOT,connect,require_lab,environment,write_report
from bench.run import prepared
from pipeline.parallel_normalization import prepare_input_sql,settings_sql
from pipeline.guarded_normalization import fast_statement
from pipeline.workspace import RELATION
from pipeline.recipes import EXTRACT,destinations


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_native_variant_e939b248ec')
    p.add_argument('--repeats',type=int,default=2)
    p.add_argument('--output',default='reports/worker-degree-components-1m.json')
    args=p.parse_args()
    report={'kind':'rollback_only_worker_degree_components','official_score':False,'database':args.database,'runs':[],'scope':'Projection CTAS and transform-to-server-file separately. No COPY FROM, keys, FKs, preflight, versions or commit. Component selection only.'}
    parent=ROOT/'.runtime/worker_probe';parent.mkdir(exist_ok=True)
    directory=Path(tempfile.mkdtemp(prefix='degree_',dir=parent)).resolve();path=directory/'transform.bin'
    assert directory.parent==parent.resolve()
    try:
        with connect(args.database,autocommit=True) as c:
            require_lab(c);report['environment']=environment(c)
            scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
            fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])});record='ROW('+','.join(['raw_id']+fields+['pipeline_domain_safe'])+')'
            expected=None
            for repeat in range(args.repeats):
                modes=[8,4,2,0];shift=repeat%len(modes)
                for workers in modes[shift:]+modes[:shift]:
                    with c.transaction(force_rollback=True):
                        c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql(workers)+" SET LOCAL work_mem='64MB';")
                        schema='worker_degree_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                        query=prepare_input_sql(scopes,logged=True).replace('challenge_lab.pipeline_normalized_input',schema+'.input')
                        started=time.perf_counter();c.execute(query);seconds=time.perf_counter()-started
                        fp=c.execute(f'SELECT count(*),sum(hash_record_extended({record},17)::numeric)::text,sum(hash_record_extended({record},97)::numeric)::text FROM {schema}.input').fetchone()
                        if expected is None:expected=fp
                        assert fp==expected
                        report['runs'].append({'repeat':repeat,'component':'projection','workers':workers,'seconds':seconds,'fingerprint':fp})
                        print('projection',repeat,workers,round(seconds,4),flush=True)
            with c.transaction(force_rollback=True):
                schema='worker_degree_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                c.execute(settings_sql()+" SET LOCAL work_mem='64MB';")
                c.execute(prepare_input_sql(scopes,logged=True).replace('challenge_lab.pipeline_normalized_input',schema+'.input'))
                c.execute('ANALYZE '+schema+'.input(raw_id,text_column_1,text_column_2,text_column_3)')
                queries={}
                for d in prepared('guarded_copy',scopes):
                    table=d['pure_table_name']
                    if table not in ('table1','table2','table5','table6'):continue
                    statement=fast_statement(d['statements'][0],scopes,'guarded_copy').replace(RELATION,schema+'.input')
                    query=re.fullmatch(r'INSERT INTO public\.table[1-6]\s*\(.*?\)\s*(SELECT\s.*?);?',statement,re.S)[1].rstrip(';')
                    queries[table]=query
                for repeat in range(args.repeats):
                    for table,query in queries.items():
                        modes=[8,4,2,0];shift=repeat%len(modes)
                        for workers in modes[shift:]+modes[:shift]:
                            c.execute(settings_sql(workers))
                            c.execute('SET LOCAL enable_parallel_hash='+('off' if table=='table1' else 'on'))
                            started=time.perf_counter();cur=c.execute(sql.SQL('COPY ('+query+') TO {} (FORMAT BINARY)').format(sql.Literal(path.as_posix())));seconds=time.perf_counter()-started
                            report['runs'].append({'repeat':repeat,'component':table,'workers':workers,'seconds':seconds,'command_tag':cur.statusmessage,'binary_bytes':path.stat().st_size})
                            print(table,repeat,workers,round(seconds,4),flush=True)
                            path.unlink()
            report['all_scratch_transactionally_rolled_back']=True
            report['summary']={component:{str(workers):statistics.median(r['seconds'] for r in report['runs'] if r['component']==component and r['workers']==workers) for workers in (8,4,2,0)} for component in ('projection','table1','table2','table5','table6')}
            write_report(args.output,report)
    finally:
        path.unlink(missing_ok=True);directory.rmdir()


if __name__=='__main__':main()
