"""Fresh unranked scale profile, preserving plan files and actual stage size."""
import argparse
import shutil
import uuid
import psutil
from psycopg import sql
from bench.common import ROOT,connect,environment,write_report,require_lab
from bench.run import execute
from pipeline.guarded_normalization import MODES,prepare_sql


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--template',default='lab_template_1m')
    parser.add_argument('--mode',choices=MODES,default='guarded_prefix')
    parser.add_argument('--cpu-affinity',type=int,default=2)
    args=parser.parse_args()
    if not args.template.startswith('lab_'):parser.error('Use a preserved lab_ template')
    database='lab_profile_'+uuid.uuid4().hex[:10]
    with connect('postgres',autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {} STRATEGY FILE_COPY').format(sql.Identifier(database),sql.Identifier(args.template)))
    base=ROOT/'reports'/'plans'
    preserved=base/'previous-prefix-100k';preserved.mkdir(parents=True,exist_ok=True)
    for path in base.glob(args.mode+'_public.*.json'):
        target=preserved/path.name
        if not target.exists():shutil.copy2(path,target)
    with connect(database,autocommit=True) as conn:
        require_lab(conn)
        if conn.info.host not in ('localhost','127.0.0.1','::1'):raise RuntimeError('This optional CPU pin requires a local server')
        process=psutil.Process(conn.info.backend_pid)
        if 'postgres' not in process.name().lower():raise RuntimeError('Backend identity mismatch')
        process.cpu_affinity([args.cpu_affinity])
        scopes=[r[0] for r in conn.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        report={'kind':'instrumented_scale_profile_not_ranked','official_score':False,'database':database,'mode':args.mode,
                'source_rows':conn.execute('SELECT count(*) FROM public.raw_data').fetchone()[0],'environment':environment(conn)}
        # Standalone component: exact stage inputs and full preparation, rolled
        # back before the six-table profile; no persistent destination warmup.
        with conn.transaction(force_rollback=True):
            conn.execute(prepare_sql(scopes,args.mode))
            report['stage_storage_component']=dict(zip(('heap_bytes','total_relation_bytes'),conn.execute("SELECT pg_relation_size('pg_temp.pipeline_normalized_input'),pg_total_relation_size('pg_temp.pipeline_normalized_input')").fetchone()))
            report['stage_storage_component']['safe_domain']=conn.execute('SELECT safe FROM pg_temp.pipeline_normalization_domain').fetchone()[0]
        result=execute(conn,args.mode,scopes,profile=True,reset_destinations=False)
        result.pop('sql_seconds');result.pop('stages')
        report['instrumented_run']=result
    directory=base/(args.mode+'-'+str(report['source_rows']));directory.mkdir(parents=True,exist_ok=True)
    for path in base.glob(args.mode+'_public.*.json'):shutil.copy2(path,directory/path.name)
    report['plan_directory']=str(directory.relative_to(ROOT))
    report['scope']='Instrumentation is not ranked timing. Stage size measured as a separate component; backend RSS is not whole-server peak. Retained lab clone is intentional.'
    write_report(directory/'profile.json',report)
    print('Fresh scale plans and explicit temporary-stage size saved to',directory,flush=True)


if __name__=='__main__':main()
