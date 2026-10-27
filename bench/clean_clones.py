"""Reclaim explicitly receipted SQL-only clones; never native-versioned DBs."""
import argparse
from datetime import datetime,timezone
import json
import os
import re
from pathlib import Path
from psycopg import sql
from bench.common import ROOT,connect,require_lab,write_report


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--reports',nargs='+',required=True)
    p.add_argument('--modes',nargs='+',default=['baseline'])
    p.add_argument('--databases',nargs='+',help='Further restrict cleanup to these explicitly receipted databases')
    p.add_argument('--apply',action='store_true')
    args=p.parse_args()
    candidates={}
    for name in args.reports:
        path=(ROOT/name).resolve()
        if not path.is_relative_to(ROOT/'reports'):p.error('Use project receipts')
        r=json.loads(path.read_text())
        if r.get('kind')!='fresh_database_per_run_sql_diagnostic' or r.get('template')!='lab_template_1m':p.error('Use our fresh SQL-only 1M cohort receipts')
        for row in r['runs']:
            if row['mode'] not in args.modes:continue
            if not row.get('schema_equivalent') or not row.get('fingerprints_equivalent'):raise RuntimeError('Unverified run cannot authorize cleanup')
            db=row['database']
            if not re.fullmatch(r'lab_fresh_[0-9a-f]{10}',db):raise RuntimeError('Unexpected clone name')
            if args.databases and db not in args.databases:continue
            candidates[db]={'database':db,'mode':row['mode'],'receipt':name}
    if args.databases and set(args.databases)!=set(candidates):p.error('Every explicitly named database must have a verified selected-mode receipt')
    events=[]
    with connect('postgres',autocommit=True) as admin,connect(os.environ.get('PDM_CONTROL_DB','postgres_data_manager'),autocommit=True) as control:
        data=Path(admin.execute('SHOW data_directory').fetchone()[0]).resolve()
        if admin.info.host not in ('127.0.0.1','localhost','::1') or data!=(ROOT/'.runtime/pgdata').resolve():raise RuntimeError('Cleanup requires this exact private local cluster')
        for db,event in sorted(candidates.items()):
            if not admin.execute('SELECT 1 FROM pg_database WHERE datname=%s AND NOT datistemplate',(db,)).fetchone():continue
            if control.execute('SELECT count(*) FROM app_control.table_versions WHERE database_name=%s',(db,)).fetchone()[0]:raise RuntimeError('Native version history exists; retain '+db)
            with connect(db,autocommit=True) as c:
                require_lab(c)
                if c.execute("SELECT to_regclass('public.pgdm_table_row_counts')").fetchone()[0]:raise RuntimeError('Native counters exist; retain '+db)
                event['bytes']=c.execute('SELECT pg_database_size(current_database())').fetchone()[0]
            if admin.execute("SELECT count(*) FROM pg_stat_activity WHERE datname=%s AND backend_type='client backend'",(db,)).fetchone()[0]:raise RuntimeError('Clone has a live client; retain '+db)
            if args.apply:admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(db)))
            event['removed']=args.apply
            events.append(event);print(event,flush=True)
    print('Total GiB:',sum(e['bytes'] for e in events)/1024**3,flush=True)
    if args.apply:
        path=ROOT/'reports/disposable-clone-cleanup.json'
        report=json.loads(path.read_text()) if path.exists() else {'kind':'verified_sql_only_disposable_clone_cleanup','runs':[]}
        report['runs'].append({'timestamp_utc':datetime.now(timezone.utc).isoformat(),'events':events,'scope':'Only explicitly receipted SQL-only disposable clones, no native versions/counters; raw templates, canonical oracle, native successful outputs and metrics retained.'})
        write_report(path,report)


if __name__=='__main__':main()
