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
        if r.get('kind')!='fresh_database_per_run_sql_diagnostic' or r.get('template') not in ('lab_template_1m','lab_generated_100k'):p.error('Use our exact known-template SQL-only cohort receipts')
        for row in r['runs']:
            if row['mode'] not in args.modes:continue
            if not row.get('schema_equivalent') or not row.get('fingerprints_equivalent'):raise RuntimeError('Unverified run cannot authorize cleanup')
            db=row['database']
            if not re.fullmatch(r'lab_fresh_[0-9a-f]{10}',db):raise RuntimeError('Unexpected clone name')
            if args.databases and db not in args.databases:continue
            candidates[db]={'database':db,'mode':row['mode'],'receipt':name,'template':r['template']}
    if args.databases and set(args.databases)!=set(candidates):p.error('Every explicitly named database must have a verified selected-mode receipt')
    events=[]
    legacy_reference=None
    with connect('postgres',autocommit=True) as admin,connect(os.environ.get('PDM_CONTROL_DB','postgres_data_manager'),autocommit=True) as control:
        data=Path(admin.execute('SHOW data_directory').fetchone()[0]).resolve()
        if admin.info.host not in ('127.0.0.1','localhost','::1') or data!=(ROOT/'.runtime/pgdata').resolve():raise RuntimeError('Cleanup requires this exact private local cluster')
        for db,event in sorted(candidates.items()):
            if not admin.execute('SELECT 1 FROM pg_database WHERE datname=%s AND NOT datistemplate',(db,)).fetchone():continue
            if control.execute('SELECT count(*) FROM app_control.table_versions WHERE database_name=%s',(db,)).fetchone()[0]:raise RuntimeError('Native version history exists; retain '+db)
            with connect(db,autocommit=True) as c:
                marker=c.execute("SELECT to_regclass('challenge_lab.disposable')").fetchone()[0]
                if marker:require_lab(c)
                else:
                    # Early 100k clones predate the lab marker. Require a
                    # stronger live proof instead of creating a marker merely
                    # to pass cleanup: exact retained-template raw bytes,
                    # protected catalog/native sequence metadata, and no extra
                    # user objects. Receipts and zero native history still apply.
                    assert event['template']=='lab_generated_100k'
                    from bench.retire_failed_initialization import raw_fingerprint
                    from bench.audit_ranked_result import schema_value
                    from tests.equivalence.oracle import sequence_signature
                    if legacy_reference is None:
                        with connect('lab_generated_100k',autocommit=True) as reference:
                            assert reference.execute('SELECT count(*) FROM public.raw_data').fetchone()[0]==100000
                            legacy_reference=(raw_fingerprint(reference),schema_value(reference),sequence_signature(reference))
                    assert c.execute('SELECT count(*) FROM public.raw_data').fetchone()[0]==100000
                    assert (raw_fingerprint(c),schema_value(c),sequence_signature(c))==legacy_reference,'Legacy source/catalog/sequence differs; retain '+db
                    objects=c.execute("SELECT relname,relkind FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind IN ('r','p','v','m','f') ORDER BY relname").fetchall()
                    assert objects==[(table,'r') for table in ['raw_data','table1','table2','table3','table4','table5','table6']]
                    assert not c.execute("SELECT nspname FROM pg_namespace WHERE nspname NOT IN ('public','information_schema') AND nspname !~ '^pg_'").fetchall()
                    assert not c.execute("SELECT 1 FROM pg_proc WHERE pronamespace='public'::regnamespace").fetchall()
                    event['legacy_marker_alternative']={'retained_template_exact_native_raw_copy':legacy_reference[0],'same_protected_catalog_native_sequence_options':True,'exactly_seven_public_tables_no_extra_user_objects':True,'source_rows':100000}
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
