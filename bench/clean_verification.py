"""Reclaim our reproducible oracle copies; retain candidate DBs and reports.

Only explicit completed-run receipts authorize a verify_<hex> namespace. Six
plain single-jsonb-column tables are required. DROP TABLE/SCHEMA use RESTRICT,
so external dependencies or unexpected contents abort rather than cascade.
"""
import argparse
from datetime import datetime,timezone
import json
from pathlib import Path
import re
from psycopg import sql
from bench.common import ROOT,connect,require_lab,write_report
from pipeline.recipes import ORDER


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--reports',nargs='+',required=True)
    p.add_argument('--apply',action='store_true')
    args=p.parse_args()
    receipts={}
    for name in args.reports:
        path=(ROOT/name).resolve()
        if not path.is_relative_to(ROOT/'reports'):p.error('Use project report files')
        r=json.loads(path.read_text())
        single=r.get('kind')=='local_transport_organizer_service'
        native=r.get('kind')=='fresh_local_original_service_complete_processing'
        if not single and not native and r.get('kind')!='fresh_database_per_run_sql_diagnostic':p.error('Not a completed-run receipt: '+name)
        if native and not r.get('summary'):p.error('Native cohort is not complete: '+name)
        if single:
            gates=('exact_six_tables','six_table_fingerprints_equivalent','counter_reconciliation','protected_schema_equivalent','sequence_options_and_ownership_equivalent','workspace_namespace_restored')
            if not all(r.get(k) is True for k in gates) or len(r.get('versions',[]))!=6:p.error('Single service exact verification is not complete: '+name)
        database=r.get('separate_reference') if single else r['reference']
        if database not in ('lab_100k','lab_1m'):p.error('Unknown retained reference: '+database)
        for run in [r] if single else r['runs']:
            namespace=run.get('verification_schema')
            verified=run.get('exact_except_all') is True
            if native:verified=run.get('exact_six_tables') is True and run.get('six_table_fingerprints_equivalent') is True and run.get('counter_reconciliation') is True and run.get('versions')==6
            if single:verified=True # All complete-service gates checked above.
            if namespace and verified:
                if not re.fullmatch(r'verify_[0-9a-f]{10}',namespace):p.error('Unexpected namespace')
                receipts[(database,namespace)]=name
    events=[]
    for (database,namespace),receipt in sorted(receipts.items()):
        with connect(database,autocommit=True) as c:
            require_lab(c)
            assert c.info.host in ('localhost','127.0.0.1','::1')
            assert (ROOT/'.runtime').resolve() in Path(c.execute('SHOW data_directory').fetchone()[0]).resolve().parents
            with c.transaction():
                present=c.execute('SELECT oid FROM pg_namespace WHERE nspname=%s',(namespace,)).fetchone()
                if not present:continue
                relations=c.execute("SELECT relname,relkind FROM pg_class WHERE relnamespace=%s ORDER BY relname",(present[0],)).fetchall()
                if relations!=[(t,'r') for t in sorted(ORDER)]:raise RuntimeError('Unexpected namespace contents: '+namespace)
                for table in ORDER:
                    c.execute(sql.SQL('LOCK TABLE {} IN ACCESS EXCLUSIVE MODE NOWAIT').format(sql.Identifier(namespace,table)))
                columns=c.execute("SELECT c.relname,a.attname,a.atttypid::regtype::text FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid WHERE c.relnamespace=%s AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum",(present[0],)).fetchall()
                if columns!=[(t,'value','jsonb') for t in sorted(ORDER)]:raise RuntimeError('Unexpected verification table shape')
                size=c.execute('SELECT sum(pg_total_relation_size(oid)) FROM pg_class WHERE relnamespace=%s',(present[0],)).fetchone()[0]
                if args.apply:
                    c.execute(sql.SQL('DROP TABLE '+','.join('{}' for _ in ORDER)+' RESTRICT').format(*(sql.Identifier(namespace,t) for t in ORDER)))
                    c.execute(sql.SQL('DROP SCHEMA {} RESTRICT').format(sql.Identifier(namespace)))
                event={'database':database,'schema':namespace,'receipt':receipt,'bytes':int(size),'removed':args.apply}
                events.append(event)
                print(event,flush=True)
    print('Total MiB:',sum(e['bytes'] for e in events)/1024**2,flush=True)
    if args.apply:
        output=ROOT/'reports/verification-scratch-cleanup.json'
        audit=json.loads(output.read_text()) if output.exists() else {'kind':'reproducible_verification_storage_cleanup','runs':[]}
        audit['runs'].append({'timestamp_utc':datetime.now(timezone.utc).isoformat(),'events':events,'scope':'Only oracle copies removed. Canonical reference, all candidate databases and timing/verification receipts retained.'})
        write_report(output,audit)


if __name__=='__main__':main()
