"""Read-only durability/index/table metadata supplement to the live row audit."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path

from bench.common import ROOT,connect,write_report
from pipeline.recipes import ORDER


def catalog(c):
    relations=['raw_data',*ORDER]
    tables=c.execute('''SELECT relname,relkind,relpersistence,relispartition,relrowsecurity,relforcerowsecurity,
        pg_get_userbyid(relowner),reloptions,reltablespace,relacl::text
        FROM pg_class WHERE relnamespace='public'::regnamespace AND relname=ANY(%s) ORDER BY relname''',(relations,)).fetchall()
    assert len(tables)==7 and all(t[1:6]==('r','p',False,False,False) for t in tables)
    indexes=c.execute('''SELECT t.relname,x.relname,i.indisvalid,i.indisready,i.indislive
        FROM pg_index i JOIN pg_class t ON t.oid=i.indrelid JOIN pg_class x ON x.oid=i.indexrelid
        WHERE t.relnamespace='public'::regnamespace AND t.relname=ANY(%s) ORDER BY t.relname,x.relname''',(relations,)).fetchall()
    assert indexes and all(all(row[2:]) for row in indexes)
    metadata=c.execute("SELECT relname,relpersistence FROM pg_class WHERE relnamespace='public'::regnamespace AND relname LIKE 'pgdm_%' AND relkind='r' ORDER BY relname").fetchall()
    assert metadata and all(row[1]=='p' for row in metadata)
    durability={key:c.execute('SELECT current_setting(%s)',(key,)).fetchone()[0] for key in ('fsync','full_page_writes','synchronous_commit')}
    assert all(value=='on' for value in durability.values())
    return {'tables':tables,'indexes':indexes,'durable_metadata_tables':metadata,'session_durability':durability}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input',default='reports/ranked-cohort-live-audit-v4.json')
    p.add_argument('--output',default='reports/ranked-cohort-catalog-supplement.json')
    args=p.parse_args();source=(ROOT/args.input).resolve();target=(ROOT/args.output).resolve()
    if not source.is_relative_to(ROOT/'reports') or not target.is_relative_to(ROOT/'reports') or target.exists():p.error('Use an existing input and a new output under reports/')
    audit=json.loads(source.read_text(encoding='utf-8'))
    checks=audit.get('live_checks',[])
    assert audit['passed'] and checks and len({row['database'] for row in checks})==len(checks)
    result={'kind':'read_only_catalog_and_durability_supplement','observed_at_utc':datetime.now(timezone.utc).isoformat(),'input_audit_sha256_normalized_lf':hashlib.sha256(source.read_text(encoding='utf-8').encode()).hexdigest(),'source_sha256_normalized_lf':hashlib.sha256(Path(__file__).read_text(encoding='utf-8').encode()).hexdigest(),'passed':False,'databases':[],'scope':'Current catalog/session checks after the row audit; no new timing measurement or root-cause diagnosis.'}
    with connect(audit['reference']['database'],autocommit=True) as c:expected=catalog(c)
    for row in audit['live_checks']:
        db=row['database'];assert db.startswith('lab_')
        with connect(db,autocommit=True) as c:actual=catalog(c)
        assert actual==expected,db
        result['databases'].append({'database':db,**actual})
    with connect('postgres_data_manager',autocommit=True) as c:
        controls=c.execute("SELECT relname,relpersistence FROM pg_class WHERE relnamespace='app_control'::regnamespace AND relkind='r' ORDER BY relname").fetchall()
        assert controls and all(row[1]=='p' for row in controls)
        result['durable_control_tables']=controls
    result['passed']=True;write_report(target,result)
    print('Catalog/durability PASS:',len(result['databases']),'databases; all indexes valid/ready/live; final/control metadata logged',flush=True)


if __name__=='__main__':main()
