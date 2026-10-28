"""Archive a receipted losing native clone after a full verified restore.

All table values (including raw JSON and allocated identities) are compared
as SHA256 of ordered native binary COPY, with schema, owners, ACLs, sequences
and unchanged control history. Only then may both lab copies be retired.
The compressed dump and manifest remain; the original name can be restored.
"""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time
import uuid
from psycopg import sql
from bench.common import ROOT,connect,require_lab,write_report
from bench.audit_ranked_result import schema_value
from tests.recovery.bulk_dump import history,sequence_state
from tests.equivalence.oracle import sequence_signature


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()


def snapshot(database):
    with connect(database,autocommit=True) as c:
        require_lab(c)
        c.execute("SET TimeZone='UTC'; SET search_path=pg_catalog;")
        with c.transaction():
            c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            tables=c.execute("SELECT n.nspname,r.relname,r.relpersistence,r.relowner::regrole::text,r.relacl::text FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace WHERE r.relkind='r' AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%' AND n.nspname NOT LIKE 'pg_temp%' ORDER BY 1,2").fetchall()
            assert all(row[2]=='p' for row in tables),'Only wholly durable completed clones'
            fingerprints={}
            for schema,table,*_ in tables:
                identifier=sql.Identifier(schema,table)
                columns=c.execute("SELECT a.attname FROM pg_index i CROSS JOIN LATERAL unnest(i.indkey::smallint[]) WITH ORDINALITY k(attnum,pos) JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum WHERE i.indrelid=%s::regclass AND i.indisprimary ORDER BY k.pos",(schema+'.'+table,)).fetchall()
                if columns:
                    order=sql.SQL(',').join(sql.Identifier(row[0]) for row in columns)
                else:
                    assert c.execute(sql.SQL('SELECT count(*) FROM {}').format(identifier)).fetchone()[0]<=10000,'Large tables require a deterministic primary key'
                    order=sql.SQL('(t::text) COLLATE "C"')
                query=sql.SQL('COPY (SELECT t.* FROM {} t ORDER BY {}) TO STDOUT (FORMAT BINARY)').format(identifier,order)
                h=hashlib.sha256();size=0
                with c.cursor().copy(query) as reader:
                    for block in reader:h.update(block);size+=len(block)
                fingerprints[schema+'.'+table]={'sha256':h.hexdigest(),'binary_bytes':size}
            return {'public_catalog':schema_value(c),'tables_owners_acl_persistence':tables,'all_table_binary_values':fingerprints,'sequence_options':sequence_signature(c),'sequence_values':sequence_state(c)}


def retire_verified(admin,database):
    # libpq close and backend unregister are asynchronous. Never terminate a
    # client: allow a short drain, then fail closed if a session remains.
    deadline=time.monotonic()+30
    admin.execute('SELECT pg_stat_clear_snapshot()')
    while admin.execute("SELECT pid FROM pg_stat_activity WHERE datname=%s AND backend_type='client backend'",(database,)).fetchall():
        if time.monotonic()>=deadline:raise RuntimeError('Live clients remain; retain '+database)
        time.sleep(.05)
        admin.execute('SELECT pg_stat_clear_snapshot()')
    # Ordinary DROP DATABASE handles its own autovacuum workers. No FORCE and
    # no client termination: PostgreSQL still refuses other blocking sessions.
    admin.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(database)))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--cohort',required=True)
    p.add_argument('--database',required=True)
    p.add_argument('--apply',action='store_true')
    p.add_argument('--resume-verified',action='store_true',help='Recheck both complete copies after a verified archive was interrupted during retirement')
    args=p.parse_args();receipt=(ROOT/args.cohort).resolve()
    assert receipt.is_relative_to(ROOT/'reports')
    cohort=json.loads(receipt.read_text(encoding='utf8'))
    assert cohort['kind']=='fresh_local_original_service_complete_processing' and cohort.get('summary')
    run=next(r for r in cohort['runs'] if r['database']==args.database)
    permitted=run['mode'] in ('fast_copy','scalar_copy','early_copy','scaled_copy','unlogged_copy','server_copy')
    # Preserve the complete paired refined cohort live; only the superseded
    # unpaired pilot is eligible here. Every byte remains restorable in archive.
    permitted=permitted or (run['mode']=='refined_copy' and args.database=='lab_native_variant_7913643ace')
    permitted=permitted or (run['mode']=='memo_copy' and args.database=='lab_native_variant_bb5fad3206')
    assert permitted,'Retain all official controls and selected paired refined/memo workers live'
    assert re.fullmatch('lab_native_variant_[0-9a-f]{10}',args.database)
    child_path=Path(run['report']);child_path=child_path if child_path.is_absolute() else ROOT/child_path
    assert child_path.resolve().is_relative_to(ROOT/'reports')
    child=json.loads(child_path.read_text(encoding='utf8'))
    assert all(child[k] is True for k in ('six_table_fingerprints_equivalent','counter_reconciliation','protected_schema_equivalent','sequence_options_and_ownership_equivalent','workspace_namespace_restored')) and len(child['versions'])==6
    directory=(ROOT/'.runtime/native_archives').resolve();directory.mkdir(exist_ok=True)
    dump=directory/(args.database+'.dump');output=ROOT/'reports/native-archives'/(args.database+'.json')
    if not args.resume_verified:assert not dump.exists() and not output.exists(),'Never overwrite an archive'
    restored='lab_archive_verify_'+uuid.uuid4().hex[:10]
    report={'kind':'verified_native_clone_archive','database':args.database,'restore_test_database':restored,'cohort':str(receipt.relative_to(ROOT)),'timestamp_utc':datetime.now(timezone.utc).isoformat(),'passed':False,'original_removed':False,'dump':str(dump.relative_to(ROOT))}
    source=Path(__file__).read_text(encoding='utf8').encode();source_hash=hashlib.sha256(source).hexdigest()
    source_archive=ROOT/'reports/source'/(source_hash+'.py')
    if not source_archive.exists():source_archive.write_bytes(source)
    assert digest(source_archive)==source_hash
    report['archive_tool_source']={'sha256_normalized_lf':source_hash,'archive':str(source_archive.relative_to(ROOT))}
    with connect('postgres',autocommit=True) as admin:
        assert admin.info.host in ('127.0.0.1','localhost','::1')
        assert Path(admin.execute('SHOW data_directory').fetchone()[0]).resolve()==(ROOT/'.runtime/pgdata').resolve()
        assert not admin.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'No overlapping database jobs'
        assert not admin.execute('SELECT pid FROM pg_stat_activity WHERE datname=%s',(args.database,)).fetchall(),'Do not interrupt clients'
        encoding,collation,ctype=admin.execute('SELECT pg_encoding_to_char(encoding),datcollate,datctype FROM pg_database WHERE datname=%s',(args.database,)).fetchone()
        if not args.apply:print('Would dump, restore, byte-check and archive',args.database);return
        assert shutil.disk_usage(ROOT).free>=8*1024**3,'Full duplicate restore requires >=8 GiB headroom; use an explicitly qualified template-backed archive for superseded pilots'
        with connect(os.environ.get('PDM_CONTROL_DB','postgres_data_manager'),autocommit=True) as control:
            versions=history(control,args.database);assert len([v for v, in versions if v['version_code']!='0000'])==6
        if args.resume_verified:
            report=json.loads(output.read_text(encoding='utf8'))
            assert report['passed'] and report['full_restore_all_native_bytes_catalog_sequences_verified'] and not report['original_removed']
            assert report['database']==args.database and report['cohort']==str(receipt.relative_to(ROOT))
            restored=report['restore_test_database'];assert re.fullmatch('lab_archive_verify_[0-9a-f]{10}',restored)
            assert report['dump']==str(dump.relative_to(ROOT)) and digest(dump)==report['dump_sha256']
            for database in (restored,args.database):
                current=json.loads(json.dumps(snapshot(database),default=str))
                assert current==report['before'],'Verified copy changed; retain both'
            with connect(os.environ.get('PDM_CONTROL_DB','postgres_data_manager'),autocommit=True) as control:assert history(control,args.database)==versions
            report['resume_rechecked_both_full_copies']=True
            report['finish_archive_tool_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
            for database in (restored,args.database):
                retire_verified(admin,database)
                report['restore_test_removed' if database==restored else 'original_removed']=True;write_report(output,report)
            print('Verified archive retirement resumed PASS',args.database,flush=True);return
        report['before']=snapshot(args.database);write_report(output,report)
        print('Snapshot complete; dumping',args.database,flush=True)
        executable=ROOT/'.runtime/postgresql/pgsql/bin'
        subprocess.run([str(executable/'pg_dump.exe'),'-Fc','-f',str(dump),args.database],check=True)
        report['dump_sha256']=digest(dump);report['dump_bytes']=dump.stat().st_size;write_report(output,report)
        admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0 ENCODING {} LC_COLLATE {} LC_CTYPE {}').format(sql.Identifier(restored),sql.Literal(encoding),sql.Literal(collation),sql.Literal(ctype)))
        print('Restoring private verification copy',restored,flush=True)
        subprocess.run([str(executable/'pg_restore.exe'),'--exit-on-error','-d',restored,str(dump)],check=True)
        assert snapshot(restored)==report['before'],'Restored values/catalog/sequences/ACLs differ; retain original'
        # Check the original again, catching any concurrent mutation during dump.
        assert snapshot(args.database)==report['before'],'Original changed; retain both copies'
        with connect(os.environ.get('PDM_CONTROL_DB','postgres_data_manager'),autocommit=True) as control:assert history(control,args.database)==versions
        assert digest(dump)==report['dump_sha256']
        report['passed']=True;report['full_restore_all_native_bytes_catalog_sequences_verified']=True
        report['control_versions_unchanged']=True;write_report(output,report)
        for database in (restored,args.database):
            retire_verified(admin,database)
            report['restore_test_removed' if database==restored else 'original_removed']=True;write_report(output,report)
        print('Verified restorable archive PASS',args.database,report['dump_bytes'],flush=True)


if __name__=='__main__':main()
