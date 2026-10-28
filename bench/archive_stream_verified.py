"""Verify every pg_restore COPY row plus an actual schema/sequence restore.

This avoids a second multi-GB raw heap during archive verification. A full
self-contained custom dump remains. Restored COPY text is matched byte-for-byte
against PostgreSQL's original COPY output under identical session settings;
all schema/ACL/sequence state is separately restored in an empty verifier DB.
It is explicitly stream verification, not a claimed full populated restore.
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
import uuid
from psycopg import sql
from bench.common import ROOT,connect,require_lab,write_report
from bench.archive_native_clone import snapshot,digest,retire_verified
from tests.recovery.bulk_dump import history


def normalized(value):return json.loads(json.dumps(value,default=str))


def verify_data(c,archive,pg_restore,tables):
    expected={}
    for schema,table,*_ in tables:
        key=schema+'.'+table;columns=c.execute('SELECT attname FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum',(key,)).fetchall()
        assert all(re.fullmatch('[a-z_][a-z_0-9]*',name) for name, in columns)
        h=hashlib.sha256();size=rows=0
        with c.cursor().copy(sql.SQL('COPY {} TO STDOUT').format(sql.Identifier(schema,table))) as reader:
            for block in reader:
                value=bytes(block);h.update(value);size+=len(value);rows+=value.count(b'\n')
        expected[key]={'columns':','.join(name for name, in columns),'sha256':h.hexdigest(),'bytes':size,'rows':rows}
    # PG dump/restore writes LF or platform CRLF record separators. Literal
    # CR/LF inside a field are COPY-escaped, so normalizing only the separator
    # preserves every field byte and cannot conceal a changed data character.
    process=subprocess.Popen([str(pg_restore),'--data-only','-f','-',str(archive)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=False,creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
    actual={};key=None;h=None;size=rows=0
    try:
        for line in process.stdout:
            line=line.rstrip(b'\r\n')
            if key is None:
                match=re.fullmatch(rb'COPY ([a-z_][a-z_0-9]*\.[a-z_][a-z_0-9]*) \(([^)]+)\) FROM stdin;',line)
                if match:
                    key=match[1].decode('ascii');assert key in expected and key not in actual
                    assert match[2].decode('ascii').replace(' ','')==expected[key]['columns']
                    h=hashlib.sha256();size=rows=0
            elif line==b'\\.':
                actual[key]={'columns':expected[key]['columns'],'sha256':h.hexdigest(),'bytes':size,'rows':rows};assert actual[key]==expected[key],key;key=None
            else:
                h.update(line+b'\n');size+=len(line)+1;rows+=1
        error=process.stderr.read();process.wait(timeout=30)
        assert process.returncode==0,error.decode('utf8',errors='replace')
        assert key is None and actual==expected,'Incomplete or unexpected table data in rendered archive'
        return actual
    except BaseException:
        if process.poll() is None:process.kill()
        process.wait(timeout=10);raise
    finally:process.stdout.close();process.stderr.close()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--cohort',required=True);p.add_argument('--database',required=True);p.add_argument('--apply',action='store_true');p.add_argument('--archive-official',action='store_true',help='Archive only an explicitly named superseded native control, with all rows and metadata verified');args=p.parse_args()
    path=(ROOT/args.cohort).resolve();assert path.is_relative_to(ROOT/'reports')
    cohort=json.loads(path.read_text(encoding='utf8'));assert cohort['summary']
    native=cohort['kind']=='fresh_native_compact_complete_processing_pair'
    assert native or cohort['kind']=='fresh_local_original_service_complete_processing'
    run=next(row for row in cohort['runs'] if row['database']==args.database)
    if args.archive_official:
        assert run['mode']=='official'
        if not native:
            legacy_controls={
                'native-official-recheck-after-recovery-1m.json':{'lab_native_variant_55ed80f430'},
                'native-refined-copy-official-1m-repeated.json':{'lab_native_variant_9813b98fdf','lab_native_variant_10534e476c','lab_native_variant_45af04425c'},
            }
            assert args.database in legacy_controls.get(path.name,set())
    else:assert run['mode'] not in ('official','baseline','combined_shape_copy'),'Official controls require the separate explicit archive option'
    if native:
        # Only the two superseded C cohorts can use this path. Keep the
        # qualified shared-details worker and every official control live.
        implementation=cohort.get('candidate_implementation')
        if implementation is None:
            assert path.name=='native-c-compact-official-1m-pair.json' and args.database==('lab_native_official_b75fcb252b' if args.archive_official else 'lab_native_compact_9b3ae1a447')
            implementation='compact'
        assert implementation in ('compact','minimal','fk_stream')
        if implementation=='fk_stream':assert path.name=='native-fk-stream-official-1m-pair.json'
        assert args.database==cohort['official_database' if args.archive_official else 'candidate_database']
        assert run['mode']==('official' if args.archive_official else 'native_compact')
        current=json.loads((ROOT/'reports/native-fk-official-1m-pair.json').read_text())
        assert current['summary']['exact_six_outputs_and_checks_passed']
        assert current['summary']['reduction_percent']>cohort['summary']['reduction_percent']
        assert re.fullmatch('lab_native_'+('official' if args.archive_official else 'compact')+'_[0-9a-f]{10}',args.database)
    else:assert re.fullmatch('lab_native_(variant|fresh)_[0-9a-f]{10}',args.database)
    child_path=Path(run['report']);assert child_path.resolve().is_relative_to(ROOT/'reports')
    child=json.loads(child_path.read_text(encoding='utf8'))
    if args.archive_official and native:assert child['unmodified_reference_established'] is True and child['destination_warmup_before_measured_run'] is False
    else:assert child.get('six_table_fingerprints_equivalent',child.get('exact_six_tables')) is True
    assert len(child['versions'])==6 and all(child[k] is True for k in ('counter_reconciliation','protected_schema_equivalent','sequence_options_and_ownership_equivalent','workspace_namespace_restored'))
    if not args.apply:print('Would create/check full archive and retire',args.database);return
    assert shutil.disk_usage(ROOT).free>=512*1024**2
    db=args.database;restored='lab_stream_archive_'+uuid.uuid4().hex[:10]
    archive=ROOT/'.runtime/native_archives'/(db+'-stream-verified.dump');output=ROOT/'reports/native-archives'/(db+'-stream-verified.json')
    assert not archive.exists() and not output.exists(),'Never overwrite backup evidence'
    source=Path(__file__).read_text(encoding='utf8').encode();sha=hashlib.sha256(source).hexdigest();source_archive=ROOT/'reports/source'/(sha+'.py');source_archive.write_bytes(source)
    report={'kind':'native_complete_dump_stream_and_metadata_restore_verified','database':db,'restore_test_database':restored,'cohort':str(path.relative_to(ROOT)),'archive':str(archive.relative_to(ROOT)),'passed':False,'original_removed':False,'source_sha256':sha,'source_archive':str(source_archive.relative_to(ROOT)),'timestamp_utc':datetime.now(timezone.utc).isoformat(),'scope':'Full self-contained custom dump; all native COPY text rows compared by SHA256/count/bytes, actual empty schema/owner/ACL/sequence restore checked. A full populated restore is not claimed for this archive.'}
    try:
        with connect('postgres',autocommit=True) as admin,connect('postgres_data_manager',autocommit=True) as control,connect(db,autocommit=True) as original:
            require_lab(original)
            expected_directory=ROOT/('.runtime/native_pgdata' if native else '.runtime/pgdata')
            assert admin.info.host in ('127.0.0.1','localhost','::1') and Path(admin.execute('SHOW data_directory').fetchone()[0]).resolve()==expected_directory.resolve()
            assert not admin.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>ALL(%s)",([admin.info.backend_pid,control.info.backend_pid,original.info.backend_pid],)).fetchall(),'No overlapping jobs'
            versions=history(control,db);assert len([v for v, in versions if v['version_code']!='0000'])==6
            original.execute("SET TimeZone='UTC'; SET DateStyle='ISO, MDY'; SET IntervalStyle='postgres'; SET extra_float_digits=3; SET bytea_output='hex'")
            with original.transaction():
                original.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                tables=original.execute("SELECT n.nspname,c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='r' AND n.nspname NOT IN ('pg_catalog','information_schema') AND n.nspname NOT LIKE 'pg_toast%' ORDER BY 1,2").fetchall()
                original.execute(sql.SQL('LOCK TABLE '+','.join('{}' for _ in tables)+' IN SHARE MODE').format(*(sql.Identifier(*table) for table in tables)))
                before=snapshot(db);snapshot_id=original.execute('SELECT pg_export_snapshot()').fetchone()[0]
                encoding,collation,ctype=original.execute('SELECT pg_encoding_to_char(encoding),datcollate,datctype FROM pg_database WHERE datname=current_database()').fetchone()
                report['before']=before;write_report(output,report)
                executable=ROOT/'.runtime/postgresql/pgsql/bin'
                env={**os.environ,'PGOPTIONS':'-c timezone=UTC','PGCLIENTENCODING':'UTF8'}
                print('Dumping stable locked snapshot',db,flush=True)
                subprocess.run([str(executable/'pg_dump.exe'),'-Fc','--snapshot='+snapshot_id,'-f',str(archive),db],env=env,check=True)
                report['archive_sha256']=digest(archive);report['archive_bytes']=archive.stat().st_size;write_report(output,report)
                print('Verifying every rendered native COPY row',db,flush=True)
                report['all_table_archive_copy_text']=verify_data(original,archive,executable/'pg_restore.exe',before['tables_owners_acl_persistence'])
                toc=subprocess.check_output([str(executable/'pg_restore.exe'),'-l',str(archive)],text=True,encoding='utf8')
                selected=[line for line in toc.splitlines() if not re.search(r'; \d+ \d+ (TABLE DATA|MATERIALIZED VIEW DATA) ',line)]
                toc_path=archive.with_suffix('.metadata.toc');assert not toc_path.exists();toc_path.write_text('\n'.join(selected)+'\n',encoding='utf8')
                report['metadata_toc_sha256']=digest(toc_path)
                admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0 ENCODING {} LC_COLLATE {} LC_CTYPE {}').format(sql.Identifier(restored),sql.Literal(encoding),sql.Literal(collation),sql.Literal(ctype)))
                subprocess.run([str(executable/'pg_restore.exe'),'--exit-on-error','-L',str(toc_path),'-d',restored,str(archive)],check=True)
                restored_state=normalized(snapshot(restored));expected=normalized(before)
                del restored_state['all_table_binary_values'];del expected['all_table_binary_values']
                assert restored_state==expected,'Restored schema/ACL/sequence metadata differs; retain original'
                assert snapshot(db)==before and history(control,db)==versions and digest(archive)==report['archive_sha256']
                report.update({'passed':True,'actual_schema_owner_acl_sequence_restore_verified':True,'all_source_and_destination_copy_text_rows_identical':True,'control_history_unchanged':True,'full_populated_restore_claimed':False});write_report(output,report)
        # The original SHARE locks and snapshot are closed before normal DROP;
        # no FORCE, client termination, manual data-file work or version edits.
        with connect('postgres',autocommit=True) as admin:
            retire_verified(admin,restored);report['restore_test_removed']=True;write_report(output,report)
            retire_verified(admin,db);report['original_removed']=True;write_report(output,report)
        print('Full archive streams and native metadata restore PASS',db,flush=True)
    except BaseException as error:report['error']=type(error).__name__+': '+str(error);write_report(output,report);raise


if __name__=='__main__':main()
