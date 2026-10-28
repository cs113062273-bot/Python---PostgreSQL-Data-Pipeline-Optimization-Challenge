"""Restore-test superseded pilots with a byte-verified deduplicated raw input.

The locked durable input template and a complete metadata/non-raw-data dump
contain every native value. All non-raw state is restored and byte-checked
before retirement; then the complete raw input is reconstructed and the entire
database checked again. Control history is never removed. A failed restore
before retirement keeps the original; later failure keeps the complete backup
and its locked input source for a subsequent restore.
"""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import uuid
from psycopg import sql
from bench.common import ROOT,connect,require_lab,write_report
from bench.archive_native_clone import snapshot,retire_verified,digest
from bench.retire_failed_initialization import raw_fingerprint
from tests.recovery.bulk_dump import history
from pipeline.bulk_fks import FOREIGN_KEYS


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--cohort',required=True);p.add_argument('--database',required=True)
    p.add_argument('--resume-backup',action='store_true',help='Reuse a checked backup after incomplete restore cleanup')
    args=p.parse_args()
    receipt=(ROOT/args.cohort).resolve();assert receipt.is_relative_to(ROOT/'reports')
    cohort=json.loads(receipt.read_text(encoding='utf8'));assert cohort['kind']=='fresh_local_original_service_complete_processing' and cohort['summary']
    row=next(r for r in cohort['runs'] if r['database']==args.database)
    eligible={'lab_native_variant_47bfb797fe':'server_copy','lab_native_variant_7913643ace':'refined_copy','lab_native_variant_bb5fad3206':'memo_copy','lab_native_variant_4a7318e196':'guarded_copy'}
    assert row['mode']==eligible[args.database]
    child_path=Path(row['report']);child=json.loads(child_path.read_text(encoding='utf8'))
    assert all(child[k] is True for k in ('exact_six_tables','six_table_fingerprints_equivalent','counter_reconciliation','protected_schema_equivalent','sequence_options_and_ownership_equivalent','workspace_namespace_restored')) and len(child['versions'])==6
    assert shutil.disk_usage(ROOT).free>=int(2.5*1024**3),'Reserve space for a non-raw restore; never start a full raw duplicate here'
    db=args.database;template='lab_template_1m';restored='lab_template_archive_'+uuid.uuid4().hex[:10]
    archive=(ROOT/'.runtime/native_archives'/(db+'-without-raw.dump')).resolve()
    output=ROOT/'reports/native-archives'/(db+'-template-backed.json')
    if not args.resume_backup:assert not archive.exists() and not output.exists(),'Never overwrite archival evidence'
    prior=None
    if args.resume_backup:
        prior=json.loads(output.read_text(encoding='utf8'))
        assert not prior['original_removed'] and not prior['passed'] and prior['incomplete_restore_removed']
        assert prior['database']==db and prior['cohort']==str(receipt.relative_to(ROOT))
        assert prior['archive']==str(archive.relative_to(ROOT)) and digest(archive)==prior['archive_sha256']
    source=Path(__file__).read_text(encoding='utf8').encode();sha=hashlib.sha256(source).hexdigest()
    source_archive=ROOT/'reports/source'/(sha+'.py');source_archive.write_bytes(source)
    report={'kind':'native_template_backed_archive','database':db,'template':template,'restore_test_database':restored,'cohort':str(receipt.relative_to(ROOT)),'timestamp_utc':datetime.now(timezone.utc).isoformat(),'archive':str(archive.relative_to(ROOT)),'passed':False,'original_removed':False,'source_sha256':sha,'source_archive':str(source_archive.relative_to(ROOT))}
    if prior:
        report['previous_failed_restore']={k:prior[k] for k in ('restore_test_database','error','source_sha256','source_archive','cleanup_timestamp_utc')}
    def normalized(value):return json.loads(json.dumps(value,default=str))
    try:
        with connect('postgres',autocommit=True) as admin,connect(os.environ.get('PDM_CONTROL_DB','postgres_data_manager'),autocommit=True) as control,connect(template,autocommit=True) as source:
            assert admin.info.host in ('127.0.0.1','localhost','::1')
            assert Path(admin.execute('SHOW data_directory').fetchone()[0]).resolve()==(ROOT/'.runtime/pgdata').resolve()
            assert not admin.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>ALL(%s)",([admin.info.backend_pid,control.info.backend_pid,source.info.backend_pid],)).fetchall(),'No overlapping jobs'
            versions=history(control,db);assert len([v for v, in versions if v['version_code']!='0000'])==6
            report['control_history_sha256']=hashlib.sha256(json.dumps(versions,default=str,sort_keys=True).encode()).hexdigest()
            with source.transaction():
                require_lab(source);source.execute('LOCK TABLE public.raw_data IN SHARE MODE')
                before=snapshot(db);template_raw=raw_fingerprint(source)
                assert before['all_table_binary_values']['public.raw_data']==template_raw
                if prior:assert normalized(before)==prior['before']
                with connect(db,autocommit=True) as original:
                    require_lab(original)
                    encoding,collation,ctype=original.execute('SELECT pg_encoding_to_char(encoding),datcollate,datctype FROM pg_database WHERE datname=current_database()').fetchone()
                report.update({'before':before,'locked_template_raw':template_raw,'raw_restore_sequence':'After all other restored native bytes/catalog/sequences are checked, retire original, then reconstruct raw from the locked byte-identical logged template and check the entire database.'});write_report(output,report)
                executable=ROOT/'.runtime/postgresql/pgsql/bin'
                print('Raw bytes match locked template; dumping all other state',db,flush=True)
                if not prior:subprocess.run([str(executable/'pg_dump.exe'),'-Fc','--exclude-table-data=public.raw_data','-f',str(archive),db],check=True)
                report.update({'archive_sha256':digest(archive),'archive_bytes':archive.stat().st_size});write_report(output,report)
                toc=subprocess.check_output([str(executable/'pg_restore.exe'),'-l',str(archive)],text=True,encoding='utf8')
                foreign=[line for line in toc.splitlines() if re.search(r'; \d+ \d+ FK CONSTRAINT ',line)]
                assert len(foreign)==6
                assert {(line.split(' FK CONSTRAINT ',1)[1].split()[1],line.split(' FK CONSTRAINT ',1)[1].split()[2]) for line in foreign}=={(table,name) for table,name,_ in FOREIGN_KEYS}
                pre_list=archive.with_suffix('.pre-raw.toc');post_list=archive.with_suffix('.post-raw.toc')
                assert not pre_list.exists() and not post_list.exists(),'Never overwrite restore selection evidence'
                pre_list.write_text('\n'.join(line for line in toc.splitlines() if line not in foreign)+'\n',encoding='utf8')
                post_list.write_text('\n'.join(foreign)+'\n',encoding='utf8')
                report['deferred_verifier_fk_objects']=foreign
                report['pre_raw_toc_sha256']=digest(pre_list);report['post_raw_toc_sha256']=digest(post_list)
                admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0 ENCODING {} LC_COLLATE {} LC_CTYPE {}').format(sql.Identifier(restored),sql.Literal(encoding),sql.Literal(collation),sql.Literal(ctype)))
                subprocess.run([str(executable/'pg_restore.exe'),'--exit-on-error','-L',str(pre_list),'-d',restored,str(archive)],check=True)
                partial=normalized(snapshot(restored));expected=normalized(before)
                del partial['all_table_binary_values']['public.raw_data'];del expected['all_table_binary_values']['public.raw_data']
                keys={(table,name) for table,name,_ in FOREIGN_KEYS}
                constraints=expected['public_catalog']['constraints']
                excluded=[row for row in constraints if tuple(row[:2]) in keys]
                assert len(excluded)==6 and all(row[3] is True for row in excluded)
                expected['public_catalog']['constraints']=[row for row in constraints if tuple(row[:2]) not in keys]
                assert partial==expected,'Non-raw restore differs; retain original'
                assert snapshot(db)==before and history(control,db)==versions and digest(archive)==report['archive_sha256']
                report['all_non_raw_native_values_catalog_acl_sequences_verified']=True;write_report(output,report)
                retire_verified(admin,db);report['original_removed']=True;write_report(output,report)
                print('Reconstructing complete raw input',restored,flush=True)
                with connect(restored,autocommit=True) as target:
                    with source.cursor().copy('COPY (SELECT * FROM public.raw_data ORDER BY raw_id) TO STDOUT (FORMAT BINARY)') as reader:
                        with target.cursor().copy('COPY public.raw_data FROM STDIN (FORMAT BINARY)') as writer:
                            pending=bytearray()
                            for block in reader:
                                pending.extend(block)
                                if len(pending)>=1024*1024:writer.write(bytes(pending));pending.clear()
                            if pending:writer.write(bytes(pending))
                # This applies/validates every original FK before the final
                # whole-database comparison. Deferred only in this private
                # verifier copy; the actual pipeline completion is unchanged.
                subprocess.run([str(executable/'pg_restore.exe'),'--exit-on-error','-L',str(post_list),'-d',restored,str(archive)],check=True)
                assert snapshot(restored)==before,'Full reconstruction differs; retain restore and complete backup'
                assert raw_fingerprint(source)==template_raw and history(control,db)==versions and digest(archive)==report['archive_sha256']
                report['full_reconstruction_all_native_values_catalog_acl_sequences_verified']=True
                report['control_history_unchanged']=True;report['passed']=True;write_report(output,report)
                retire_verified(admin,restored);report['restore_test_removed']=True;write_report(output,report)
        print('Verified deduplicated archive PASS',db,flush=True)
    except BaseException as error:
        report['error']=type(error).__name__+': '+str(error);write_report(output,report);raise


if __name__=='__main__':main()
