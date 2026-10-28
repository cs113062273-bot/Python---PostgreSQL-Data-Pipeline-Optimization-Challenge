"""Retire a zero-output failed clone using a verified duplicate input backup.

The retained input template must match every ordered native raw-data byte.
All other objects/data are pg_dump'ed and restore-tested first. After removal,
the full input is reconstructed from that locked template and every native
value/catalog/sequence is checked again before retiring the verification copy.
No completed processing result or control history is deleted.
"""
import argparse
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import subprocess
import uuid
from psycopg import sql
from bench.common import ROOT,connect,require_lab,write_report
from bench.archive_native_clone import snapshot,retire_verified,digest
from pipeline.recipes import ORDER
from tests.recovery.bulk_dump import history


def raw_fingerprint(c):
    h=hashlib.sha256();size=0
    with c.cursor().copy('COPY (SELECT * FROM public.raw_data ORDER BY raw_id) TO STDOUT (FORMAT BINARY)') as reader:
        for block in reader:h.update(block);size+=len(block)
    return {'sha256':h.hexdigest(),'binary_bytes':size}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--evidence',required=True)
    args=p.parse_args();evidence=(ROOT/args.evidence).resolve()
    assert evidence.is_relative_to(ROOT/'reports')
    recorded=json.loads(evidence.read_text(encoding='utf8'))
    db=recorded['excluded_startup_database'];assert db=='lab_native_variant_08fad60ca3'
    assert recorded['protected_data_script_not_sent'] and recorded['excluded_startup_expanded_versions']==0
    template='lab_template_1m';restored='lab_failed_restore_'+uuid.uuid4().hex[:10]
    parent=(ROOT/'.runtime/native_archives').resolve();parent.mkdir(exist_ok=True)
    archive=parent/(db+'-metadata-only.dump');output=ROOT/'reports/native-archives'/(db+'-failed-initialization.json')
    assert not archive.exists() and not output.exists(),'Never overwrite retirement evidence'
    report={'kind':'verified_zero_output_failed_clone_retirement','database':db,'template':template,'restore_test_database':restored,'evidence':str(evidence.relative_to(ROOT)),'timestamp_utc':datetime.now(timezone.utc).isoformat(),'original_removed':False,'passed':False,'metadata_dump':str(archive.relative_to(ROOT))}
    with connect('postgres',autocommit=True) as admin,connect('postgres_data_manager',autocommit=True) as control,connect(template,autocommit=True) as source:
        assert admin.info.host in ('127.0.0.1','localhost','::1')
        assert Path(admin.execute('SHOW data_directory').fetchone()[0]).resolve()==(ROOT/'.runtime/pgdata').resolve()
        assert not admin.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>ALL(%s)",([admin.info.backend_pid,control.info.backend_pid,source.info.backend_pid],)).fetchall(),'No overlapping jobs'
        versions=history(control,db);assert versions and not any(v['version_code']!='0000' for v, in versions)
        with source.transaction():
            require_lab(source);source.execute('LOCK TABLE public.raw_data IN SHARE MODE')
            with connect(db,autocommit=True) as original:
                require_lab(original)
                assert not any(original.execute('SELECT count(*) FROM public.'+table).fetchone()[0] for table in ORDER)
                assert original.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()[0] is None
                encoding,collation,ctype=original.execute('SELECT pg_encoding_to_char(encoding),datcollate,datctype FROM pg_database WHERE datname=current_database()').fetchone()
            before=snapshot(db);template_raw=raw_fingerprint(source)
            assert before['all_table_binary_values']['public.raw_data']==template_raw
            report.update({'before':before,'locked_retained_template_raw':template_raw,'all_six_destinations_empty':True,'expanded_versions':0})
            write_report(output,report)
            print('Input bytes match retained template; backing up all remaining state',flush=True)
            executable=ROOT/'.runtime/postgresql/pgsql/bin'
            subprocess.run([str(executable/'pg_dump.exe'),'-Fc','--exclude-table-data=public.raw_data','-f',str(archive),db],check=True)
            report['metadata_dump_sha256']=digest(archive);report['metadata_dump_bytes']=archive.stat().st_size
            admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0 ENCODING {} LC_COLLATE {} LC_CTYPE {}').format(sql.Identifier(restored),sql.Literal(encoding),sql.Literal(collation),sql.Literal(ctype)))
            subprocess.run([str(executable/'pg_restore.exe'),'--exit-on-error','-d',restored,str(archive)],check=True)
            partial=snapshot(restored);expected=json.loads(json.dumps(before,default=str));partial=json.loads(json.dumps(partial,default=str))
            del partial['all_table_binary_values']['public.raw_data'];del expected['all_table_binary_values']['public.raw_data']
            assert partial==expected,'Metadata/empty destinations differ; retain original'
            assert snapshot(db)==before and history(control,db)==versions
            report['all_non_raw_state_restore_verified_before_retirement']=True;write_report(output,report)
            # At this point a checked metadata dump plus the still-locked,
            # byte-identical durable template contain every original value.
            retire_verified(admin,db);report['original_removed']=True;write_report(output,report)
            print('Reconstructing complete verification copy from retained input',flush=True)
            with connect(restored,autocommit=True) as target:
                with source.cursor().copy('COPY (SELECT * FROM public.raw_data ORDER BY raw_id) TO STDOUT (FORMAT BINARY)') as reader:
                    with target.cursor().copy('COPY public.raw_data FROM STDIN (FORMAT BINARY)') as writer:
                        pending=bytearray()
                        for block in reader:
                            pending.extend(block)
                            if len(pending)>=1024*1024:writer.write(bytes(pending));pending.clear()
                        if pending:writer.write(bytes(pending))
            assert snapshot(restored)==before,'Full reconstruction differs; retain verification copy and backup'
            assert raw_fingerprint(source)==template_raw and history(control,db)==versions
            assert digest(archive)==report['metadata_dump_sha256']
            report['full_reconstruction_all_native_values_catalog_acl_sequences_verified']=True
            report['control_history_unchanged']=True;report['passed']=True;write_report(output,report)
            retire_verified(admin,restored);report['restore_test_removed']=True;write_report(output,report)
    print('Failed initialization safely retired; full reconstruction verified',flush=True)


if __name__=='__main__':main()
