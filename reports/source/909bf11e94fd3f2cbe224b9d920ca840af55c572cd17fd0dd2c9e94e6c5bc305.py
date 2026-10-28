"""Remove only an incomplete private restore while its exact original exists."""
import argparse
from datetime import datetime,timezone
import json
from pathlib import Path
import re
from bench.common import ROOT,connect,write_report
from bench.archive_native_clone import snapshot,digest,retire_verified


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--manifest',required=True);args=p.parse_args()
    manifest=(ROOT/args.manifest).resolve();assert manifest.is_relative_to(ROOT/'reports/native-archives')
    report=json.loads(manifest.read_text(encoding='utf8'))
    assert report['kind']=='verified_native_clone_archive' and not report['passed'] and not report['original_removed']
    original=report['database'];restored=report['restore_test_database']
    assert re.fullmatch('lab_native_variant_[0-9a-f]{10}',original)
    assert re.fullmatch('lab_archive_verify_[0-9a-f]{10}',restored)
    archive=(ROOT/report['dump']).resolve();assert archive.is_relative_to(ROOT/'.runtime/native_archives')
    assert digest(archive)==report['dump_sha256']
    with connect('postgres',autocommit=True) as admin:
        assert admin.info.host in ('127.0.0.1','localhost','::1')
        assert Path(admin.execute('SHOW data_directory').fetchone()[0]).resolve()==(ROOT/'.runtime/pgdata').resolve()
        assert not admin.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall()
        assert json.loads(json.dumps(snapshot(original),default=str))==report['before'],'Original changed; retain all copies'
        assert admin.execute('SELECT 1 FROM pg_database WHERE datname=%s',(restored,)).fetchone()
        retire_verified(admin,restored)
    report['incomplete_restore_removed']=True
    report['cleanup_timestamp_utc']=datetime.now(timezone.utc).isoformat()
    report['cleanup_scope']='Only failed restore removed; full original native bytes/catalog/sequences independently rechecked against the pre-dump manifest; dump SHA verified; original and all versions retained.'
    write_report(manifest,report)
    print('Incomplete restore removed; exact original and checked dump retained',restored,flush=True)


if __name__=='__main__':main()
