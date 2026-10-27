"""Isolated native index-memory/worker experiment; no default or durability change."""
import hashlib
from pathlib import Path
import sys


def source_identity():
    return hashlib.sha256(Path(__file__).read_text(encoding='utf-8').encode()).hexdigest()


def install():
    import pipeline.bulk_indexes as module
    original=module.restore_sql
    def restored(table,*,mode=None):
        body=original(table,mode=mode)
        if mode!='guarded_copy':return body
        body=body.replace("'128MB'","'512MB'")
        return '-- lab index tuning source_sha256='+source_identity()+'''\nDO $index_probe_save$ BEGIN
          CREATE TEMP TABLE pipeline_index_probe_settings(value text) ON COMMIT DROP;
          INSERT INTO pg_temp.pipeline_index_probe_settings SELECT current_setting('max_parallel_maintenance_workers');
          PERFORM set_config('max_parallel_maintenance_workers','8',true);
        END $index_probe_save$;
        '''+body+'''
        DO $index_probe_restore$ BEGIN
          PERFORM set_config('max_parallel_maintenance_workers',(SELECT value FROM pg_temp.pipeline_index_probe_settings),true);
          DROP TABLE pg_temp.pipeline_index_probe_settings;
        END $index_probe_restore$;'''
    module.restore_sql=restored


def main():
    if len(sys.argv)<2 or sys.argv[1] not in ('service','semantics'):
        raise SystemExit('Use service or semantics followed by driver arguments')
    action=sys.argv.pop(1);install()
    if action=='service':
        from bench.service_run import main as driver
    else:
        from tests.semantics.native_copy import main as driver
    driver()


if __name__=='__main__':main()
