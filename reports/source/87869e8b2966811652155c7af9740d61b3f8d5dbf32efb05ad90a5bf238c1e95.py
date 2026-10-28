"""Empty-target storage reset under native durable minimal WAL, opt-in only.

The existing source/destination/function guards and original fallback remain.
Only a proven initially empty canonical destination set is reset. No sequence
restart, CASCADE, FREEZE, unlogged final table, fsync-off or extra data commit.
"""
import hashlib
from pathlib import Path
from bench import native_compact_probe as base


def source_identity():
    return hashlib.sha256((base.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def reset_sql():
    from pipeline.recipes import ORDER
    tables=','.join('public.'+t for t in ORDER)
    # No trigger, inherited table, RLS, extra constraint/index/type/default or
    # nonempty target can reach the current canonical binary-COPY safe domain.
    # The truncate privilege is additional and checked before selecting this
    # optimization, instead of changing acceptance for an ordinary INSERT role.
    privileges=' AND '.join("pg_catalog.has_table_privilege(current_user,'public."+t+"','TRUNCATE')" for t in ORDER)
    no_external_fk="NOT EXISTS(SELECT 1 FROM pg_catalog.pg_constraint WHERE contype='f' AND confrelid IN ("+','.join("'public."+t+"'::regclass" for t in ORDER)+") AND conrelid NOT IN ("+','.join("'public."+t+"'::regclass" for t in ORDER)+'))'
    return "DO $minimal_storage_reset$ BEGIN IF pg_catalog.current_setting('wal_level')='minimal' AND "+privileges+' AND '+no_external_fk+" THEN\nTRUNCATE "+tables+" CONTINUE IDENTITY RESTRICT;\nEND IF; END $minimal_storage_reset$;"


def install():
    base.install()
    import pipeline.bulk_fks as fks
    original=fks.prepare_sql
    fks.prepare_sql=lambda:original()+'\n'+reset_sql()


def main():
    frozen=source_identity();install()
    import bench.service_run as driver
    original=driver.write_report
    def recorded(path,value):
        assert source_identity()==frozen
        original(path,{**value,'native_minimal_hook_sha256':frozen,'official_eligibility_unproven':True,
                       'requires_server_configuration_for_wal_skip':True})
    driver.write_report=recorded;driver.main()


if __name__=='__main__':main()
