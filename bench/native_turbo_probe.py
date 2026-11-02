"""Shared main with exact parser fast paths and contiguous native identities.

Research adapter only. The shared proofs, receiving guards, counters, keys/FKs,
durable commit and versions are unchanged. Identity numbers still start at the
sequence's native next value, and its exact final state is set in the same
ShareRowExclusive window. Any unprovable case uses per-row native nextval.
"""
import hashlib
import json
from pathlib import Path
import re

from bench import native_c_lab as lab, native_main_shared_probe as shared, native_main_probe as base, native_details_probe as details
from bench.common import ROOT

KINDS = {'main': 'main', 'stamp': 'stamp', 'memo': 'details', 'fk': 'fk'}
MANIFEST = 'CREATE TEMP TABLE "pgdm_cross_table_hash_manifest" ON COMMIT DROP AS'
MANIFEST_END = 'GROUP BY "raw_schema", "raw_hash";\n'
STAGE_ANALYZE = 'ANALYZE pgdm_pipeline_work.pipeline_normalized_input(raw_id,text_column_1,text_column_2,text_column_3);'
MANIFEST_SETTINGS = ('max_parallel_workers_per_gather', 'min_parallel_table_scan_size', 'parallel_setup_cost', 'parallel_tuple_cost')
STAGE = 'pgdm_pipeline_work.pipeline_normalized_input'
# One parallel aggregate replaces the proof, domain and dimension stage scans.
# Row-level AND/OR over these groups is exactly the per-row predicate.
STAGE_DIMS = '''DO $stage_dims$ BEGIN
CREATE TEMP TABLE pipeline_stage_summary(domain_safe boolean) ON COMMIT DROP;
IF EXISTS(SELECT 1 FROM pg_temp.pipeline_native_extension_state) THEN
  CREATE TEMP TABLE pipeline_stage_dims ON COMMIT DROP AS SELECT text_column_1,text_column_2,text_column_3,
    count(*) AS row_total,coalesce(sum(pipeline_axis_rows),0) AS axis_total,bool_and(pipeline_domain_safe) AS domain_safe,
    bit_or(pipeline_native_flags) AS native_flags FROM ''' + STAGE + ''' GROUP BY 1,2,3;
  INSERT INTO pg_temp.pipeline_stage_summary SELECT coalesce(bool_and(domain_safe),true) FROM pg_temp.pipeline_stage_dims;
END IF; END $stage_dims$;
'''
NATIVE_READY = '(coalesce((SELECT usable FROM pg_temp.pipeline_main_state),false) AND coalesce((SELECT usable FROM pg_temp.pipeline_details_state),false))'


def package(kind='main'):
    info = json.loads((ROOT / ('reports/native-turbo-' + KINDS[kind] + '-trusted-package.json')).read_text())
    source = (ROOT / info['source']).resolve()
    assert source.is_relative_to(ROOT / 'native')
    assert hashlib.sha256(source.read_bytes()).hexdigest() == info['source_sha256']
    for item in info['files']:
        path = (ROOT / item['path']).resolve()
        assert path.is_relative_to(ROOT / '.runtime/postgresql/pgsql')
        assert hashlib.sha256(path.read_bytes()).hexdigest() == item['sha256']
    for item in info['vendor_sources']['files']:
        assert hashlib.sha256((ROOT / item['path']).read_bytes()).hexdigest() == item['sha256']
    return info


shared.package = package
base.package = package
base.base.package = lambda: package('fk')


def rewrite(text):
    # Worker count and sample size change plans and statistics, never results.
    assert text.count(MANIFEST) == 1 and text.count(MANIFEST_END) == 1 and text.count(STAGE_ANALYZE) == 1
    names = ','.join("'" + name + "'" for name in MANIFEST_SETTINGS)
    before = ('CREATE TEMP TABLE pipeline_manifest_settings ON COMMIT DROP AS SELECT name,setting FROM pg_catalog.pg_settings WHERE name IN (' + names + ');\n'
              'SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;\n')
    after = ('DO $restore_manifest$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_manifest_settings LOOP '
             'PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_manifest$;\n')
    text = text.replace(MANIFEST, before + MANIFEST, 1).replace(MANIFEST_END, MANIFEST_END + after, 1)
    return summarize(text)


def once(text, old, new, count=1):
    assert text.count(old) == count, old
    return text.replace(old, new)


def summarize(text):
    assert text.count('DO $details_install$') == 1 and text.index('$main_install$') < text.index(STAGE_ANALYZE)
    text = text.replace('DO $details_install$', STAGE_DIMS + 'DO $details_install$', 1)
    text = once(text, 'pipeline_domain_safe AND (pipeline_native_flags&2)=0', 'domain_safe AND (native_flags&2)=0')
    text = once(text, 'AND count(*)<=128::bigint*1024*1024/96 AND coalesce(sum(pipeline_axis_rows),0)*10<=64::bigint*1024*1024',
                'AND coalesce(sum(row_total),0)<=128::bigint*1024*1024/96 AND coalesce(sum(axis_total),0)*10<=64::bigint*1024*1024')
    text, count = re.subn(r'pipeline_domain_safe AND\s+\(pipeline_native_flags&1\)=0', 'domain_safe AND (native_flags&1)=0', text)
    assert count == 2
    text = once(text, ' INTO native_details_usable,main_data_safe FROM ' + STAGE + ';', ' INTO native_details_usable,main_data_safe FROM pg_temp.pipeline_stage_dims;')
    text = once(text, ' INTO main_usable FROM ' + STAGE + ';', ' INTO main_usable FROM pg_temp.pipeline_stage_dims;')
    domain = re.search(r'CREATE TEMP TABLE pipeline_normalization_domain ON COMMIT DROP AS SELECT coalesce\(bool_and\(pipeline_domain_safe\),true\)(.*?) AS safe FROM ' + re.escape(STAGE) + ';', text)
    assert domain and text.count('coalesce(bool_and(pipeline_domain_safe),true)') == 1
    text = text.replace(domain.group(0), 'CREATE TEMP TABLE pipeline_normalization_domain ON COMMIT DROP AS SELECT coalesce((SELECT domain_safe FROM pg_temp.pipeline_stage_summary),'
                        '(SELECT coalesce(bool_and(pipeline_domain_safe),true) FROM ' + STAGE + '))' + domain.group(1) + ' AS safe;', 1)
    source = 'FROM ' + STAGE + ' AS source_row WHERE '
    for condition in ("(source_row.text_column_1) <> ''", "((source_row.text_column_3)) ~ '^[A-Z]{2}$'"):
        text = once(text, source + condition, 'FROM pg_temp.pipeline_stage_dims AS source_row WHERE ' + condition, 2)
    # Native loaders plan no join over these relations; SQL fallback keeps every ANALYZE.
    for statement in (STAGE_ANALYZE, 'ANALYZE public.table3;', 'ANALYZE public.table4;', 'ANALYZE public.table1(integer_column_1);'):
        text = once(text, statement, 'IF NOT ' + NATIVE_READY + ' THEN ' + statement + ' END IF;')
    # The stage writer drains faster alone; restore_parallel returns this setting.
    marker = '-- native compact source_sha256='
    assert text.count(marker) == 1
    text = text.replace(marker, 'SET LOCAL parallel_leader_participation=off;' + chr(10) + marker, 1)
    text, count = re.subn(r"(CREATE TEMP TABLE pipeline_parallel_settings ON COMMIT DROP AS SELECT name,setting FROM pg_settings WHERE name IN \([^)]*'enable_parallel_hash')\)", lambda match: match.group(1) + ",'parallel_leader_participation')", text)
    assert count == 1
    return text


def source_identity():
    body = shared.source_identity() + Path(__file__).read_text(encoding='utf8') + json.dumps(package('fk'), sort_keys=True)
    return hashlib.sha256(body.encode()).hexdigest()


def install():
    assert lab.COMPACT_FIELDS.count(('pipeline_stamp', 'text')) == 1
    lab.COMPACT_FIELDS[lab.COMPACT_FIELDS.index(('pipeline_stamp', 'text'))] = ('pipeline_native_flags', 'int2')
    original_details, original_main = details.addon_sql, base.addon_sql

    def bounded_details():
        # The one-pass buffer holds every validated frame, so bound it before loading.
        body = original_details()
        needle = 'AND count(*)<=128::bigint*1024*1024/96'
        code = "(text_column_3 IS NULL OR text_column_3='' OR text_column_3 ~ '^[A-Z]{2}$')"
        assert body.count(needle) == 1 and body.count(code) == 1
        body = body.replace(code, '(pipeline_native_flags&2)=0', 1)
        return body.replace(needle, needle + ' AND coalesce(sum(pipeline_axis_rows),0)*10<=64::bigint*1024*1024', 1)

    def flagged_main():
        # A ^[A-Z]{2}$ match is exactly two ASCII letters, so only the name bound can fail.
        body, count = re.subn(r"\(text_column_1 IS NULL OR octet_length\(text_column_1\)<=128\) AND\s+"
                              r"\(text_column_2 IS NULL OR NOT\(text_column_2 ~ '\^\[A-Z\]\{2\}\$'\) OR\s+"
                              r"\(octet_length\(text_column_2\)=2 AND ascii\(left\(text_column_2,1\)\) BETWEEN 65 AND 90 AND ascii\(right\(text_column_2,1\)\) BETWEEN 65 AND 90\)\)",
                              lambda match: '(pipeline_native_flags&1)=0', original_main())
        assert count == 1
        return body

    details.addon_sql = bounded_details
    base.addon_sql = flagged_main
    shared.install()
    from bench import server_copy_probe as server
    original_rewrite = server.rewrite
    server.rewrite = lambda text, cwd: rewrite(original_rewrite(text, cwd))


def main():
    frozen = source_identity()
    install()
    from bench import service_run as driver
    original = driver.write_report

    def recorded(path, value):
        assert source_identity() == frozen
        original(path, {**value, 'native_main_hook_sha256': frozen,
                       'trusted_main_package': package(), 'trusted_stamp_package': package('stamp'),
                       'trusted_details_package': package('memo'), 'trusted_fk_package': package('fk'),
                       'official_eligibility_unproven': True})

    driver.write_report = recorded
    driver.main()


if __name__ == '__main__':
    main()
