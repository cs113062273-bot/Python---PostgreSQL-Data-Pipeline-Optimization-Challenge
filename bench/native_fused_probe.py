"""Research-only single stage scan for main and all three detail COPY loaders.

Actual native main IDs are collected inside the same C invocation; no parent
map or second stage scan is needed. All guards, native COPY checks, final key
builds, full FK validation, counters, durable commit and versions remain.
"""
import hashlib
import json
from pathlib import Path
import re

from bench import native_turbo_probe as turbo
from bench.common import ROOT

ORIGINAL_PACKAGE = turbo.package


def package(kind='main'):
    if kind != 'main':
        return ORIGINAL_PACKAGE(kind)
    info = json.loads((ROOT / 'reports/native-fused-trusted-package.json').read_text())
    for item in info['source_files'] + info['files'] + info['vendor_sources']['files']:
        path = (ROOT / item['path']).resolve()
        assert path.is_relative_to(ROOT)
        assert hashlib.sha256(path.read_bytes()).hexdigest() == item['sha256']
    sources = ({'source_files': info['source_files'], 'configuration': info['configuration']}
               if 'configuration' in info else info['source_files'])
    assert hashlib.sha256(json.dumps(sources, sort_keys=True).encode()).hexdigest() == info['source_sha256']
    return info


turbo.package = package
turbo.shared.package = package
turbo.base.package = package


def source_identity():
    return hashlib.sha256((turbo.source_identity() + Path(__file__).read_text(encoding='utf8')
                           + json.dumps(package(), sort_keys=True)).encode()).hexdigest()


def member_guard():
    from utils import sql_literal as lit
    old, new = turbo.details.package(), package()
    oldlib = next(Path(x['path']).stem for x in old['files'] if x['path'].endswith('.dll'))
    newlib = next(Path(x['path']).stem for x in new['files'] if x['path'].endswith('.dll'))
    body = turbo.details.member_guard()
    for before, after in [(lit(old['extension']), lit(new['extension'])),
                          (lit('$libdir/' + oldlib), lit('$libdir/' + newlib)),
                          ("'details_load'", "'main_load'"), ('challenge_details_ingest', 'challenge_fused_ingest'),
                          ("'details_identity'", "'main_identity'"), ('challenge_details_identity', 'challenge_fused_identity'),
                          ("'bigint[]'::regtype,6,", "'bigint[]'::regtype,7,"),
                          ('wanted.args=6', 'wanted.args=7'), ("::text||' ',5)", "::text||' ',6)")]:
        assert before in body, before
        body = body.replace(before, after)
    return body


def rewrite(text):
    from pipeline import bulk_indexes as indexes
    call = "UPDATE pg_temp.pipeline_main_state SET affected=pgdm_pipeline_work.main_load('pgdm_pipeline_work.pipeline_normalized_input','public.table1','public.table3');"
    ready = "SELECT :'pipeline_main_ready'::boolean AND (SELECT count(*)"
    old_details = "counts=pgdm_pipeline_work.details_load('pgdm_pipeline_work.pipeline_normalized_input','public.table1','public.table5','public.table6','public.table2','public.table4')"
    assert text.count(call) in (0, 1) and text.count(old_details) in (0, 1)
    assert text.count(call) + text.count(old_details) > 0
    fused = """
UPDATE pg_temp.pipeline_fused_state SET executed=true,counts=pgdm_pipeline_work.main_load(
 'pgdm_pipeline_work.pipeline_normalized_input','public.table1','public.table3','public.table5','public.table6','public.table2','public.table4');
UPDATE pg_temp.pipeline_main_state SET affected=(SELECT counts[1] FROM pg_temp.pipeline_fused_state);
UPDATE pg_temp.pipeline_details_state SET counts=(SELECT counts[2:4] FROM pg_temp.pipeline_fused_state);
"""
    if call in text:
        assert text.count(ready) == 1
        text = text.replace(ready, "SELECT :'pipeline_main_ready'::boolean AND :'pipeline_details_ready'::boolean AND (SELECT count(*)", 1)
        prepare = '\n'.join(indexes.prepare_mode_sql('guarded_copy', table) for table in ('table5', 'table6', 'table2'))
        text = text.replace(call, prepare + '\n' + fused, 1)
    if old_details in text:
        text = text.replace(old_details, 'counts=CASE WHEN (SELECT executed FROM pg_temp.pipeline_fused_state) THEN counts ELSE '
                            + old_details.removeprefix('counts=') + ' END', 1)
    return text


def install():
    turbo.base.main_member_guard = member_guard
    turbo.install()
    from pipeline import workspace as work, native_copy as native
    from bench import server_copy_probe as server
    previous_prepare, previous_cleanup = work.prepare_sql, work.cleanup_sql
    previous_copy, previous_observed, previous_run = native.copy_sql, server.ORIGINAL_RUN, server.run
    work.prepare_sql = lambda scopes: ("CREATE TEMP TABLE pipeline_fused_state(executed boolean NOT NULL,counts bigint[] CHECK(array_length(counts,1)=4)) ON COMMIT DROP;"
                                      " INSERT INTO pg_temp.pipeline_fused_state VALUES(false,NULL);\n" + previous_prepare(scopes))
    work.cleanup_sql = lambda: """SELECT coalesce(array_to_string(counts,','),'fallback') AS pipeline_fused_counts FROM pg_temp.pipeline_fused_state
\gset
\echo PGDM_FUSED_COUNTS=:pipeline_fused_counts
""" + previous_cleanup()
    # Generate the complete new ABI before transport/capture. Captured batches
    # remain directly replayable through ordinary psql as well as server COPY.
    native.copy_sql = lambda statement, table: (rewrite(previous_copy(statement, table)) if table in ('table1', 'table5')
                                                else previous_copy(statement, table))

    def observed(*args, **kwargs):
        result = previous_observed(*args, **kwargs)
        matches = re.findall(r'^PGDM_FUSED_COUNTS=(fallback|[0-9]+(?:,[0-9]+){3})\s*$', result.stdout, re.M)
        # The same transport also executes unchanged baseline/metadata commands.
        # Full-service candidate receipts independently require a real fused tag.
        assert result.returncode != 0 or len(matches) <= 1
        result.fused_counts = matches[0] if matches else None
        result.stdout = re.sub(r'^PGDM_FUSED_COUNTS=(?:fallback|[0-9]+(?:,[0-9]+){3})\r?\n', '', result.stdout, flags=re.M)
        return result

    def diagnosed(*args, **kwargs):
        result = previous_run(*args, **kwargs)
        result.local_backend_diagnostics['fused_counts'] = getattr(result, 'fused_counts', None)
        return result

    server.ORIGINAL_RUN = observed
    server.run = diagnosed


def main():
    frozen = source_identity()
    install()
    from bench import service_run as driver
    previous = driver.write_report

    def recorded(path, value):
        assert source_identity() == frozen
        previous(path, {**value, 'native_main_hook_sha256': frozen, 'trusted_main_package': package(),
                        'trusted_stamp_package': package('stamp'), 'trusted_details_package': package('memo'),
                        'trusted_fk_package': package('fk'), 'official_eligibility_unproven': True})

    driver.write_report = recorded
    driver.main()


if __name__ == '__main__':
    main()
