"""Keep the 13-column timestamp path; share proofs and reuse per-call numerics.

Research adapter only. Every original main/details predicate is retained,
including separate fallback when the details package cannot be installed.
No validation result or data survives the transaction. Defaults are unchanged.
"""
import hashlib
import json
from pathlib import Path

from bench import native_main_probe as base, native_details_probe as details
from bench.common import ROOT


def package(kind='main'):
    if kind != 'memo':
        return base.package(kind)
    info = json.loads((ROOT / 'reports/native-details-memo-trusted-package.json').read_text())
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


def source_identity():
    # base.source_identity() consults details.package(), which install replaces.
    # Hash static dependency bytes and explicit packages instead, both before
    # and after installation, as the other composed research adapters do.
    from bench import native_minimal_probe, native_compact_sql, native_compile_cache_probe
    modules = (base, base.base, base.fk, details, base.compact, base.lab,
               native_minimal_probe, native_compact_sql, native_compile_cache_probe)
    body = base.compact.base.source_identity()
    body += ''.join(Path(m.__file__).read_text(encoding='utf8') for m in modules)
    body += Path(__file__).read_text(encoding='utf8')
    software = [package(), package('stamp'), package('memo'), base.base.package()]
    return hashlib.sha256((body + json.dumps(software, sort_keys=True)).encode()).hexdigest()


def main_proof(program):
    start = program.index('SELECT coalesce(bool_and(pipeline_domain_safe AND')
    end = program.index(' INTO main_usable FROM pgdm_pipeline_work.pipeline_normalized_input;', start)
    return program[start + len('SELECT '):end]


def install():
    # Bind the selected same-ABI details package before main_proof builds its
    # catalog guard; clean installations have no historical package receipts.
    details.package = lambda: package('memo')
    original_main = base.addon_sql
    proof = main_proof(original_main())
    original_details = details.addon_sql

    def shared_details():
        body = original_details()
        declaration = 'DECLARE native_details_usable boolean:=false; BEGIN'
        ending = ' INTO native_details_usable FROM pgdm_pipeline_work.pipeline_normalized_input;'
        assert body.count(declaration) == 1 and body.count(ending) == 1
        body = body.replace(declaration, 'DECLARE native_details_usable boolean:=false; main_data_safe boolean; BEGIN', 1)
        return body.replace(ending, ',' + proof +
                            ' INTO native_details_usable,main_data_safe FROM pgdm_pipeline_work.pipeline_normalized_input;\n'
                            'UPDATE pg_temp.pipeline_main_state SET data_safe=main_data_safe;', 1)

    def shared_main():
        body = original_main()
        scan = 'SELECT ' + proof + ' INTO main_usable FROM pgdm_pipeline_work.pipeline_normalized_input;'
        assert body.count(scan) == 1
        return body.replace(scan,
            'SELECT data_safe INTO main_usable FROM pg_temp.pipeline_main_state;\n'
            'IF main_usable IS NULL THEN\n' + scan + '\nEND IF;', 1)

    details.addon_sql = shared_details
    base.addon_sql = shared_main
    base.install()
    import pipeline.workspace as work
    original_prepare = work.prepare_sql

    def prepare(scopes):
        body = original_prepare(scopes)
        needle = 'affected bigint,previous_work_mem text) ON COMMIT DROP;'
        assert body.count(needle) == 1
        return body.replace(needle, 'affected bigint,previous_work_mem text,data_safe boolean) ON COMMIT DROP;', 1)

    work.prepare_sql = prepare


def main():
    frozen = source_identity()
    install()
    from bench import service_run as driver
    original = driver.write_report

    def recorded(path, value):
        assert source_identity() == frozen
        original(path, {**value, 'native_main_hook_sha256': frozen,
                       'trusted_main_package': package(), 'trusted_stamp_package': package('stamp'),
                       'trusted_details_package': package('memo'), 'official_eligibility_unproven': True})

    driver.write_report = recorded
    driver.main()


if __name__ == '__main__':
    main()
