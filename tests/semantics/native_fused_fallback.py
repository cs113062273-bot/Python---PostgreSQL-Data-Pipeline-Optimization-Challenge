"""Exact full-service fallback when either fused dependency/proof is unavailable."""
import uuid
import argparse
import importlib
import sys
from bench import native_fused_probe as fused, native_c_lab as lab, fast_transport_probe as fast
from bench.common import write_report
from bench.control import seed_initial_versions
from bench.run import execute
from pipeline.local_service import LocalService
from pipeline.bulk_fks import BulkFKMixin
from pipeline.bulk_indexes import BulkIndexMixin
from pipeline.guarded_normalization import GuardedNormalizationMixin
from pipeline.recipes import ORDER, destinations, bulk_unique_tables
from pipeline.checkpoints import source_digest
from pipeline.upstream import PostgresAdminService
from tests.equivalence.oracle import snapshot, assert_exact, protected_schema_signature, sequence_signature


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--variant', choices=['fused', 'stream', 'temp-stage'], default='fused')
    args, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0], *remaining]
    candidate = importlib.import_module('bench.' + {'fused': 'native_fused_probe', 'stream': 'native_stream_probe', 'temp-stage': 'native_temp_stage_probe'}[args.variant])
    database = 'lab_fused_fallback_' + uuid.uuid4().hex[:8]
    lab.fixture(database)
    seed_initial_versions(database)
    frozen = candidate.source_identity()
    candidate.install()
    main_module, details_module = fused.turbo.base, fused.turbo.details
    original_main, original_details = main_module.addon_sql, details_module.addon_sql
    unavailable = set()

    def availability(body, kind):
        if kind in unavailable:
            assert body.count(' AND trusted)') == 1
            return body.replace(' AND trusted)', ' AND trusted AND false)', 1)
        return body

    main_module.addon_sql = lambda: availability(original_main(), 'main')
    details_module.addon_sql = lambda: availability(original_details(), 'details')

    class Service(BulkFKMixin, BulkIndexMixin, GuardedNormalizationMixin, LocalService):
        bulk_foreign_keys = guarded_normalization = True
        bulk_recipe_mode = 'guarded_copy'
        bulk_unique_tables = bulk_unique_tables('guarded_copy')

    checks = []
    cases = [('main_unavailable', {'main'}, None, 3), ('details_unavailable', {'details'}, None, 6),
             ('both_unavailable', {'main', 'details'}, None, 6),
             ('main_proof_declines_long_name', set(), ('text_column_1', 'N' * 129), 3),
             ('details_proof_declines_code', set(), ('text_column_3', 'ABC'), 6)]
    with lab.connect(database, autocommit=True) as conn:
        raw_id, original = conn.execute('SELECT raw_id,raw FROM public.raw_data ORDER BY raw_id LIMIT 1').fetchone()
        from psycopg.types.json import Jsonb
        import copy
        for name, missing, edit, files in cases:
            unavailable.clear()
            unavailable.update(missing)
            raw = copy.deepcopy(original)
            if edit:
                raw['values'][edit[0]] = edit[1]
            conn.execute('UPDATE public.raw_data SET raw=%s WHERE raw_id=%s', (Jsonb(raw), raw_id))
            conn.execute('TRUNCATE ' + ','.join('public.' + t for t in ORDER) + ' RESTART IDENTITY')
            execute(conn, 'baseline', ['group001'])
            snapshot(conn)
            conn.execute('TRUNCATE ' + ','.join('public.' + t for t in ORDER) + ' RESTART IDENTITY')
            if conn.execute("SELECT to_regclass('public.pgdm_table_row_counts')").fetchone()[0]:
                conn.execute("UPDATE public.pgdm_table_row_counts SET row_count=0 WHERE schema_name='public'")
            schema, sequences, source = protected_schema_signature(conn), sequence_signature(conn), source_digest(conn, ['group001'])
            service = Service()
            timing = PostgresAdminService.create_cross_table_expansion_timing_report(database, 'public.raw_data', ['group001'], [d['table_name'] for d in destinations()])
            service.execute_cross_table_expansion(database, 'public.raw_data', ['group001'], destinations('guarded_copy'), timing_report=timing)
            diag = service._expansion_timing_local.local_backend_diagnostics
            assert diag['fused_counts'] == 'fallback' and diag['typed_main_affected'] == 'fallback'
            assert diag['actual_binary_files'] == files, (name, diag)
            fast.close_sessions(service)
            assert_exact(conn)
            assert protected_schema_signature(conn) == schema and sequence_signature(conn) == sequences
            assert source_digest(conn, ['group001']) == source
            for table in ORDER:
                assert conn.execute('SELECT count(*) FROM public.' + table).fetchone() == conn.execute(
                    "SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s", (table,)).fetchone()
            assert conn.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone() == (None,)
            checks.append({'case': name, 'fused_path_declined': True, 'exact_outputs_source_schema_counters_passed': True, 'binary_files': files})
            print(name, 'PASS', flush=True)
    assert candidate.source_identity() == frozen
    write_report('reports/semantics-native-' + args.variant + '-fallback.json', {
        'kind': 'fused_dependency_and_proof_fallback', 'database': database, 'candidate_hook_sha256': frozen,
        'all_passed': True, 'cases': checks})


if __name__ == '__main__':
    main()
