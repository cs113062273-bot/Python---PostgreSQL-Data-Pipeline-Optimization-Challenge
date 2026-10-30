"""Exercise independent main proof when the optional details package is absent."""
import uuid

from bench import native_c_lab as lab, native_main_shared_probe as candidate
from bench import native_details_probe as details, fast_transport_probe as transport
from bench.common import write_report
from bench.control import seed_initial_versions
from bench.run import execute
from pipeline.local_service import LocalService
from pipeline.bulk_fks import BulkFKMixin
from pipeline.bulk_indexes import BulkIndexMixin
from pipeline.guarded_normalization import GuardedNormalizationMixin
from pipeline.recipes import ORDER, destinations, bulk_unique_tables
from pipeline.upstream import PostgresAdminService
from tests.equivalence.oracle import snapshot, assert_exact, protected_schema_signature, sequence_signature
from tests.recovery.native_copy_atomic import source_sha


def main():
    database = 'lab_shared_fallback_' + uuid.uuid4().hex[:8]
    lab.fixture(database)
    seed_initial_versions(database)
    frozen = candidate.source_identity()
    candidate.install()
    original_details = details.addon_sql

    def unavailable():
        body = original_details()
        assert body.count(' AND trusted)') == 1
        return body.replace(' AND trusted)', ' AND trusted AND false)', 1)

    # Fault injection changes only the availability branch in the batch; the
    # installed package, catalog and pinned implementation remain untouched.
    details.addon_sql = unavailable

    class Service(BulkFKMixin, BulkIndexMixin, GuardedNormalizationMixin, LocalService):
        bulk_foreign_keys = guarded_normalization = True
        bulk_recipe_mode = 'guarded_copy'
        bulk_unique_tables = bulk_unique_tables('guarded_copy')

    checks = []
    service = None
    try:
        with lab.connect(database, autocommit=True) as c:
            for name, long_name in (('independent_main_proof_accepts', False), ('independent_main_proof_rejects', True)):
                if long_name:
                    c.execute("UPDATE public.raw_data SET raw=jsonb_set(raw,'{values,text_column_1}',to_jsonb(repeat('N',129))) WHERE raw_id=(SELECT min(raw_id) FROM public.raw_data)")
                execute(c, 'baseline', ['group001'])
                snapshot(c)
                c.execute('TRUNCATE ' + ','.join('public.' + t for t in ORDER) + ' RESTART IDENTITY')
                if c.execute("SELECT to_regclass('public.pgdm_table_row_counts')").fetchone()[0]:
                    c.execute("UPDATE public.pgdm_table_row_counts SET row_count=0 WHERE schema_name='public'")
                schema = protected_schema_signature(c)
                sequences = sequence_signature(c)
                source = source_sha(c)
                service = Service()
                timing = PostgresAdminService.create_cross_table_expansion_timing_report(database, 'public.raw_data', ['group001'], [d['table_name'] for d in destinations()])
                result = service.execute_cross_table_expansion(database, 'public.raw_data', ['group001'], destinations('guarded_copy'), timing_report=timing)
                diag = service._expansion_timing_local.local_backend_diagnostics
                affected = diag['typed_main_affected']
                assert (affected == 'fallback') is long_name, (name, diag)
                assert diag['actual_binary_files'] == (6 if long_name else 5), diag
                assert_exact(c)
                assert protected_schema_signature(c) == schema and sequence_signature(c) == sequences
                assert source_sha(c) == source
                for table in ORDER:
                    assert c.execute('SELECT count(*) FROM public.' + table).fetchone() == c.execute("SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s", (table,)).fetchone()
                assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone() == (None,)
                checks.append({'case': name, 'typed_main_affected': affected, 'binary_files': diag['actual_binary_files'], 'exact_outputs_source_schema_counters_passed': True})
                transport.close_sessions(service)
                print(name, 'PASS', flush=True)
                c.execute('TRUNCATE ' + ','.join('public.' + t for t in ORDER) + ' RESTART IDENTITY')
                c.execute("UPDATE public.pgdm_table_row_counts SET row_count=0 WHERE schema_name='public'")
    finally:
        if service is not None:
            transport.close_sessions(service)
        details.addon_sql = original_details
    assert candidate.source_identity() == frozen
    write_report('reports/semantics-native-main-shared-fallback.json', {
        'kind': 'shared_main_independent_proof_without_details_package', 'database': database,
        'candidate_hook_sha256': frozen, 'all_passed': True, 'cases': checks,
        'scope': 'Actual native service batches with availability-condition fault injection. No software/catalog changes, timing or crash/version qualification claim.'})


if __name__ == '__main__':
    main()
