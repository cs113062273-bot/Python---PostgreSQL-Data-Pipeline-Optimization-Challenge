"""Actual fused-loader failures, full transaction rollback and exact retry."""
import re
import types
import argparse
import importlib
import sys
from bench import fast_transport_probe as fast
from bench.common import write_report
from tests.recovery import refined_copy_atomic as driver


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--variant', choices=['fused', 'stream', 'temp-stage'], default='fused')
    args, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0], *remaining]
    candidate = importlib.import_module('bench.' + {'fused': 'native_fused_probe', 'stream': 'native_stream_probe', 'temp-stage': 'native_temp_stage_probe'}[args.variant])
    frozen = candidate.source_identity()
    driver.refined = types.SimpleNamespace(source_identity=candidate.source_identity, install=candidate.install)
    driver.transport = types.SimpleNamespace(install=lambda: None, close_sessions=fast.close_sessions)

    def marker(point, text):
        if point in ('before_main', 'after_main', 'before_details', 'after_details'):
            matches = re.findall(r'UPDATE pg_temp.pipeline_fused_state SET executed=true,counts=pgdm_pipeline_work.main_load\([\s\S]*?\);', text)
        elif point == 'after_main_receipt':
            matches = [line for line in text.splitlines() if line.startswith("INSERT INTO pg_temp.pipeline_copy_receipts SELECT 'table1',affected")]
        else:
            matches = [line for line in text.splitlines() if line.startswith(' CROSS JOIN LATERAL (SELECT counts.table_name) names;')]
        assert len(matches) == 1, (point, matches)
        return matches[0]

    points = ('after_projection', 'before_main', 'after_main', 'after_main_receipt',
              'after_details_receipts', 'before_fk_restore', 'after_fk_restore', 'before_commit')
    cases = [(point, kind) for point in points for kind in ('server_exception', 'backend_termination')]
    cases += [('before_details', 'bad_frame'), ('before_main', 'identity_exhaustion'),
              ('copy_from_table3', 'missing_file'), ('copy_to_table3', 'bad_binary')]

    def recorded(ignored, value):
        assert candidate.source_identity() == frozen
        if args.variant == 'temp-stage':
            value = {**value, 'scope': value['scope'].replace('only disposable normalized cache is unlogged', 'disposable normalized cache is temporary')}
        if value['all_passed']:
            diag = value['retry_local_backend_diagnostics']
            assert diag['actual_binary_files'] == 2 and diag['fused_counts'] not in (None, 'fallback')
            assert diag['shared_fk_checked'] not in (None, 'fallback')
        write_report('reports/recovery-native-' + args.variant + '-atomic.json', {
            **value, 'kind': 'fused_main_details_atomic_faults', 'candidate_hook_sha256': frozen,
            'fused_boundary_note': 'Main and details execute in one native invocation; after_main is after all four COPY operations.',
            'official_eligibility_unproven': True})

    driver.write_report = recorded
    stage = 'pg_temp.pipeline_native_temp_input_v1' if args.variant == 'temp-stage' else 'pgdm_pipeline_work.pipeline_normalized_input'
    driver.main(cases_override=cases, marker_override=marker, stage_relation=stage)


if __name__ == '__main__':
    main()
