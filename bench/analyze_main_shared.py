"""Audit shared-main receipts and retain all alternating candidate timings."""
import hashlib
import json
from pathlib import Path
import statistics

from bench import native_main_shared_probe as candidate
from bench.common import ROOT, write_report


def read(name):
    path = ROOT / 'reports' / name
    value = json.loads(path.read_text(encoding='utf8'))
    return value, {'report': str(path.relative_to(ROOT)), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}


def main():
    frozen = candidate.source_identity()
    qualification = []
    for name, count, rows_key, identity_key in (
        ('semantics-native-main-shared-native.json', 121, 'cases', 'slim_copy_hook_sha256'),
        ('semantics-native-main-shared-boundaries-82.json', 82, 'cases', 'slim_copy_hook_sha256'),
        ('semantics-native-main-shared-text-112.json', 112, 'cases', 'slim_copy_hook_sha256'),
        ('semantics-native-main-shared-layouts-11-v2.json', 11, 'cases', 'slim_copy_hook_sha256'),
        ('recovery-native-main-shared-atomic.json', 24, 'tests', 'candidate_hook_sha256'),
        ('semantics-native-main-shared-fallback.json', 2, 'cases', 'candidate_hook_sha256')):
        value, receipt = read(name)
        assert value['all_passed'] is True and value[identity_key] == frozen
        assert len(value[rows_key]) == count and not value.get('mismatches')
        qualification.append({**receipt, 'passed_cases': count})
    cohort, cohort_receipt = read('native-main-shared-fresh-1m-20261030.json')
    assert cohort['all_passed'] is True and len(cohort['runs']) == 6
    assert cohort['implementation_identity']['shared_hook'] == frozen
    rounds = []
    settings = None
    for repeat in (1, 2, 3):
        pair = {r['mode']: r for r in cohort['runs'] if r['repeat'] == repeat}
        assert set(pair) == {'prior', 'shared'}
        for mode, run in pair.items():
            child = json.loads((ROOT / run['report']).read_text(encoding='utf8'))
            assert child['native_main_hook_sha256'] == cohort['implementation_identity'][mode + '_hook']
            assert len(child['versions']) == 6 and len(child['exact_comparison_details']) == 6
            assert child['exact_six_tables'] and child['counter_reconciliation']
            assert child['protected_schema_equivalent'] and child['sequence_options_and_ownership_equivalent']
            assert child['workspace_namespace_restored'] and not child['destination_warmup_before_measured_run']
            assert child['local_backend_diagnostics']['actual_binary_files'] == 2
            assert len(child['local_backend_diagnostics']['shared_fk_checked'].split(',')) == 6
            if settings is None:
                settings = child['environment']['settings']
            assert child['environment']['settings'] == settings
        old, new = pair['prior']['wall_seconds'], pair['shared']['wall_seconds']
        rounds.append({'repeat': repeat, 'order': [r['mode'] for r in cohort['runs'] if r['repeat'] == repeat],
                       'prior_seconds': old, 'shared_seconds': new,
                       'saved_seconds': old - new, 'reduction_percent': 100 * (1 - new / old)})
    restored, restored_receipt = read('native-main-shared-wal-restored-20261030.json')
    assert restored['passed'] and restored['after']['wal_level'] == 'replica'
    assert all(restored['after'][k] == 'on' for k in ('fsync', 'full_page_writes', 'synchronous_commit'))
    report = {'kind': 'audited_shared_main_candidate_comparison', 'candidate_hook_sha256': frozen,
              'all_receipt_checks_passed': True, 'qualification': qualification,
              'cohort_receipt': cohort_receipt, 'rounds': rounds, 'summary': cohort['summary'],
              'all_three_rounds_faster': all(r['saved_seconds'] > 0 for r in rounds),
              'ranges_seconds': {mode: [min(r['wall_seconds'] for r in cohort['runs'] if r['mode'] == mode),
                                       max(r['wall_seconds'] for r in cohort['runs'] if r['mode'] == mode)] for mode in ('prior', 'shared')},
              'paired_reduction_median_percent': statistics.median(r['reduction_percent'] for r in rounds),
              'wal_restored': restored_receipt, 'official_90_percent_target_proven': False,
              'limitations': ['Large timing drift: prior 15.94-33.15s, shared 14.43-24.25s. Descriptive single-cohort gains, not a guaranteed effect size or robust repeatability.',
                              'Official baseline not rerun in this cohort; historical 89.791059% best remains separate.',
                              'No new server-crash/dump-restore, Linux/SSH, 300M or official eligibility qualification; defaults unchanged.']}
    write_report(ROOT / 'reports/native-main-shared-analysis-20261030.json', report)
    print(json.dumps({'summary': report['summary'], 'rounds': rounds, 'all_receipt_checks_passed': True}, indent=2))


if __name__ == '__main__':
    main()
