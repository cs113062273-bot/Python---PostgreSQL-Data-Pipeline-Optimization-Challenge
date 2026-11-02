"""Alternating fresh complete-service trials of best prior main versus shared main.

The retained official database is an output oracle only, never a timing control.
Report improvements only against the candidate measured in this same cohort.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import uuid

from psycopg import sql
from bench import native_c_lab as lab, native_main_probe as prior, native_main_shared_probe as shared
from bench.common import ROOT, write_report, environment
from bench.source_snapshot import snapshot
from pipeline.checkpoints import source_digest
from pipeline.recipes import ORDER


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--reference', required=True)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--output', required=True)
    args = p.parse_args()
    assert args.reference.startswith('lab_') and args.repeats > 0
    output = (ROOT / args.output).resolve()
    assert output.is_relative_to(ROOT / 'reports') and not output.exists()
    directory = output.with_suffix('')
    directory.mkdir(exist_ok=False)
    assert shutil.disk_usage(ROOT).free > (args.repeats * 2 + 1) * 3 * 1024**3
    env = {**os.environ, 'PGHOST': '127.0.0.1', 'PGPORT': str(lab.PORT), 'PGUSER': 'native_probe',
           'CHALLENGE_DSN': 'host=127.0.0.1 port=55433 user=native_probe',
           'CHALLENGE_PSQL': str(ROOT / '.runtime/postgresql/pgsql/bin/psql.exe')}
    for key in ('CHALLENGE_BACKEND_CPU_AFFINITY', 'CHALLENGE_RECORD_LEADER_CPU'):
        env.pop(key, None)

    def identity():
        return {'prior_hook': prior.source_identity(), 'shared_hook': shared.source_identity(),
                'driver': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                'service': hashlib.sha256((ROOT / 'bench/service_run.py').read_bytes()).hexdigest()}

    frozen = identity()
    report = {'kind': 'alternating_fresh_complete_candidate_comparison', 'official_score': False,
              'timestamp_utc': datetime.now(timezone.utc).isoformat(), 'implementation_identity': frozen,
              'source_snapshot': snapshot(),
              'oracle_database': args.reference, 'runs': [],
              'scope': 'Fresh FILE_COPY per trial from untouched generator input. Complete service including proofs, keys/FKs, counters, durable commit and six versions. Exact six-table oracle outside timer on every run. Retained official database supplies values only; its historic time is never reused. Local Windows, not Linux/SSH, scale or eligibility qualification.'}
    with lab.connect(args.reference, autocommit=True) as c:
        report['environment'] = environment(c)
        assert all(report['environment']['settings'][key] == 'on' for key in ('fsync', 'full_page_writes', 'synchronous_commit'))
        scopes = [r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        digest = source_digest(c, scopes)
    template = 'lab_shared_template_' + uuid.uuid4().hex[:10]
    report['template'] = template
    write_report(output, report)
    print('Generating untouched 1M template', template, flush=True)
    lab.fixture(template, rows=1000000)
    with lab.connect(template, autocommit=True) as c:
        assert source_digest(c, scopes) == digest
        assert all(c.execute('SELECT count(*) FROM public.' + table).fetchone() == (0,) for table in ORDER)
    report['source_digest'] = digest
    write_report(output, report)
    for repeat in range(args.repeats):
        modes = ('prior', 'shared') if repeat % 2 == 0 else ('shared', 'prior')
        for mode in modes:
            assert identity() == frozen
            database = 'lab_main_' + mode + '_' + uuid.uuid4().hex[:10]
            with lab.connect('postgres', autocommit=True) as admin:
                assert not admin.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall()
                admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {} STRATEGY FILE_COPY').format(sql.Identifier(database), sql.Identifier(template)))
            path = directory / (str(repeat + 1) + '-' + mode + '.json')
            module = 'bench.native_main_probe' if mode == 'prior' else 'bench.native_main_shared_probe'
            print('Complete service', repeat + 1, mode, database, flush=True)
            with path.with_suffix('.log').open('x', encoding='utf8') as log:
                child = subprocess.run([sys.executable, '-u', '-m', module, '--database', database,
                    '--mode', 'guarded_copy', '--reference', args.reference, '--output', str(path)],
                    cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
            assert child.returncode == 0, (mode, str(path.with_suffix('.log')))
            assert identity() == frozen
            result = json.loads(path.read_text(encoding='utf8'))
            for key in ('exact_six_tables', 'counter_reconciliation', 'protected_schema_equivalent',
                        'sequence_options_and_ownership_equivalent', 'workspace_namespace_restored'):
                assert result[key] is True, key
            assert len(result['versions']) == 6
            assert result['execution_architecture']['safe_domain'] is True
            diag = result['local_backend_diagnostics']
            assert diag['actual_binary_files'] == 2 and diag['typed_main_affected'] not in (None, 'fallback')
            assert diag['shared_fk_checked'] not in (None, 'fallback')
            assert result['destination_warmup_before_measured_run'] is False
            assert result['native_main_hook_sha256'] == frozen[mode + '_hook']
            assert result['environment']['settings'] == report['environment']['settings']
            with lab.connect(database, autocommit=True) as c:
                assert source_digest(c, scopes) == digest
                assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone() == (None,)
            row = {'repeat': repeat + 1, 'mode': mode, 'database': database,
                   'report': str(path.relative_to(ROOT)), 'wall_seconds': result['wall_seconds'],
                   'native_measured_seconds': sum(i['seconds'] for i in result['timing_report']['items'] if i.get('include_in_total', True)),
                   'six_exact_outputs_counters_versions_schema_source_passed': True}
            report['runs'].append(row)
            write_report(output, report)
            print(mode, row['wall_seconds'], 'seconds, exact gates PASS', flush=True)
    medians = {mode: statistics.median(r['wall_seconds'] for r in report['runs'] if r['mode'] == mode) for mode in ('prior', 'shared')}
    report['summary'] = {'median_wall_seconds': medians, 'saved_seconds': medians['prior'] - medians['shared'],
                         'reduction_vs_same_cohort_prior_candidate_percent': 100 * (1 - medians['shared'] / medians['prior']),
                         'official_90_percent_target_evaluated': False}
    report['all_passed'] = True
    write_report(output, report)
    print(json.dumps(report['summary'], indent=2), flush=True)


if __name__ == '__main__':
    main()
