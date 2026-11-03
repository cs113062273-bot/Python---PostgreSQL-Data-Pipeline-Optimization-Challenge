"""Compare unchanged original and a native candidate on fresh input copies."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import uuid

from psycopg import sql
from bench import native_c_lab as lab
from bench.common import ROOT, write_report, environment
from pipeline.checkpoints import source_digest
from pipeline.recipes import ORDER


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--rows', type=int, choices=[64, 100000, 1000000], default=100000)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--candidate', choices=['temp-stage'], default='temp-stage')
    p.add_argument('--template', help='Retained disposable untouched input template; row count and empty outputs are checked')
    p.add_argument('--leader-cpu', type=int, help='Separate diagnostic: identical explicit leader affinity in both modes')
    p.add_argument('--output', required=True)
    args = p.parse_args()
    if args.repeats < 1:
        p.error('repeats must be positive')
    if args.leader_cpu is not None and not 0 <= args.leader_cpu < os.cpu_count():
        p.error('leader-cpu is outside the available logical processors')
    candidate_module = {'turbo': 'bench.native_turbo_probe', 'shared': 'bench.native_main_shared_probe',
                        'stream': 'bench.native_stream_probe', 'temp-stage': 'bench.native_temp_stage_probe'}[args.candidate]
    candidate = importlib.import_module(candidate_module)
    candidate_mode = args.candidate
    output = (ROOT / args.output).resolve()
    assert output.is_relative_to(ROOT / 'reports') and not output.exists()
    directory = output.with_suffix('')
    directory.mkdir(exist_ok=False)
    required_free = args.rows * 7000 * (args.repeats * 2 + 1) + 2 * 1024**3
    assert shutil.disk_usage(ROOT).free > required_free, 'Insufficient room for retained fresh databases and verification'
    env = {**os.environ, 'PGHOST': '127.0.0.1', 'PGPORT': str(lab.PORT), 'PGUSER': 'native_probe',
           'CHALLENGE_DSN': 'host=127.0.0.1 port=55433 user=native_probe',
           'CHALLENGE_PSQL': str(ROOT / '.runtime/postgresql/pgsql/bin/psql.exe')}
    for key in ('CHALLENGE_BACKEND_CPU_AFFINITY', 'CHALLENGE_RECORD_LEADER_CPU'):
        env.pop(key, None)
    if args.leader_cpu is not None:
        env['CHALLENGE_BACKEND_CPU_AFFINITY'] = str(args.leader_cpu)
    frozen = candidate.source_identity()
    report = {'kind': 'portable_fresh_original_candidate_pairs', 'official_score': False,
              'candidate_implementation': candidate_mode, 'official_eligibility_unproven': True,
              'timestamp_utc': datetime.now(timezone.utc).isoformat(), 'rows': args.rows,
              'diagnostic_leader_cpu': args.leader_cpu,
              'candidate_hook_sha256': frozen, 'runs': [], 'pairs': [],
              'driver_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'scope': 'Windows local service wall and separately native accounted total. Fresh untouched generator input and FILE_COPY per run, alternating AB/BA order. Preflight, identities, counters, all keys/FKs, commit and six versions included. Exact six-table comparison outside timer every candidate run. No official score or Linux/SSH claim.'}
    if args.leader_cpu is not None:
        report['scope'] += ' Separate diagnostic with the same explicitly pinned SQL leader in original and candidate; workers unchanged, pin setup included. Not default OS scheduling.'
    driver_archive = ROOT / 'reports/source' / (report['driver_sha256'] + '.py')
    driver_archive.parent.mkdir(parents=True, exist_ok=True)
    if driver_archive.exists():
        assert driver_archive.read_bytes() == Path(__file__).read_bytes()
    else:
        driver_archive.write_bytes(Path(__file__).read_bytes())
    report['driver_source_archive'] = str(driver_archive.relative_to(ROOT))
    with lab.connect('postgres', autocommit=True) as c:
        report['environment'] = environment(c)
        assert all(report['environment']['settings'][key] == 'on' for key in ('fsync', 'full_page_writes', 'synchronous_commit'))
    write_report(output, report)
    template = args.template or 'lab_portable_template_' + uuid.uuid4().hex[:10]
    if args.template:
        assert template.startswith('lab_')
        print('Validating retained untouched input:', template, flush=True)
    else:
        print('Generating', args.rows, 'unchanged input rows:', template, flush=True)
        lab.fixture(template, rows=args.rows)
    with lab.connect(template, autocommit=True) as c:
        from bench.common import require_lab
        require_lab(c)
        assert c.execute('SELECT count(*) FROM public.raw_data').fetchone() == (args.rows,)
        scopes = [r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        digest = source_digest(c, scopes)
        assert all(c.execute('SELECT count(*) FROM public.' + t).fetchone() == (0,) for t in ORDER)
    report.update({'template': template, 'source_digest': digest})
    write_report(output, report)

    def run(mode, database, label, reference):
        assert candidate.source_identity() == frozen
        with lab.connect('postgres', autocommit=True) as admin:
            assert not admin.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(), 'Close other benchmark jobs'
        path = directory / (label + '.json')
        command = [sys.executable, '-u', '-m', 'bench.service_run' if mode == 'official' else candidate_module,
                   '--database', database, '--mode', 'baseline' if mode == 'official' else 'guarded_copy', '--output', str(path)]
        command += ['--establish-reference'] if mode == 'official' else ['--reference', reference]
        print('Starting', label, database, flush=True)
        with path.with_suffix('.log').open('x', encoding='utf8') as log:
            result = subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        assert result.returncode == 0, 'See ' + str(path.with_suffix('.log'))
        assert candidate.source_identity() == frozen
        child = json.loads(path.read_text(encoding='utf8'))
        for field in ('counter_reconciliation', 'protected_schema_equivalent', 'sequence_options_and_ownership_equivalent', 'workspace_namespace_restored'):
            assert child[field] is True, field
        assert len(child['versions']) == 6 and child['destination_warmup_before_measured_run'] is False
        assert child['environment']['settings'] == report['environment']['settings']
        if args.leader_cpu is not None:
            diagnostics = child['local_backend_diagnostics']
            affinity = diagnostics.get('leader_affinity_diagnostic', diagnostics)
            assert affinity['requested_cpu'] == args.leader_cpu and affinity['measured_affinity'] == [args.leader_cpu]
        if mode == 'official':
            assert child['unmodified_reference_established'] is True
        else:
            assert child['exact_six_tables'] is True and len(child['exact_comparison_details']) == 6
            assert child['native_main_hook_sha256'] == frozen
            assert child['execution_architecture']['safe_domain'] is True
            diag = child['local_backend_diagnostics']
            assert diag['actual_binary_files'] == 2
            assert diag['typed_main_affected'] not in (None, 'fallback')
            assert len(diag['shared_fk_checked'].split(',')) == 6
        with lab.connect(database, autocommit=True) as c:
            assert source_digest(c, scopes) == digest
            assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone() == (None,)
            if mode != 'official':
                assert int(diag['typed_main_affected']) == c.execute('SELECT count(*) FROM public.table1').fetchone()[0]
                for kind in ('main', 'stamp', 'memo'):
                    assert not c.execute('SELECT 1 FROM pg_extension WHERE extname=%s', (candidate.package(kind)['extension'],)).fetchall()
                if candidate_mode != 'shared':
                    assert not c.execute('SELECT 1 FROM pg_extension WHERE extname=%s', (candidate.package('fk')['extension'],)).fetchall()
        row = {'mode': mode, 'database': database, 'report': str(path.relative_to(ROOT)),
               'wall_seconds': child['wall_seconds'],
               'native_measured_seconds': sum(i['seconds'] for i in child['timing_report']['items'] if i.get('include_in_total', True)),
               'outputs_counters_versions_catalog_source_passed': True}
        report['runs'].append(row)
        write_report(output, report)
        print(label, round(row['wall_seconds'], 6), 'seconds; all gates PASS', flush=True)
        return row

    # Retain the first measured original as the exact-output oracle for BA
    # rounds. All measured destinations are still fresh FILE_COPY clones.
    oracle = None
    for repeat in range(args.repeats):
        names = {mode: 'lab_portable_' + mode.replace('-', '_') + '_' + uuid.uuid4().hex[:10] for mode in ('official', candidate_mode)}
        with lab.connect('postgres', autocommit=True) as admin:
            for name in names.values():
                admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {} STRATEGY FILE_COPY').format(sql.Identifier(name), sql.Identifier(template)))
        order = ('official', candidate_mode) if repeat % 2 == 0 else (candidate_mode, 'official')
        pair = {}
        for mode in order:
            reference = names['official'] if 'official' in pair else oracle
            pair[mode] = run(mode, names[mode], str(repeat + 1) + '-' + mode, reference)
            if mode == 'official' and oracle is None:
                oracle = names['official']
        old, new = pair['official'], pair[candidate_mode]
        report['pairs'].append({'repeat': repeat + 1, 'order': list(order),
            'official_wall_seconds': old['wall_seconds'], 'candidate_wall_seconds': new['wall_seconds'],
            'wall_reduction_percent': 100 * (1 - new['wall_seconds'] / old['wall_seconds']),
            'native_accounted_reduction_percent': 100 * (1 - new['native_measured_seconds'] / old['native_measured_seconds'])})
        write_report(output, report)
    medians = {mode: statistics.median(r['wall_seconds'] for r in report['runs'] if r['mode'] == mode) for mode in ('official', candidate_mode)}
    report['summary'] = {'median_wall_seconds': medians, 'median_wall_reduction_percent': 100 * (1 - medians[candidate_mode] / medians['official']),
                         'every_pair_at_least_90_percent': all(p['wall_reduction_percent'] >= 90 for p in report['pairs']),
                         'every_pair_at_least_95_percent': all(p['wall_reduction_percent'] >= 95 for p in report['pairs']),
                         'official_score': False, 'performance_reproduced_on_this_pc_only': True}
    report['all_passed'] = True
    write_report(output, report)
    print(json.dumps(report['summary'], indent=2), flush=True)


if __name__ == '__main__':
    main()
