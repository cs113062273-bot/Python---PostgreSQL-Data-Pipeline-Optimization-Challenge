"""Retained 1M fused outputs: actual immediate restart, SHA-256 and full values."""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib
from pathlib import Path
import subprocess
import time
import psycopg
from psycopg import sql
from bench import native_c_lab as lab
from bench.common import ROOT, write_report, require_lab, environment
from pipeline.checkpoints import source_digest
from pipeline.recipes import ORDER
from tests.equivalence.oracle import protected_schema_signature, sequence_signature
from tests.recovery.bulk_dump import history


def state(database):
    with lab.connect(database, autocommit=True) as conn:
        require_lab(conn)
        assert conn.execute('SHOW wal_level').fetchone() == ('minimal',)
        assert all(conn.execute('SHOW ' + k).fetchone() == ('on',) for k in ('fsync', 'full_page_writes', 'synchronous_commit'))
        assert conn.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone() == (None,)
        scopes = [r[0] for r in conn.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        tables = {}
        for table in ORDER:
            digest = hashlib.sha256()
            with conn.cursor().copy('COPY (SELECT * FROM public.' + table + ' ORDER BY id_column_1) TO STDOUT (FORMAT BINARY)') as stream:
                for block in stream:
                    digest.update(block)
            count = conn.execute('SELECT count(*) FROM public.' + table).fetchone()[0]
            assert conn.execute("SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s", (table,)).fetchone() == (count,)
            tables[table] = {'rows': count, 'ordered_native_binary_sha256_including_ids': digest.hexdigest()}
        sequences = {}
        for schema, name in conn.execute("SELECT schemaname,sequencename FROM pg_sequences WHERE schemaname='public' ORDER BY 1,2"):
            sequences[schema + '.' + name] = conn.execute(sql.SQL('SELECT last_value,log_cnt,is_called FROM {}').format(sql.Identifier(schema, name))).fetchone()
        return {'tables': tables, 'source_sha256': source_digest(conn, scopes),
                'catalog': protected_schema_signature(conn), 'sequence_options': sequence_signature(conn),
                'sequence_reservations': sequences,
                'counters': conn.execute('SELECT to_jsonb(c) FROM public.pgdm_table_row_counts c ORDER BY schema_name,table_name').fetchall()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', required=True)
    parser.add_argument('--reference', required=True)
    parser.add_argument('--variant', choices=['fused', 'stream', 'temp-stage'], default='fused')
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    candidate = importlib.import_module('bench.' + {'fused': 'native_fused_probe', 'stream': 'native_stream_probe', 'temp-stage': 'native_temp_stage_probe'}[args.variant])
    output = (ROOT / args.output).resolve()
    assert output.is_relative_to(ROOT / 'reports') and not output.exists()
    frozen = candidate.source_identity()
    with lab.connect('postgres', autocommit=True) as conn:
        assert not conn.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid()").fetchall()
        settings = environment(conn)
    print('Hashing acknowledged native rows before immediate restart', flush=True)
    before = state(args.database)
    with lab.connect('postgres_data_manager', autocommit=True) as conn:
        versions = history(conn, args.database)
        assert sum(v[0]['version_code'] != '0000' for v in versions) == 6
    record = {'kind': 'retained_fused_1m_immediate_restart_audit', 'timestamp_utc': datetime.now(timezone.utc).isoformat(),
              'database': args.database, 'reference': args.reference, 'variant': args.variant, 'candidate_hook_sha256': frozen,
              'environment': settings, 'before': before, 'six_version_history': versions, 'passed': False,
              'scope': 'Actual immediate stop of verified private cluster after successful data and version commits. Ordered binary SHA-256 includes physical native identity values. Full logical six-table values/multiplicities compared after restart. Native sequence gaps limited to preexisting WAL reservations. Not a power-cut, PITR or post-data/pre-version saga test.'}
    write_report(output, record)
    with lab.connect('postgres', autocommit=True) as conn:
        assert not conn.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid()").fetchall()
    directory = (ROOT / '.runtime/native_pgdata').resolve()
    assert directory.is_relative_to(ROOT / '.runtime')
    binary = ROOT / '.runtime/postgresql/pgsql/bin/pg_ctl.exe'
    log = ROOT / '.runtime/fused-postcommit-restart.log'
    print('Immediate stop of the private cluster; recovering acknowledged rows', flush=True)
    with output.with_suffix('.pgctl.txt').open('x', encoding='utf8') as stream:
        subprocess.run([str(binary), '-D', str(directory), '-m', 'immediate', '-w', '-t', '60', 'stop'], stdout=stream, stderr=subprocess.STDOUT, check=True)
        subprocess.run([str(binary), '-D', str(directory), '-l', str(log), '-o',
                        '-h 127.0.0.1 -p 55433 -c wal_level=minimal -c max_wal_senders=0 -c fsync=on -c full_page_writes=on -c synchronous_commit=on',
                        '-W', 'start'], stdout=stream, stderr=subprocess.STDOUT, check=True)
    deadline = time.monotonic() + 180
    while True:
        try:
            with lab.connect('postgres', autocommit=True):
                break
        except psycopg.OperationalError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)
    after = state(args.database)
    assert {k: v for k, v in before.items() if k != 'sequence_reservations'} == {k: v for k, v in after.items() if k != 'sequence_reservations'}
    reservations = []
    for name, (old, logged, called) in before['sequence_reservations'].items():
        value, remaining, now_called = after['sequence_reservations'][name]
        assert old <= value <= old + logged and called == now_called
        reservations.append({'sequence': name, 'before': old, 'reserved_through': old + logged, 'after': value})
    with lab.connect('postgres_data_manager', autocommit=True) as conn:
        assert history(conn, args.database) == versions
    from bench.audit_ranked_result import exact_via_private_temp
    print('Comparing all six logical tables against retained unchanged original', flush=True)
    with lab.connect(args.database, autocommit=True) as conn, lab.connect(args.reference, autocommit=True) as reference:
        require_lab(reference)
        exact = exact_via_private_temp(conn, reference, comparison='tagged_count')
        assert len(exact) == 6
    assert candidate.source_identity() == frozen
    record.update({'passed': True, 'ordered_sha256_source_catalog_counters_versions_preserved': True,
                   'native_sequence_reservations_checked': reservations, 'exact_six_tables_after_recovery': exact})
    write_report(output, record)
    print('Fused 1M committed data, versions and exact six-table recovery PASS', flush=True)


if __name__ == '__main__':
    main()
