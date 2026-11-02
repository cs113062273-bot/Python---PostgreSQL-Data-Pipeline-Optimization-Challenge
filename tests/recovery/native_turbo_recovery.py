"""Actual pre-commit and post-success server crashes of the turbo adapter."""
import os
import sys

from bench import native_turbo_probe as candidate
from bench.common import ROOT, write_report


def crash():
    from tests.recovery import native_copy_crash as driver
    os.environ.update({'PGHOST': '127.0.0.1', 'PGPORT': '55433', 'PGUSER': 'native_probe',
                       'CHALLENGE_DSN': 'host=127.0.0.1 port=55433 user=native_probe',
                       'CHALLENGE_PSQL': str(ROOT / '.runtime/postgresql/pgsql/bin/psql.exe')})
    frozen = candidate.source_identity()
    candidate.install()

    def recorded(ignored, value):
        assert candidate.source_identity() == frozen and value['execution_architecture']['safe_domain'] is True
        assert value['local_backend_diagnostics']['actual_binary_files'] == 2
        write_report('reports/recovery-native-turbo-server-crash.json', {**value, 'candidate_hook_sha256': frozen, 'explicit_wal_level': 'minimal',
                                                                       'official_eligibility_unproven': True})

    driver.write_report = recorded
    driver.main()


def durable():
    from tests.recovery import native_minimal_durable as driver
    driver.candidate = candidate
    driver.main(expected_files=2)


def main():
    mode = sys.argv.pop(1)
    {'crash': crash, 'durable': durable}[mode]()


if __name__ == '__main__':
    main()
