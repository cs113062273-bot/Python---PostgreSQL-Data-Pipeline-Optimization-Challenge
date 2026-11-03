"""Private-server precommit crash and exact retry for a fused composition."""
import argparse
import importlib
import os
import sys
from bench.common import ROOT, write_report
from tests.recovery import native_copy_crash as driver


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--variant', choices=['fused', 'stream', 'temp-stage'], default='fused')
    args, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0], *remaining]
    for flag in ('--fresh-service-retry', '--pause-after-fk-validation'):
        if flag not in sys.argv:
            sys.argv.append(flag)
    candidate = importlib.import_module('bench.' + {'fused': 'native_fused_probe', 'stream': 'native_stream_probe', 'temp-stage': 'native_temp_stage_probe'}[args.variant])
    os.environ.update(PGHOST='127.0.0.1', PGPORT='55433', PGUSER='native_probe',
                      CHALLENGE_DSN='host=127.0.0.1 port=55433 user=native_probe',
                      CHALLENGE_PSQL=str(ROOT / '.runtime/postgresql/pgsql/bin/psql.exe'))
    frozen = candidate.source_identity()
    candidate.install()

    def recorded(ignored, value):
        assert candidate.source_identity() == frozen
        assert value['passed'] and value['six_validated_fks_proven_in_data_transaction']
        assert value['execution_architecture']['safe_domain'] is True
        diagnostic = value['local_backend_diagnostics']
        assert diagnostic['actual_binary_files'] == 2 and diagnostic['fused_counts'] not in (None, 'fallback')
        assert len(diagnostic['shared_fk_checked'].split(',')) == 6
        write_report('reports/recovery-native-' + args.variant + '-server-crash.json', {
            **value, 'candidate_hook_sha256': frozen, 'variant': args.variant,
            'official_eligibility_unproven': True})

    driver.write_report = recorded
    driver.main()


if __name__ == '__main__':
    main()
