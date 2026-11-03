"""Full recipe differential, boundary, layout and axis replay for fused COPY."""
import argparse
import importlib
import sys
from bench.common import write_report
from tests.semantics import lazy_axis_text as driver


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--variant', choices=['fused', 'stream', 'temp-stage'], default='fused')
    args, remaining = parser.parse_known_args()
    sys.argv = [sys.argv[0], *remaining]
    module = {'fused': 'native_fused_probe', 'stream': 'native_stream_probe', 'temp-stage': 'native_temp_stage_probe'}[args.variant]
    candidate = importlib.import_module('bench.' + module)
    driver.candidate = candidate
    driver.write_report = lambda path, value: write_report(str(path).replace('lazy-axis', 'native-' + args.variant), value)
    driver.main()


if __name__ == '__main__':
    main()
