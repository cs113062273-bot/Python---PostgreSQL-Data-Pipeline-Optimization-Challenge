"""Native official differential for bounded inline quantization/trim identity."""
import sys
from bench import t1_inline_probe as inline
from bench.common import write_report
from tests.semantics import native_copy as driver


def main():
    frozen=inline.source_identity();inline.install()
    def recorded(path,value):
        assert inline.source_identity()==frozen
        write_report(path,{**value,'inline_hook_sha256':frozen})
    driver.write_report=recorded
    if '--output' not in sys.argv:sys.argv.extend(['--output','reports/semantics-t1-inline-copy-121.json'])
    driver.main()


if __name__=='__main__':main()
