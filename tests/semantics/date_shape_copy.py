"""Native semantics and targeted fallback tests for the equivalent date shape."""
import sys
import types
from bench import date_shape_probe as shape
from bench.common import write_report


def main():
    frozen=shape.source_identity()
    if len(sys.argv)>1 and sys.argv[1]=='boundaries':
        sys.argv.pop(1)
        from tests.semantics import t1_inline_boundaries as driver
        driver.inline=types.SimpleNamespace(source_identity=shape.source_identity,install=shape.install)
        def recorded(ignored,value):
            assert shape.source_identity()==frozen
            write_report('reports/semantics-date-shape-boundaries-82.json',{**value,'date_shape_hook_sha256':frozen})
        driver.write_report=recorded;driver.main();return
    from tests.semantics import native_copy as driver
    shape.install()
    def recorded(path,value):
        assert shape.source_identity()==frozen
        write_report(path,{**value,'date_shape_hook_sha256':frozen})
    driver.write_report=recorded
    if '--output' not in sys.argv:sys.argv.extend(['--output','reports/semantics-date-shape-copy-121.json'])
    driver.main()


if __name__=='__main__':main()
