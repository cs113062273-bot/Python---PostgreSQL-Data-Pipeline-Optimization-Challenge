"""Strict native data/error equality with new parent stats and scratch hints."""
import sys
import types
from bench import parent_hint_probe as candidate
from bench.common import write_report


def main():
    frozen=candidate.source_identity()
    if len(sys.argv)>1 and sys.argv[1]=='boundaries':
        sys.argv.pop(1)
        from tests.semantics import t1_inline_boundaries as driver
        driver.inline=types.SimpleNamespace(source_identity=candidate.source_identity,install=candidate.install)
        def recorded(ignored,value):
            assert candidate.source_identity()==frozen
            write_report('reports/semantics-parent-hint-boundaries-82.json',{**value,'parent_hint_hook_sha256':frozen})
        driver.write_report=recorded;driver.main();return
    from tests.semantics import native_copy as driver
    candidate.install()
    def recorded(path,value):
        assert candidate.source_identity()==frozen
        write_report(path,{**value,'parent_hint_hook_sha256':frozen})
    driver.write_report=recorded
    if '--output' not in sys.argv:sys.argv.extend(['--output','reports/semantics-parent-hint-copy-121.json'])
    driver.main()


if __name__=='__main__':main()
