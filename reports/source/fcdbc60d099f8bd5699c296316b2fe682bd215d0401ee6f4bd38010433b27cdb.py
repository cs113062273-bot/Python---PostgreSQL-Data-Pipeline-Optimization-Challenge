"""Native output/error/layout/default checks for constant-NULL COPY omission."""
import sys
import types
from bench import slim_copy_probe as candidate
from bench.common import write_report


def main():
    frozen=candidate.source_identity()
    if len(sys.argv)>1 and sys.argv[1]=='boundaries':
        sys.argv.pop(1)
        from tests.semantics import t1_inline_boundaries as driver
        driver.inline=types.SimpleNamespace(source_identity=candidate.source_identity,install=candidate.install)
        def recorded(ignored,value):
            assert candidate.source_identity()==frozen
            write_report('reports/semantics-slim-copy-boundaries-82.json',{**value,'slim_copy_hook_sha256':frozen})
        driver.write_report=recorded;driver.main();return
    candidate.install()
    if len(sys.argv)>1 and sys.argv[1]=='layouts':
        from tests.semantics import copy_layout as driver
        extras=[('omitted_default_'+table+'_'+column,'ALTER TABLE public.'+table+' ALTER COLUMN '+column+' SET DEFAULT '+value,'ALTER TABLE public.'+table+' ALTER COLUMN '+column+' DROP DEFAULT',None if value=='NULL' else False) for table,column,value in [('table1','integer_column_4','11'),('table2','integer_column_3','19'),('table3','numeric_column_1','22'),('table5','numeric_column_2','1/0'),('table1','text_column_1','NULL')]]
        write=driver.write_report
        driver.write_report=lambda path,value:write(path,{**value,'slim_copy_hook_sha256':frozen,'added_receiving_default_cases':5})
        driver.main(extra_variants=extras,output='reports/semantics-slim-copy-layouts-11-v2.json');return
    from tests.semantics import native_copy as driver
    def recorded(path,value):
        assert candidate.source_identity()==frozen
        write_report(path,{**value,'slim_copy_hook_sha256':frozen})
    driver.write_report=recorded
    if '--output' not in sys.argv:sys.argv.extend(['--output','reports/semantics-slim-copy-native.json'])
    if '--transport' not in sys.argv:sys.argv.extend(['--transport','server'])
    if '--count' not in sys.argv:sys.argv.extend(['--count','100'])
    driver.main()


if __name__=='__main__':main()
