"""Compiler-only exact outputs and cold-per-operation parse-cache timings."""
import hashlib
from pathlib import Path
import statistics
import time
from bench import native_compact_sql as compiler,native_compile_cache_probe as candidate
from bench.common import write_report
from bench.run import prepared


def main():
    original=compiler.prepared
    vectors=[['group001'],['group001','group002'],["組別 o'brien $work_namespace$\nnext"],['x); SELECT 1; --'],['a','a','b']]
    cases=[];runs=[]
    for scopes in vectors:
        recipes=prepared('baseline',scopes)
        expected=[compiler.statement(r['statements'][0],scopes,'pgdm_pipeline_work.axes') for r in recipes]
        cache=candidate.install_cache()
        actual=[compiler.statement(r['statements'][0],scopes,'pgdm_pipeline_work.axes') for r in recipes]
        assert actual==expected
        assert cache.cache_info().misses==1 and cache.cache_info().hits==5
        cases.append({'scopes':scopes,'all_six_statements_byte_identical':True,'sql_sha256':[hashlib.sha256(s.encode()).hexdigest() for s in actual]})
        compiler.prepared=original
    scopes=['group001'];recipes=prepared('baseline',scopes)
    for repeat in range(5):
        for mode in (('original','cached') if repeat%2==0 else ('cached','original')):
            compiler.prepared=original
            if mode=='cached':cache=candidate.install_cache();cache.cache_clear()
            start=time.perf_counter()
            result=[compiler.statement(r['statements'][0],scopes,'pgdm_pipeline_work.axes') for r in recipes]
            seconds=time.perf_counter()-start
            compiler.prepared=original
            runs.append({'repeat':repeat,'mode':mode,'seconds':seconds,'cold_cache_at_operation_start':True})
    summary={mode:statistics.median(r['seconds'] for r in runs if r['mode']==mode) for mode in ('original','cached')}
    write_report('reports/native-compiler-cache-components.json',{'kind':'pure_sql_compiler_cache_component','passed':True,'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'cases':cases,'runs':runs,'summary':summary,'scope':'Canonical compiler construction only. Byte-identical six SQL statements for arbitrary quoted/Unicode/dollar-tag scopes. No source/catalog/function/rowcount/version validation result is cached. No database execution or full-service speedup claim.'})
    print(summary,flush=True)


if __name__=='__main__':main()
