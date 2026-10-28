"""Try native per-vector reuse of the original fourteen-position maximum."""
import hashlib
from pathlib import Path
import re
import sys
from bench import t1_inline_probe as inline
from bench.common import write_report
from pipeline.workspace import RELATION


def source_identity():
    return hashlib.sha256((inline.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def maximum(body):
    if not body.startswith('INSERT INTO public.table1 ('):return body
    start=body.index('GREATEST(');depth=1;i=start+len('GREATEST(')
    while depth:
        if body[i]=='(':depth+=1
        elif body[i]==')':depth-=1
        i+=1
    expression=body[start:i]
    assert re.findall(r'source_row\.(integer_column_\d+)',expression)==['integer_column_'+str(n) for n in range(2,16)]
    anchor='FROM '+RELATION+' AS source_row';assert body.count(anchor)==1
    body=body[:start]+'parsed_axis_maximum.value'+body[i:]
    join='CROSS JOIN LATERAL pg_catalog.unnest(pg_catalog.array_append(ARRAY[]::smallint[],('+expression+'))) parsed_axis_maximum(value)'
    return body.replace(anchor,anchor+'\n'+join,1)


def component():
    # Reuse the already tested rollback/COPY/fingerprint harness, with explicit
    # labels that identify the changed expression in this separate report.
    original=inline.statement;identity=source_identity();original_identity=inline.source_identity
    inline.statement=lambda body:maximum(original(body))
    inline.source_identity=lambda:hashlib.sha256((original_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()
    labels={'calendar':'calendar','inline':'inline_max','inline_trim':'inline_trim_max'}
    def recorded(path,value):
        assert inline.source_identity()==identity
        value={**value,'kind':'rollback_only_table1_max_vector_reuse','max_vector_hook_sha256':identity}
        value['runs']=[{**row,'mode':labels[row['mode']]} for row in value['runs']]
        if 'summary' in value:value['summary']={labels[key]:seconds for key,seconds in value['summary'].items()}
        write_report(path,value)
    inline.write_report=recorded
    inline.component()


def install():
    inline.install()
    import pipeline.guarded_normalization as guarded
    import pipeline.parallel_normalization as parallel
    original=guarded.fast_statement
    def transformed(original_sql,scopes,mode=guarded.MODE):
        body=original(original_sql,scopes,mode)
        return maximum(body) if mode=='guarded_copy' and original_sql.startswith('INSERT INTO public.table1 (') else body
    guarded.fast_statement=transformed
    projection=parallel.prepare_input_sql
    parallel.prepare_input_sql=lambda scopes,logged=False:'-- lab position maximum reuse source_sha256='+source_identity()+'\n'+projection(scopes,logged=logged)


def main():
    if len(sys.argv)>1 and sys.argv[1]=='component':sys.argv.pop(1);component();return
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
