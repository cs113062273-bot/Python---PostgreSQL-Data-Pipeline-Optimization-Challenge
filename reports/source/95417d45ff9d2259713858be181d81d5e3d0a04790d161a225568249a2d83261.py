"""Reuse the original scalar conversions in table1/table5 after full proof."""
import hashlib
from pathlib import Path
import re
from bench import memo_axes_probe as axes
from pipeline.workspace import RELATION


def source_identity():
    return hashlib.sha256((axes.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def statement(body):
    anchor='FROM '+RELATION+' AS source_row'
    if body.startswith('INSERT INTO public.table1 ('):
        pattern=r"CASE\s+WHEN\s+NULLIF\(BTRIM\(source_row\.(numeric_column_[12])\),\s*''\)::numeric\s*>\s*0\s+THEN\s+ROUND\(\s*NULLIF\(BTRIM\(source_row\.\1\),\s*''\)::numeric\s*\*\s*1000\s*\)::integer\s+ELSE\s+NULL::integer\s+END"
        matches=list(re.finditer(pattern,body,re.I))
        assert len(matches)==2 and {m[1] for m in matches}=={'numeric_column_1','numeric_column_2'}
        joins=[]
        for match in reversed(matches):
            alias='parsed_'+match[1]
            joins.append('CROSS JOIN LATERAL pg_catalog.unnest(pg_catalog.array_append(ARRAY[]::integer[],('+match[0]+'))) '+alias+'(value)')
            body=body[:match.start()]+alias+'.value'+body[match.end():]
    elif body.startswith('INSERT INTO public.table5 ('):
        rounded=r"ROUND\(\s*NULLIF\(BTRIM\(source_row\.integer_column_1\),\s*''\)::numeric,\s*1\s*\)"
        matches=list(re.finditer(rounded,body,re.I));assert len(matches)==1
        match=matches[0]
        joins=['CROSS JOIN LATERAL pg_catalog.unnest(pg_catalog.array_append(ARRAY[]::numeric[],('+match[0]+'))) parsed_table5(value)']
        body=body[:match.start()]+'parsed_table5.value'+body[match.end():]
        # The unchanged integer grammar admits no fractional digits. Thus its
        # original numeric >0 predicate equals ROUND(value,1)>0 for every safe
        # input, including NULL/empty and signed zero. Unsafe input falls back.
        predicate=r"NULLIF\(BTRIM\(source_row\.integer_column_1\),\s*''\)::numeric\s*>\s*0"
        body,count=re.subn(predicate,'parsed_table5.value>0',body);assert count==1
    else:return body
    assert body.count(anchor)==1
    return body.replace(anchor,anchor+'\n'+'\n'.join(joins),1)


def install(*,tables=('table1','table5')):
    if not tables or not set(tables)<=set(('table1','table5')):raise ValueError(tables)
    axes.install('function')
    import pipeline.guarded_normalization as guarded
    import pipeline.parallel_normalization as parallel
    original=guarded.fast_statement
    def transformed(original_sql,scopes,mode=guarded.MODE):
        body=original(original_sql,scopes,mode)
        return statement(body) if mode=='guarded_copy' and any(original_sql.startswith('INSERT INTO public.'+table+' (') for table in tables) else body
    guarded.fast_statement=transformed
    projection=parallel.prepare_input_sql
    parallel.prepare_input_sql=lambda scopes,logged=False:'-- lab scalar conversion reuse source_sha256='+source_identity()+'\n'+projection(scopes,logged=logged)


def main():
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
