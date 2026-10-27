"""Isolated conservative compact-axis research, never the default compiler.

The existing proof bounds each accepted numeric to <=999999.999 in magnitude.
For qualifying axes a native point stores two exact integers: x encodes the
position/type and rounded nonnegative weight; y stores the positive rounded
gap, or zero for SQL NULL. x<42e9 and y<1e9, both exactly representable in
float8 (<2**53). This packs data values, never surrogate identities. Every
unsafe input still selects the original entire batch. No UDF/catalog exemption.
"""
import hashlib
from pathlib import Path
import re
import sys

from pipeline.parallel_normalization import prepare_input_sql as original_projection
from pipeline.safe_domain import condition_sql
from bench.typed_stage_probe import typed_statement


def source_identity():
    h=hashlib.sha256(Path(__file__).read_text(encoding='utf-8').encode())
    h.update(Path(__file__).with_name('typed_stage_probe.py').read_text(encoding='utf-8').encode())
    return h.hexdigest()


def compact_projection(scopes,*,logged=False):
    query=original_projection(scopes,logged=logged)
    marker='SELECT s.raw_id,n.*,'+condition_sql()+' AS pipeline_domain_safe'
    assert query.count(marker)==1
    fields=['n.text_column_1','n.text_column_2','n.text_column_3']
    for name,kind in [('id_column_1','integer'),('datetime_column_1','timestamptz'),('integer_column_1','numeric'),('numeric_column_1','numeric'),('numeric_column_2','numeric')]:
        fields.append(f"CASE WHEN proof.safe THEN NULLIF(BTRIM(n.{name}),'')::{kind} END AS {name}")
    declared=[f"NULLIF(BTRIM(n.integer_column_{i}), '')::smallint" for i in range(2,16)]
    fields.append('CASE WHEN proof.safe THEN GREATEST('+','.join(declared)+') END AS pipeline_declared_max')
    axes=[]
    for pos in range(1,15):
        weight=f"NULLIF(BTRIM(n.numeric_column_{2*pos+1}), '')::numeric"
        gap=f"NULLIF(BTRIM(n.numeric_column_{2*pos+2}), '')::numeric"
        kind=f'UPPER(BTRIM(n.text_column_{pos+3}))'
        packed=f"(({pos-1}::bigint*3+(ASCII({kind})-ASCII('X')))*1000000000+ROUND({weight}*1000)::bigint)::double precision"
        y=f'CASE WHEN {gap}>0 THEN ROUND({gap}*1000)::integer ELSE 0 END'
        axes.append(f'CASE WHEN proof.safe THEN CASE WHEN {declared[pos-1]}={pos} AND {weight}>=0 AND {kind} IN (\'X\',\'Y\',\'Z\') THEN pg_catalog.point({packed},({y})::double precision) END END')
    # point has no default equality operator, so array_remove is unsuitable.
    # Native NULL bitmap keeps empty slots compact; the original maximum-
    # position bound limits expansion and NULL points are filtered afterward.
    fields.append('ARRAY['+','.join(axes)+'] AS pipeline_axes')
    query=query.replace(marker,'SELECT s.raw_id,'+','.join(fields)+',proof.safe AS pipeline_domain_safe',1)
    assert query.endswith(';')
    query=query[:-1]+'\nCROSS JOIN LATERAL pg_catalog.unnest(ARRAY['+condition_sql()+']) proof(safe);'
    return '-- lab compact axes source_sha256='+source_identity()+'\n'+query


def axis_statement(statement):
    from pipeline.workspace import RELATION
    match=re.match(r'INSERT INTO public\.table2\s*\((.*?)\)\s*SELECT\b',statement,re.S)
    if not match:raise RuntimeError('Unknown canonical table2 header')
    columns=','.join(c.strip() for c in match[1].split(','))
    if columns!='id_column_2,integer_column_1,integer_column_2,text_column_1,integer_column_3,integer_column_4,integer_column_5,integer_column_6':
        raise RuntimeError('Unknown canonical table2 columns')
    return f'''INSERT INTO public.table2 ({columns})
      SELECT main_row.id_column_1,
        (((axis.p)[0]::bigint/1000000000)/3+1)::smallint,
        ((axis.p)[0]::bigint%1000000000)::integer,
        pg_catalog.substr('XYZ',(((axis.p)[0]::bigint/1000000000)%3+1)::integer,1),
        NULL::integer,NULL::integer,NULL::integer,NULLIF((axis.p)[1]::integer,0)
      FROM {RELATION} source_row
      JOIN public.table1 main_row ON main_row.id_column_3=source_row.raw_id
      CROSS JOIN LATERAL pg_catalog.unnest((source_row.pipeline_axes)[1:LEAST(COALESCE(main_row.integer_column_1,0),14)]) axis(p)
      WHERE axis.p IS NOT NULL'''


def install():
    import pipeline.parallel_normalization as projection
    import pipeline.guarded_normalization as guarded
    projection.prepare_input_sql=compact_projection
    original_validation=guarded.validation_destinations
    def validated(destinations):
        # New native constructor must pass the unchanged organizer guard too.
        return original_validation(destinations)+[{'statements':[],'function_references':[('pg_catalog','point')]}]
    guarded.validation_destinations=validated
    original=guarded.fast_statement
    def patched(statement,scopes,mode=guarded.MODE):
        if mode!='guarded_copy':return original(statement,scopes,mode)
        if statement.startswith('INSERT INTO public.table2 ('):return axis_statement(statement)
        result=original(statement,scopes,mode)
        if statement.startswith('INSERT INTO public.table1 ('):
            result,n=re.subn(r'GREATEST\(\s*NULLIF\(BTRIM\(source_row\.integer_column_2\).*?\n    \)', 'source_row.pipeline_declared_max',result,flags=re.S)
            if n!=1:raise RuntimeError('Unknown canonical declared-position maximum')
        return typed_statement(result)
    guarded.fast_statement=patched


def main():
    if len(sys.argv)<2 or sys.argv[1] not in ('service','semantics'):
        raise SystemExit('Use service or semantics followed by driver arguments')
    action=sys.argv.pop(1);install()
    if action=='service':
        from bench.service_run import main as driver
    else:
        from tests.semantics.native_copy import main as driver
    driver()


if __name__=='__main__':main()
