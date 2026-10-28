"""Fixed canonical recipe transformations over the compact native cache."""
import re
from bench.run import prepared
from bench.text_stage_probe import statement as normalized_text
from pipeline.guarded_normalization import fast_statement
from pipeline.normalization import source_sql
from pipeline.workspace import RELATION
from bench import native_c_lab as lab


def projection(scopes,function,relation):
    assert re.fullmatch(r'[a-z][a-z_0-9]*\.[a-z][a-z_0-9]*',relation)
    return 'CREATE TEMP TABLE '+relation.split('.')[1]+' ON COMMIT DROP AS SELECT s.raw_id,n.* FROM '+source_sql(scopes)+" s CROSS JOIN LATERAL "+function+"(s.raw->'values') n("+lab.compact_definition()+');'


def statement(original,scopes,axes_function,relation='pg_temp.native_compact_case'):
    recipes=prepared('baseline',scopes)
    recipe=next((r for r in recipes if r['statements']==[original]),None)
    if recipe is None:raise RuntimeError('Compact prototype requires an exact pinned canonical recipe')
    table=recipe['pure_table_name']
    if table in ('table3','table4','table6'):
        return normalized_text(fast_statement(original,scopes,'guarded_copy')).replace(RELATION,relation)
    match=re.match(r'(INSERT INTO public\.'+table+r'\s*\((.*?)\))\s*SELECT\b',original,re.S);assert match
    columns=','.join(s.strip() for s in match[2].split(','))
    if table=='table1':
        assert columns=='id_column_2,text_column_1,datetime_column_1,text_column_2,integer_column_1,integer_column_2,integer_column_3,text_column_3,id_column_3,id_column_4,id_column_5,integer_column_4,integer_column_5,id_column_6,integer_column_6,integer_column_7'
        select="""SELECT source_row.pipeline_identifier,NULL::varchar,
          source_row.pipeline_stamp::timestamptz AT TIME ZONE 'America/Sao_Paulo',NULL::varchar,
          source_row.pipeline_axis_max,NULL::smallint,source_row.pipeline_numeric_2,NULL::varchar,
          source_row.raw_id,dimension_row.id_column_1,NULL::bigint,NULL::integer,NULL::integer,
          NULL::integer,NULL::integer,source_row.pipeline_numeric_1
          FROM """+relation+""" source_row JOIN public.table3 dimension_row
          ON dimension_row.text_column_1=source_row.text_column_1
          AND dimension_row.text_column_2='SYNTHETIC/REFERENCE' AND dimension_row.numeric_column_1 IS NULL
          AND dimension_row.text_column_3=LEFT(source_row.text_column_2,1)
          AND dimension_row.text_column_4=RIGHT(source_row.text_column_2,1)"""
    elif table=='table5':
        assert columns=='numeric_column_1,numeric_column_2,integer_column_1,integer_column_2,id_column_2,id_column_3'
        select='SELECT ROUND(source_row.pipeline_integer_1::numeric,1),NULL::numeric,NULL::integer,NULL::integer,main_row.id_column_1,NULL::bigint FROM '+relation+' source_row JOIN public.table1 main_row ON main_row.id_column_3=source_row.raw_id WHERE source_row.pipeline_integer_1>0'
    elif table=='table2':
        assert columns=='id_column_2,integer_column_1,integer_column_2,text_column_1,integer_column_3,integer_column_4,integer_column_5,integer_column_6'
        assert re.fullmatch(r'native_lab\.axes_[0-9a-f]{16}',axes_function)
        select='SELECT main_row.id_column_1,axis.position_number,axis.weight,axis.axis_kind,NULL::integer,NULL::integer,NULL::integer,axis.gap FROM '+relation+' source_row JOIN public.table1 main_row ON main_row.id_column_3=source_row.raw_id CROSS JOIN LATERAL '+axes_function+'(source_row.pipeline_axes) axis'
    else:raise AssertionError(table)
    return match[1]+' '+select
