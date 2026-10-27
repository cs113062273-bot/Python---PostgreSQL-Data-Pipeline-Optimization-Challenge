"""Native parallel-safe extraction and safe-domain projection (experimental).

Single-element native arrays are expression barriers without correlated OFFSET
subplans. All element expressions are evaluated once. No UDF or volatility
catalog changes are used. The global proof decision still precedes all DML.
"""
from pipeline.normalization import source_sql
from pipeline.recipes import EXTRACT,destinations
from pipeline.safe_domain import _bundle,condition_sql


def prepare_input_sql(scopes,*,logged=False):
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    values=','.join(f"p.payload->>'{key}'" for key in fields)
    definitions=','.join(f'{key} text' for key in fields)
    bundles=[_bundle('integer_column',15).replace('c.','n.'),
             _bundle('numeric_column',30).replace('c.','n.'),
             "coalesce(n.id_column_1,'')","coalesce(n.datetime_column_1,'')"]
    target='CREATE TABLE challenge_lab.pipeline_normalized_input AS' if logged else 'CREATE TEMP TABLE pipeline_normalized_input ON COMMIT DROP AS'
    return target+"""
      SELECT s.raw_id,n.*,"""+condition_sql()+""" AS pipeline_domain_safe
      FROM """+source_sql(scopes)+""" s
      CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[s.raw->'values']) p(payload)
      CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW("""+values+")]) n("+definitions+")\n"+"CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW("+','.join(bundles)+")]) c(integers text,numerics text,identifier text,stamp text);"


def settings_sql(workers=8):
    if workers not in (0,2,4,8):raise ValueError(workers)
    return f"SET LOCAL max_parallel_workers_per_gather={workers}; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;"


def restore_settings_sql():
    return "DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;"


def prepare_sql(scopes,*,logged=False):
    from pipeline.recipes import ORDER
    names=('max_parallel_workers_per_gather','min_parallel_table_scan_size','parallel_setup_cost','parallel_tuple_cost','enable_parallel_hash')
    save="CREATE TEMP TABLE pipeline_parallel_settings ON COMMIT DROP AS SELECT name,setting FROM pg_settings WHERE name IN ("+','.join("'"+n+"'" for n in names)+");"
    restore=restore_settings_sql()
    empty=' AND '.join(f'NOT EXISTS(SELECT 1 FROM public.{t})' for t in ORDER)
    relation='challenge_lab.pipeline_normalized_input' if logged else 'pg_temp.pipeline_normalized_input'
    domain="CREATE TEMP TABLE pipeline_normalization_domain ON COMMIT DROP AS SELECT coalesce(bool_and(pipeline_domain_safe),true) AND "+empty+" AS safe FROM "+relation+';'
    namespace='CREATE SCHEMA IF NOT EXISTS challenge_lab;\n' if logged else ''
    return namespace+save+'\n'+settings_sql()+'\n'+prepare_input_sql(scopes,logged=logged)+'\n'+restore+'\n'+domain


def materialize_statement(statement,*,mode=None):
    """Lab-only CTAS transformation; identity allocation stays in final INSERT."""
    import re
    m=re.fullmatch(r'(INSERT INTO public\.(table[1-6])\s*\((.*?)\))\s*(SELECT\s.*?);?',statement,re.S)
    if not m:raise ValueError('Unrecognized canonical INSERT SELECT')
    header,table,columns,select=m.groups()
    columns=','.join(c.strip() for c in columns.split(','))
    name='pipeline_ready_'+table
    from pipeline.recipes import CACHE_CTAS_MODES
    tune='SET LOCAL enable_parallel_hash=off;\n' if mode in CACHE_CTAS_MODES and table=='table1' else ''
    prepare=settings_sql()+'\n'+tune+f'CREATE TEMP TABLE {name} ({columns}) ON COMMIT DROP AS '+select.rstrip(';')+';\n'+restore_settings_sql()
    insert=header+' SELECT '+columns+' FROM pg_temp.'+name
    return prepare,insert,'DROP TABLE pg_temp.'+name+';'
