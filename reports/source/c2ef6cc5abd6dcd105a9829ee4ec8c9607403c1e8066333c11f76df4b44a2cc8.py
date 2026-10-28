"""Native per-value numeric parsing reuse after the full safe-domain proof.

Only table2 changes: parsed weight/gap depend on one selected TEXT value each,
so native Memoize may reuse conversions. All original fields are still checked
by the global proof first. No cache survives the SELECT or stores answers.
"""
import hashlib
from pathlib import Path
import re
from bench import refined_copy_probe as refined,script_transport_probe as metadata
from pipeline.recipes import EXTRACT
from pipeline.workspace import RELATION


def source_identity():
    h=hashlib.sha256((refined.source_identity()+metadata.source_identity()).encode());h.update(Path(__file__).read_text(encoding='utf8').encode());return h.hexdigest()


def statement(original):
    header=re.match(r'INSERT INTO public\.table2\s*\((.*?)\)\s*SELECT\b',original,re.S)
    if not header:raise RuntimeError('Unknown protected table2 header')
    columns=','.join(c.strip() for c in header[1].split(','))
    expected='id_column_2,integer_column_1,integer_column_2,text_column_1,integer_column_3,integer_column_4,integer_column_5,integer_column_6'
    if columns!=expected:raise RuntimeError('Unknown protected table2 columns')
    matrix=re.search(r'CROSS JOIN LATERAL \(\s*VALUES\s*(.*?)\s*\) AS position_data\([^)]*\)',original,re.S)
    if matrix is None:raise RuntimeError('Unknown protected table2 matrix')
    fields=EXTRACT.findall(matrix[1])
    if len(fields)!=56:raise RuntimeError('Expected fourteen protected four-field axes')
    def selected(attribute):
        values=[]
        for index in range(14):
            value='source_row.'+fields[4*index+attribute]
            if attribute==0:value="NULLIF(BTRIM("+value+"),'')::smallint"
            values.append('WHEN '+str(index+1)+' THEN '+value)
        return 'CASE position_keys.position_number '+' '.join(values)+' END'
    positions='(ARRAY['+','.join(str(i)+'::smallint' for i in range(1,15))+'])[1:LEAST(COALESCE(main_row.integer_column_1,0),14)]'
    # Integer conversion rounds exactly as the original ROUND only because
    # the unchanged proof admits <=3 fractional digits and <=6 integer digits.
    return f'''INSERT INTO public.table2 ({columns})
      SELECT main_row.id_column_1,position_keys.position_number,
             parsed_weight.scaled,raw_axis.axis_kind,
             NULL::integer,NULL::integer,NULL::integer,
             CASE WHEN parsed_gap.scaled>0 THEN parsed_gap.scaled ELSE NULL::integer END
      FROM {RELATION} source_row
      JOIN public.table1 main_row ON main_row.id_column_3=source_row.raw_id
      CROSS JOIN LATERAL pg_catalog.unnest({positions}) position_keys(position_number)
      CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW({selected(0)},{selected(1)},{selected(2)},{selected(3)})])
          raw_axis(declared smallint,axis_kind text,weight_text text,gap_text text)
      CROSS JOIN LATERAL (SELECT (NULLIF(raw_axis.weight_text,'')::numeric*1000)::integer AS scaled OFFSET 0) parsed_weight
      CROSS JOIN LATERAL (SELECT (NULLIF(raw_axis.gap_text,'')::numeric*1000)::integer AS scaled OFFSET 0) parsed_gap
      WHERE raw_axis.declared=position_keys.position_number
        AND raw_axis.axis_kind IN ('X','Y','Z') AND parsed_weight.scaled>=0'''


def install():
    metadata.install()
    import pipeline.parallel_normalization as parallel
    import pipeline.guarded_normalization as guarded
    original_projection=parallel.prepare_input_sql
    def projection(scopes,*,logged=False):return '-- lab per-value numeric Memoize source_sha256='+source_identity()+'\n'+original_projection(scopes,logged=logged)
    parallel.prepare_input_sql=projection
    original_statement=guarded.fast_statement
    def transformed(original,scopes,mode=guarded.MODE):
        if mode=='guarded_copy' and original.startswith('INSERT INTO public.table2 ('):return statement(original)
        return original_statement(original,scopes,mode)
    guarded.fast_statement=transformed


def main():
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
