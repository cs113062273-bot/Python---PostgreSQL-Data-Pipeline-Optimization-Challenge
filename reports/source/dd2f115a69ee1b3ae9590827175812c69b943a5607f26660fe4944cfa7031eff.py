"""Dense native-parent lookup cache experiment; no identity assignment."""
import argparse
import re
import statistics
import time
import uuid
from bench.common import connect,require_lab,environment,write_report
from bench.run import prepared
from pipeline.parallel_normalization import prepare_input_sql,settings_sql
from pipeline.guarded_normalization import fast_statement
from pipeline.workspace import RELATION


def mapped(statement,relation,lo):
    parent=re.search(r'JOIN public\.table1 AS main_row\s+ON main_row\.id_column_3 = source_row\.raw_id',statement)
    if not parent:raise RuntimeError('Unknown protected parent join')
    # A nonempty append forces one flattened in-memory result. Concatenating
    # an empty array may return the original TOAST pointer and decompress the
    # entire map for every source-row lookup. Keep the array free of NULLs:
    # seeking fixed-width elements with a NULL bitmap can become linear.
    # The appended zero row is outside the proved dense ID range and unused.
    init='(SELECT pg_catalog.array_cat(ids,ARRAY[[0::bigint,0::bigint]]) FROM '+relation+')'
    index='(source_row.raw_id-('+str(lo)+')+1)::integer'
    projection='CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW('+init+'['+index+':'+index+'])]) lookup(pair bigint[])\nCROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW(lookup.pair[1][1],lookup.pair[1][2]::smallint)]) main_row(id_column_1 bigint,integer_column_1 smallint)'
    # Original INNER JOIN also removes rows with no qualifying parent.
    return statement[:parent.start()]+projection+statement[parent.end():]


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_native_variant_e939b248ec')
    p.add_argument('--repeats',type=int,default=2)
    p.add_argument('--output',default='reports/parent-map-components-1m.json')
    args=p.parse_args();report={'kind':'rollback_only_native_parent_lookup_components','official_score':False,'runs':[],'scope':'Hash-aggregated complete transformed rows only, not COPY/load/keys/FKs/versions/commit. Cached IDs are read from native parent identities, never assigned. Dense raw-ID proof required.'}
    with connect(args.database,autocommit=True) as c:
        require_lab(c);report['environment']=environment(c)
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        with c.transaction(force_rollback=True):
            c.execute("SET LOCAL statement_timeout='15s'")
            c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB';")
            schema='parent_probe_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
            c.execute(prepare_input_sql(scopes,logged=True).replace('challenge_lab.pipeline_normalized_input',schema+'.input'))
            c.execute('ANALYZE '+schema+'.input(raw_id,text_column_1,text_column_2,text_column_3)')
            lo,hi,count=c.execute('SELECT min(id_column_3),max(id_column_3),count(*) FROM public.table1').fetchone()
            assert hi-lo+1==count and count<10000000
            started=time.perf_counter()
            c.execute('CREATE TABLE '+schema+'.map AS SELECT array_agg(ARRAY[id_column_1,coalesce(integer_column_1,0)::bigint] ORDER BY id_column_3) AS ids FROM public.table1')
            report['cache_build_seconds']=time.perf_counter()-started
            for d in prepared('guarded_copy',scopes):
                table=d['pure_table_name']
                if table not in ('table2','table5','table6'):continue
                statement=fast_statement(d['statements'][0],scopes,'guarded_copy').replace(RELATION,schema+'.input')
                expected=None
                for repeat in range(args.repeats):
                    for mode in (('original','mapped') if repeat%2==0 else ('mapped','original')):
                        query=statement if mode=='original' else mapped(statement,schema+'.map',lo)
                        query=re.fullmatch(r'INSERT INTO public\.table[1-6]\s*\(.*?\)\s*(SELECT\s.*?);?',query,re.S)[1].rstrip(';')
                        if mode=='mapped':query+=' AND main_row.id_column_1 IS NOT NULL' if re.search(r'\bWHERE\b',query,re.I) else ' WHERE main_row.id_column_1 IS NOT NULL'
                        c.execute('SET LOCAL enable_memoize='+('off' if mode=='mapped' else 'on'))
                        if mode=='mapped' and repeat==0:
                            write_report('reports/parent-map-'+table+'-plan-v4.json',c.execute('EXPLAIN (FORMAT JSON) '+query).fetchone()[0])
                        started=time.perf_counter()
                        fp=c.execute('SELECT count(*),sum(hash_record_extended(q,17)::numeric)::text,sum(hash_record_extended(q,97)::numeric)::text FROM ('+query+') q').fetchone()
                        seconds=time.perf_counter()-started
                        if expected is None:expected=fp
                        assert fp==expected,(table,mode,fp,expected)
                        report['runs'].append({'table':table,'repeat':repeat,'mode':mode,'seconds':seconds,'full_typed_transformation_fingerprint':fp})
                        print(table,repeat,mode,round(seconds,4),flush=True)
            report['all_scratch_transactionally_rolled_back']=True
            report['summary']={table:{mode:statistics.median(r['seconds'] for r in report['runs'] if r['table']==table and r['mode']==mode) for mode in ('original','mapped')} for table in ('table2','table5','table6')}
            write_report(args.output,report)


if __name__=='__main__':main()
