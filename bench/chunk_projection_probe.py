"""Bounded normalized projection plus compatible source SHA, research only.

All staging DDL is transactionally rolled back. EXPLAIN instrumentation makes
these component observations separate from ranked whole-pipeline timing.
"""
import argparse
import hashlib
import statistics
import time
import uuid

from psycopg import sql

from bench.common import ROOT,connect,environment,require_lab,write_report
from pipeline.normalization import source_sql
from pipeline.parallel_normalization import prepare_input_sql,settings_sql
from pipeline.recipes import EXTRACT,destinations
from bench.source_hash_probe import BINARY_BARRIER_COPY


def read_digest(c,query,params=None):
    value=hashlib.sha256()
    with c.cursor().copy(query,params) as copy:
        for block in copy:value.update(block)
    return value.hexdigest()


def workers(node):
    return max([node.get('Workers Launched',0)]+[workers(p) for p in node.get('Plans',[])])


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_template_1m')
    p.add_argument('--chunk-rows',type=int,default=1000000)
    p.add_argument('--after-id',type=int)
    p.add_argument('--repeats',type=int,default=1)
    p.add_argument('--modes',nargs='+',choices=['plain','with_source_sha','with_binary_sha'],default=['plain','with_source_sha','with_binary_sha'])
    p.add_argument('--output',default='reports/chunk-projection-probe-1m.json')
    args=p.parse_args()
    if args.chunk_rows<1 or args.repeats<1:p.error('Positive chunk size and repeat count required')
    with connect(args.database,autocommit=True) as c:
        require_lab(c)
        report={'kind':'bounded_shared_projection_fingerprint_components','official_score':False,'database':args.database,'environment':environment(c),'chunk_rows_cap':args.chunk_rows,'after_id':args.after_id,'runs':[],'scope':'Transactional scratch, fixed row-count cap discovered by ordered source PK; actual source data never modified. Instrumented component cost, not complete pipeline time or combined checkpoint qualification.'}
        fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
        attrs=','.join(['raw_id']+fields+['pipeline_domain_safe'])
        with c.transaction(force_rollback=True):
            c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
            c.execute('LOCK TABLE public.raw_data IN SHARE MODE')
            scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY raw_schema')]
            started=time.perf_counter()
            hi,n=c.execute('''SELECT max(raw_id),count(*) FROM
                (SELECT raw_id FROM public.raw_data WHERE raw_schema=ANY(%s)
                 AND (%s::bigint IS NULL OR raw_id>%s) ORDER BY raw_id LIMIT %s) s''',(scopes,args.after_id,args.after_id,args.chunk_rows)).fetchone()
            report['boundary_discovery_seconds']=time.perf_counter()-started
            if not n:p.error('Selected chunk is empty')
            report['chunk_hi_id']=hi;report['actual_chunk_rows']=n
            assert n<=args.chunk_rows
            c.execute(settings_sql());c.execute("SET LOCAL work_mem='64MB'")
            expected_query='''COPY (SELECT raw_id::text||':'||encode(sha256(convert_to(to_jsonb(t)::text,'UTF8')),'hex')
                FROM public.raw_data t WHERE raw_schema=ANY(%s) AND raw_id<=%s
                AND (%s::bigint IS NULL OR raw_id>%s) ORDER BY raw_id) TO STDOUT'''
            started=time.perf_counter()
            expected=read_digest(c,expected_query,(scopes,hi,args.after_id,args.after_id))
            report['separate_reference_fingerprint_seconds']=time.perf_counter()-started
            report['expected_compatible_source_sha256']=expected
            expected_binary=None
            if 'with_binary_sha' in args.modes:
                query=BINARY_BARRIER_COPY.replace('WHERE raw_schema=ANY(%s)', 'WHERE raw_schema=ANY(%s) AND t.raw_id<=%s AND (%s::bigint IS NULL OR t.raw_id>%s)',1)
                started=time.perf_counter()
                expected_binary=read_digest(c,query,(scopes,hi,args.after_id,args.after_id))
                report['separate_binary_reference_seconds']=time.perf_counter()-started
                report['expected_binary_research_sha256']=expected_binary
            source=source_sql(scopes)
            bound=source.rsplit('\n)',1)[0]+f' AND "raw_id"<={int(hi)}'+('' if args.after_id is None else f' AND "raw_id">{int(args.after_id)}')+'\n)'
            expected_projection=None
            for repeat in range(args.repeats):
                shift=repeat%len(args.modes)
                modes=args.modes[shift:]+args.modes[:shift]
                for mode in modes:
                    schema='projection_probe_'+uuid.uuid4().hex[:10]
                    c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
                    query=prepare_input_sql(scopes,logged=True)
                    query=query.replace('challenge_lab.pipeline_normalized_input',schema+'.input',1)
                    assert query.count(source)==1
                    query=query.replace(source,bound,1)
                    if mode=='with_source_sha':
                        marker='SELECT s.raw_id,n.*,'
                        assert query.count(marker)==1
                        query=query.replace(marker,"SELECT s.raw_id,sha256(convert_to(to_jsonb(s)::text,'UTF8')) AS pipeline_source_sha,n.*,",1)
                    elif mode=='with_binary_sha':
                        marker='SELECT s.raw_id,n.*,'
                        assert query.count(marker)==1 and query.count(bound+' s\n')==1
                        query=query.replace(marker,'SELECT s.raw_id,fingerprint.pipeline_source_sha,n.*,',1)
                        query=query.replace(bound+' s\n',bound+' s\n CROSS JOIN LATERAL unnest(ARRAY[sha256(record_send(s))]) fingerprint(pipeline_source_sha)\n',1)
                    query='EXPLAIN (ANALYZE,BUFFERS,WAL,TIMING OFF,FORMAT JSON) '+query
                    digest=hashlib.sha256(query.encode()).hexdigest()
                    path=ROOT/'reports/sql'/(digest+'.sql');path.parent.mkdir(parents=True,exist_ok=True)
                    with path.open('w',encoding='utf-8',newline='\n') as f:f.write(query)
                    print('Projection',repeat+1,mode,'starting',flush=True)
                    started=time.perf_counter();plan=c.execute(query).fetchone()[0][0];wall=time.perf_counter()-started
                    size=c.execute('SELECT pg_total_relation_size(%s::regclass)',(schema+'.input',)).fetchone()[0]
                    started=time.perf_counter()
                    footprint=c.execute('SELECT count(*),bool_and(pipeline_domain_safe),sum(hash_record_extended(ROW('+attrs+'),17)::numeric)::text,sum(hash_record_extended(ROW('+attrs+'),97)::numeric)::text FROM '+schema+'.input').fetchone()
                    verification_seconds=time.perf_counter()-started
                    if expected_projection is None:expected_projection=footprint
                    assert footprint==expected_projection and footprint[0]==n
                    source_seconds=None
                    if mode in ('with_source_sha','with_binary_sha'):
                        started=time.perf_counter()
                        form="COPY (SELECT raw_id,pipeline_source_sha FROM {} ORDER BY raw_id) TO STDOUT (FORMAT BINARY)" if mode=='with_binary_sha' else "COPY (SELECT raw_id::text||':'||encode(pipeline_source_sha,'hex') FROM {} ORDER BY raw_id) TO STDOUT"
                        actual=read_digest(c,sql.SQL(form).format(sql.Identifier(schema,'input')))
                        source_seconds=time.perf_counter()-started
                        assert actual==(expected_binary if mode=='with_binary_sha' else expected),'Fused source fingerprint differs from its declared algorithm'
                    row={'repeat':repeat,'mode':mode,'projection_wall_seconds':wall,'execution_seconds':plan['Execution Time']/1000,'workers_launched':workers(plan['Plan']),'stage_bytes':size,'source_fingerprint_from_stage_seconds':source_seconds,'projection_fingerprint':footprint,'verification_seconds_excluded':verification_seconds,'sql_sha256':digest,'plan':plan}
                    report['runs'].append(row);write_report(args.output,report)
                    c.execute(sql.SQL('DROP TABLE {} RESTRICT').format(sql.Identifier(schema,'input')))
                    c.execute(sql.SQL('DROP SCHEMA {} RESTRICT').format(sql.Identifier(schema)))
                    print(f'Projection {mode}: {wall:.4f}s, stage fingerprint {source_seconds}, {size} bytes; compatible checks passed',flush=True)
        # force_rollback removes all diagnostic catalog/data changes, including
        # successful scratch creates/drops, and restores every local setting.
        report['all_scratch_transactionally_rolled_back']=True
        report['summary']={mode:{'projection_median_seconds':statistics.median(r['projection_wall_seconds'] for r in report['runs'] if r['mode']==mode),'projection_plus_staged_fingerprint_median_seconds':statistics.median(r['projection_wall_seconds']+r['source_fingerprint_from_stage_seconds'] for r in report['runs'] if r['mode']==mode) if mode!='plain' else None} for mode in args.modes}
        if 'plain' in report['summary']:
            report['summary']['plain']['with_separate_legacy_fingerprint_seconds']=report['summary']['plain']['projection_median_seconds']+report['separate_reference_fingerprint_seconds']
            if expected_binary is not None:report['summary']['plain']['with_separate_binary_fingerprint_seconds']=report['summary']['plain']['projection_median_seconds']+report['separate_binary_reference_seconds']
        write_report(args.output,report)


if __name__=='__main__':main()
