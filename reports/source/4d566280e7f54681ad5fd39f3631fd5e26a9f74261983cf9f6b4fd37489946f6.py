"""Check every numeric TEXT field directly without allocating proof bundles.

The accepted domain is identical to the original bounded decimal grammar.
NULL and empty remain accepted; unsafe inputs still take the original batch.
No extraction, position, error-producing cast or source field is skipped.
"""
import argparse
import hashlib
from pathlib import Path
import random
import statistics
import sys
import time
import uuid
from bench.common import connect, require_lab, environment, write_report
from bench import refined_copy_probe as refined, text_stage_probe as text
from pipeline.safe_domain import _bundle, condition_sql, condition_parts, literal, INTEGER, NUMERIC
from pipeline.parallel_normalization import settings_sql
from pipeline.recipes import EXTRACT, destinations


def source_identity():
    return hashlib.sha256((refined.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def individual(prefix, count):
    unit = INTEGER if prefix == 'integer_column' else NUMERIC
    pattern = literal(r'\A(?:'+unit+r')?\Z')
    return ' AND '.join(f'(n.{prefix}_{i} IS NULL OR (n.{prefix}_{i} COLLATE "C") ~ {pattern})' for i in range(1,count+1))


def projection(scopes, *, logged=False):
    body = text.projection(scopes, logged=logged)
    proof = individual('integer_column',15)+' AND '+individual('numeric_column',30)+' AND '+' AND '.join(condition_parts()[2:])
    assert body.count(condition_sql()) == 1
    body = body.replace(condition_sql(),proof,1)
    # The two bundle columns are internal to the one-row expression barrier.
    # They have no remaining consumer after the equivalent predicate rewrite.
    for prefix,count in (('integer_column',15),('numeric_column',30)):
        marker = _bundle(prefix,count).replace('c.','n.')
        assert body.count(marker)==1
        body = body.replace(marker,"''::text",1)
    if logged:
        body=body.replace('CREATE TABLE challenge_lab.pipeline_normalized_input AS','CREATE UNLOGGED TABLE challenge_lab.pipeline_normalized_input AS',1)
    return '-- lab individual numeric proof source_sha256='+source_identity()+'\n'+body


def component():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_template_1m')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/per-field-proof-projection-1m.json')
    args=p.parse_args(); frozen=source_identity()
    report={'kind':'rollback_only_equivalent_per_field_domain','official_score':False,'source_sha256':frozen,'runs':[]}
    rng=random.Random(77721)
    units=[None,'','0','-0','+12','1234','12345','999999','1000000','1.2','+1.000','1.2345','.1','1.','1..2','1-2','--1','+','-','|','1|2','1\n','中文','a','0e2','NaN','Infinity',' ','\t']
    with connect(args.database,autocommit=True) as c:
        require_lab(c);report['environment']=environment(c)
        assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'Do not overlap a benchmark'
        report['differential_checks']=[]
        for index,(prefix,count) in enumerate((('integer_column',15),('numeric_column',30))):
            rows=[]
            for pos in range(count):
                for unit in units:
                    row=[None]*count;row[pos]=unit;rows.append(tuple(row))
            rows.extend(tuple(rng.choice(units) for _ in range(count)) for _ in range(3000))
            columns=','.join(prefix+'_'+str(i) for i in range(1,count+1))
            different=0
            name=('integers','numerics')[index]
            for offset in range(0,len(rows),100):
                block=rows[offset:offset+100]
                values=','.join('('+','.join(['%s::text']*count)+')' for _ in block)
                query='SELECT count(*) FILTER(WHERE ('+condition_parts()[index]+') IS DISTINCT FROM ('+individual(prefix,count)+')) FROM (VALUES '+values+') n('+columns+') CROSS JOIN LATERAL (SELECT '+_bundle(prefix,count).replace('c.','n.')+' AS '+name+' OFFSET 0) c'
                different+=c.execute(query,tuple(v for row in block for v in row)).fetchone()[0]
            assert different==0,(prefix,different)
            report['differential_checks'].append({'fields':count,'vectors':len(rows),'differences':different})
        fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
        record='ROW('+','.join(['raw_id']+fields+['pipeline_domain_safe'])+')'
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')];expected=None
        for repeat in range(args.repeats):
            for mode in (('refined','individual') if repeat%2==0 else ('individual','refined')):
                assert source_identity()==frozen
                with c.transaction(force_rollback=True):
                    c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB'; SET LOCAL statement_timeout='90s';")
                    schema='individual_proof_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                    if mode=='refined':
                        from bench.proof_pattern_probe import negative_condition
                        query=text.projection(scopes,logged=True).replace(condition_sql(),negative_condition()).replace('CREATE TABLE challenge_lab.pipeline_normalized_input AS','CREATE UNLOGGED TABLE challenge_lab.pipeline_normalized_input AS')
                    else:query=projection(scopes,logged=True)
                    query=query.replace('challenge_lab.pipeline_normalized_input',schema+'.input')
                    started=time.perf_counter();c.execute(query);seconds=time.perf_counter()-started
                    fp=c.execute(f'SELECT count(*),sum(hash_record_extended({record},17)::numeric)::text,sum(hash_record_extended({record},97)::numeric)::text FROM {schema}.input').fetchone()
                    if expected is None:expected=fp
                    assert fp==expected
                    report['runs'].append({'repeat':repeat,'mode':mode,'seconds':seconds,'complete_text_and_proof_fingerprint':fp})
                    print(repeat,mode,round(seconds,4),flush=True);write_report(args.output,report)
        report['summary']={mode:statistics.median(r['seconds'] for r in report['runs'] if r['mode']==mode) for mode in ('refined','individual')}
        report['all_private_work_rolled_back']=True;write_report(args.output,report)


def install():
    from bench.script_transport_probe import install as metadata_install
    metadata_install()
    import pipeline.parallel_normalization as parallel
    parallel.prepare_input_sql=projection


def main():
    if len(sys.argv)>1 and sys.argv[1]=='component':sys.argv.pop(1);component();return
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
