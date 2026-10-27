"""Equivalent negative-pattern domain proof; isolated rollback research."""
import argparse
import hashlib
from pathlib import Path
import random
import statistics
import time
import uuid
from bench.common import ROOT,connect,require_lab,environment,write_report
from pipeline.parallel_normalization import prepare_input_sql,settings_sql
from pipeline.recipes import EXTRACT,destinations
from pipeline.safe_domain import condition_parts,condition_sql,literal


def source_identity():return hashlib.sha256(Path(__file__).read_text(encoding='utf-8').encode()).hexdigest()


def negative_parts():
    parts=condition_parts()
    patterns=[r'[^0-9|+-]|[0-9]{5}|[0-9][+-]|[+-][|+-]|[+-]$',
              r'[^0-9|+.-]|[0-9]{7}|[.][0-9]{4}|[.][0-9]*[.]|[.]([|]|$)|(^|[|])[+-]?[.]|[0-9.][+-]|[+-][|+.-]|[+-]$']
    for i,(name,count,pattern) in enumerate(zip(('integers','numerics'),(15,30),patterns)):
        parts[i]=f"(pg_catalog.octet_length(c.{name})-pg_catalog.octet_length(pg_catalog.replace(c.{name},'|',''))={count-1} AND (c.{name} COLLATE \"C\") !~ {literal(pattern)})"
    return parts


def negative_condition():return ' AND '.join(negative_parts())


def projection(scopes,*,logged=False):
    body=prepare_input_sql(scopes,logged=logged)
    assert body.count(condition_sql())==1
    return '-- lab negative domain source_sha256='+source_identity()+'\n'+body.replace(condition_sql(),negative_condition(),1)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_template_1m')
    p.add_argument('--repeats',type=int,default=2)
    p.add_argument('--output',default='reports/proof-pattern-components-1m.json')
    args=p.parse_args();report={'kind':'rollback_only_equivalent_domain_regex_components','official_score':False,'source_sha256':source_identity(),'runs':[]}
    rng=random.Random(7311)
    units=['','0','1','-0','+12','1234','12345','999999','1000000','1.2','+1.000','1.2345','.1','1.','1..2','1-2','--1','+','-','|','1|2','1\n','１２','a','0e2','NaN','Infinity',' ','\t']
    bundles=[]
    for count in (15,30):
        values=[]
        for unit in units:
            for pos in range(count):
                fields=['']*count;fields[pos]=unit;values.append('|'.join(fields))
        for _ in range(3000):
            values.append('|'.join(rng.choice(units) for _ in range(count)))
        bundles.append(values)
    # Same number of independently generated complete 15/30-field bundles.
    size=max(map(len,bundles))
    integers=(bundles[0]*2)[:size];numerics=bundles[1]
    with connect(args.database,autocommit=True) as c:
        require_lab(c);report['environment']=environment(c)
        checks=','.join('count(*) FILTER(WHERE ('+old+') IS DISTINCT FROM ('+new+'))' for old,new in zip(condition_parts()[:2],negative_parts()[:2]))
        probe='SELECT '+checks+",count(*) FROM (SELECT integers,numerics,''::text identifier,''::text stamp FROM unnest(%s::text[],%s::text[]) b(integers,numerics)) c"
        int_diff,num_diff,total=c.execute(probe,(integers,numerics)).fetchone();assert int_diff==num_diff==0,(int_diff,num_diff,total)
        report['domain_differential_vectors']=total;report['domain_differences']={'integers':int_diff,'numerics':num_diff}
        fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])});record='ROW('+','.join(['raw_id']+fields+['pipeline_domain_safe'])+')'
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')];expected=None
        for repeat in range(args.repeats):
            for mode in (('original','negative') if repeat%2==0 else ('negative','original')):
                with c.transaction(force_rollback=True):
                    c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB'; SET LOCAL statement_timeout='30s';")
                    schema='proof_pattern_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                    query=(prepare_input_sql if mode=='original' else projection)(scopes,logged=True).replace('challenge_lab.pipeline_normalized_input',schema+'.input')
                    digest=hashlib.sha256(query.encode()).hexdigest()
                    with (ROOT/'reports/sql'/(digest+'.sql')).open('w',encoding='utf-8',newline='\n') as f:f.write(query)
                    started=time.perf_counter();c.execute(query);seconds=time.perf_counter()-started
                    fp=c.execute(f'SELECT count(*),sum(hash_record_extended({record},17)::numeric)::text,sum(hash_record_extended({record},97)::numeric)::text FROM {schema}.input').fetchone()
                    if expected is None:expected=fp
                    assert fp==expected
                    report['runs'].append({'repeat':repeat,'mode':mode,'seconds':seconds,'full_projection_fingerprint':fp,'sql_sha256':digest})
                    print(repeat,mode,round(seconds,4),flush=True)
        report['summary']={mode:statistics.median(r['seconds'] for r in report['runs'] if r['mode']==mode) for mode in ('original','negative')}
        report['all_scratch_transactionally_rolled_back']=True;write_report(args.output,report)


if __name__=='__main__':main()
