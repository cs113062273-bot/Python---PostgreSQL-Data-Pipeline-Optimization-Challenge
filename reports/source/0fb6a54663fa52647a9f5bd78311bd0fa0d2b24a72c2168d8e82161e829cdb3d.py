"""Equivalent Gregorian proof: common days need no year/day integer casts."""
import argparse
import hashlib
from pathlib import Path
import statistics
import sys
import time
import uuid
from bench.common import connect,require_lab,environment,write_report
from bench import refined_copy_probe as refined,text_stage_probe as text,memo_scalar_probe as scalar
from bench.proof_pattern_probe import negative_condition
from pipeline.safe_domain import condition_sql,condition_parts,literal,STAMP_SHAPE
from pipeline.parallel_normalization import settings_sql
from pipeline.recipes import EXTRACT,destinations


def source_identity():
    return hashlib.sha256((scalar.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def calendar():
    year='pg_catalog.substr(c.stamp,1,4)::integer'
    month='(pg_catalog.substr(c.stamp,6,2) COLLATE "C")'
    day='(pg_catalog.substr(c.stamp,9,2) COLLATE "C")'
    return '''(CASE WHEN c.stamp='' THEN true
      WHEN (c.stamp COLLATE "C") ~ '''+literal(STAMP_SHAPE)+''' THEN CASE
      WHEN (pg_catalog.substr(c.stamp,1,4) COLLATE "C")='0000' THEN false
      WHEN '''+day+'''<='28' THEN true
      WHEN '''+month+'''='02' THEN '''+day+'''='29' AND '''+year+'%4=0 AND ('+year+'%100<>0 OR '+year+'''%400=0)
      WHEN '''+day+'''='31' THEN '''+month+''' NOT IN ('04','06','09','11')
      ELSE true END ELSE false END)'''


def projection(scopes,*,logged=False):
    body=text.projection(scopes,logged=logged).replace(condition_sql(),negative_condition())
    marker=condition_parts()[3];assert body.count(marker)==1
    body=body.replace(marker,calendar(),1)
    if logged:body=body.replace('CREATE TABLE challenge_lab.pipeline_normalized_input AS','CREATE UNLOGGED TABLE challenge_lab.pipeline_normalized_input AS',1)
    return '-- lab equivalent Gregorian proof source_sha256='+source_identity()+'\n'+body


def differential(c):
    # A complete 400-year Gregorian cycle, every 01..31 day of every month,
    # including impossible dates. Shape fixtures add empty/invalid/time zones.
    old=condition_parts()[3];new=calendar()
    query="SELECT count(*),count(*) FILTER(WHERE ("+old+') IS DISTINCT FROM ('+new+")) FROM (SELECT lpad(y::text,4,'0')||'-'||lpad(m::text,2,'0')||'-'||lpad(d::text,2,'0')||' 23:59:59' AS stamp FROM generate_series(1600,1999) y CROSS JOIN generate_series(1,12) m CROSS JOIN generate_series(1,31) d) c"
    total,diff=c.execute(query).fetchone();assert diff==0
    fixtures=['','0000-01-01 00:00:00','0001-01-01 00:00:00','9999-12-31T23:59:59.123456+12:59','2000-02-29 00:00:00Z','1900-02-29 00:00:00','2000-02-30 00:00:00','2000-04-31 00:00:00','2000-12-31 24:00:00','2000-12-31 00:00:60','2000-12-31 00:00:00+13','2000-00-01 00:00:00','中文','NaN','2000-02-29 00:00:00\n','2000-02-29 00:00:00 ']
    count,different=c.execute('SELECT count(*),count(*) FILTER(WHERE ('+old.replace('%','%%')+') IS DISTINCT FROM ('+new.replace('%','%%')+')) FROM unnest(%s::text[]) c(stamp)',(fixtures,)).fetchone();assert different==0
    return {'full_400_year_cycle_vectors':total,'boundary_shape_vectors':count,'differences':diff+different}


def component():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_template_1m')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/calendar-proof-projection-1m.json')
    args=p.parse_args();frozen=source_identity()
    report={'kind':'rollback_only_equivalent_calendar_proof','official_score':False,'source_sha256':frozen,'runs':[]}
    with connect(args.database,autocommit=True) as c:
        require_lab(c)
        assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'Do not overlap a benchmark'
        report['environment']=environment(c);report['differential']=differential(c)
        fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
        record='ROW('+','.join(['raw_id']+fields+['pipeline_domain_safe'])+')'
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')];expected=None
        for repeat in range(args.repeats):
            for mode in (('refined','calendar') if repeat%2==0 else ('calendar','refined')):
                assert source_identity()==frozen
                with c.transaction(force_rollback=True):
                    c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB'; SET LOCAL statement_timeout='90s';")
                    schema='calendar_proof_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                    if mode=='refined':query=text.projection(scopes,logged=True).replace(condition_sql(),negative_condition()).replace('CREATE TABLE challenge_lab.pipeline_normalized_input AS','CREATE UNLOGGED TABLE challenge_lab.pipeline_normalized_input AS')
                    else:query=projection(scopes,logged=True)
                    query=query.replace('challenge_lab.pipeline_normalized_input',schema+'.input')
                    started=time.perf_counter();c.execute(query);seconds=time.perf_counter()-started
                    fp=c.execute(f'SELECT count(*),sum(hash_record_extended({record},17)::numeric)::text,sum(hash_record_extended({record},97)::numeric)::text FROM {schema}.input').fetchone()
                    if expected is None:expected=fp
                    assert fp==expected
                    report['runs'].append({'repeat':repeat,'mode':mode,'seconds':seconds,'complete_text_and_proof_fingerprint':fp});write_report(args.output,report)
                    print(repeat,mode,round(seconds,4),flush=True)
        report['summary']={mode:statistics.median(r['seconds'] for r in report['runs'] if r['mode']==mode) for mode in ('refined','calendar')}
        report['all_private_work_rolled_back']=True;write_report(args.output,report)


def install():
    scalar.install(tables=('table5',))
    import pipeline.parallel_normalization as parallel
    parallel.prepare_input_sql=projection


def main():
    if len(sys.argv)>1 and sys.argv[1]=='component':sys.argv.pop(1);component();return
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
