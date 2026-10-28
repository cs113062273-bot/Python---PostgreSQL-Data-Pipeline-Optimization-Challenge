"""Skip the strict numeric expression chain for NULL inputs after full proof.

The ProjectSet proof still examines every original field. This changes only
CASE evaluation for a NULL scalar, whose original strict cast/multiply chain
also returns NULL. Non-NULL and unsafe-input behavior stays unchanged.
"""
import argparse
import hashlib
from pathlib import Path
import statistics
import sys
import time
import uuid
from bench.common import ROOT,connect,require_lab,environment,write_report
from bench import scaled_int_probe as scaled,refined_copy_probe as refined,text_stage_probe as text
from bench.proof_pattern_probe import negative_condition
from pipeline.safe_domain import condition_sql
from pipeline.parallel_normalization import settings_sql
from pipeline.recipes import EXTRACT,destinations


def source_identity():
    digest=hashlib.sha256(scaled.source_identity().encode());digest.update(Path(__file__).read_text(encoding='utf-8').encode());return digest.hexdigest()


def projection(scopes,*,logged=False):
    body=scaled.projection(scopes,logged=logged)
    for index in range(1,31):
        field='v.numeric_column_'+str(index)
        marker='CASE WHEN v.pipeline_domain_safe THEN (NULLIF(BTRIM('+field+')'
        assert body.count(marker)==1
        replacement='CASE WHEN '+field+' IS NULL THEN NULL WHEN v.pipeline_domain_safe THEN (NULLIF(BTRIM('+field+')'
        body=body.replace(marker,replacement,1)
    return '-- lab NULL-first exact scaled int32 source_sha256='+source_identity()+'\n'+body


def component():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_template_1m')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/scaled-null-first-projection-1m.json')
    args=p.parse_args();frozen=source_identity()
    report={'kind':'rollback_only_scaled_projection_comparison','official_score':False,'source_sha256':frozen,'runs':[],'scope':'Native CTAS extraction/proof/scaling only. No final transformation/load, counters, keys/FKs, versions or commit. Typed stage fingerprints compare complete contents outside the timer. No source/final-table writes.'}
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    with connect(args.database,autocommit=True) as c:
        require_lab(c)
        others=c.execute("SELECT datname,pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall()
        assert not others,('Do not overlap a processing benchmark',others)
        report['environment']=environment(c);scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')];expected=None
        for repeat in range(args.repeats):
            modes=['refined_text','scaled_int','null_first_scaled_int'];shift=repeat%len(modes)
            for mode in modes[shift:]+modes[:shift]:
                assert source_identity()==frozen
                with c.transaction(force_rollback=True):
                    c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB'; SET LOCAL statement_timeout='90s';")
                    schema='scaled_null_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                    if mode=='refined_text':
                        query=text.projection(scopes,logged=True).replace(condition_sql(),negative_condition()).replace('CREATE TABLE challenge_lab.pipeline_normalized_input AS','CREATE UNLOGGED TABLE challenge_lab.pipeline_normalized_input AS')
                    else:query=(scaled.projection if mode=='scaled_int' else projection)(scopes,logged=True)
                    query=query.replace('challenge_lab.pipeline_normalized_input',schema+'.input')
                    started=time.perf_counter();c.execute(query);seconds=time.perf_counter()-started
                    expressions=['raw_id']
                    for field in fields:
                        expressions.append("CASE WHEN pipeline_domain_safe THEN (NULLIF(BTRIM("+field+"),'')::numeric*1000)::integer END" if mode=='refined_text' and field.startswith('numeric_column_') else field)
                    expressions.append('pipeline_domain_safe');record='ROW('+','.join(expressions)+')'
                    fp=c.execute(f'SELECT count(*),sum(hash_record_extended({record},17)::numeric)::text,sum(hash_record_extended({record},97)::numeric)::text FROM {schema}.input').fetchone()
                    if expected is None:expected=fp
                    assert fp==expected,(mode,fp,expected)
                    size=c.execute('SELECT pg_total_relation_size(%s::regclass)',(schema+'.input',)).fetchone()[0]
                    report['runs'].append({'repeat':repeat,'mode':mode,'seconds':seconds,'full_typed_projection_fingerprint':fp,'stage_bytes':size})
                    print(repeat,mode,round(seconds,4),flush=True);write_report(args.output,report)
        report['summary']={mode:statistics.median(row['seconds'] for row in report['runs'] if row['mode']==mode) for mode in modes};report['all_scratch_transactionally_rolled_back']=True;write_report(args.output,report)


def main():
    if len(sys.argv)>1 and sys.argv[1]=='component':sys.argv.pop(1);component();return
    from bench.script_transport_probe import install
    install('scaled')
    import pipeline.parallel_normalization as parallel
    parallel.prepare_input_sql=projection
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
