"""Skip text allocations only for groups proved entirely SQL NULL.

Every field is still extracted and the same proof string is built. A NULL
group contributes its exact original separators; any non-NULL malformed value
keeps the full original proof/fallback behavior. No position-based skipping.
"""
import argparse
import hashlib
from pathlib import Path
import random
import statistics
import sys
import time
import uuid
from bench.common import ROOT,connect,require_lab,environment,write_report
from bench import refined_copy_probe as refined,text_stage_probe as text
from bench.proof_pattern_probe import negative_condition
from pipeline.safe_domain import _bundle,condition_sql
from pipeline.parallel_normalization import settings_sql
from pipeline.recipes import EXTRACT,destinations


def source_identity():
    h=hashlib.sha256(refined.source_identity().encode());h.update(Path(__file__).read_text(encoding='utf8').encode());return h.hexdigest()


def sparse_bundle(prefix,count,minimum=4):
    fields=['n.'+prefix+'_'+str(i) for i in range(1,count+1)]
    def build(items):
        if len(items)==1:return "coalesce("+items[0]+",'')"
        middle=len(items)//2
        value='('+build(items[:middle])+"||'|'||"+build(items[middle:])+')'
        if len(items)>=minimum:
            value="CASE WHEN "+' AND '.join(field+' IS NULL' for field in items)+" THEN '"+'|'*(len(items)-1)+"' ELSE "+value+' END'
        return '('+value+')'
    return build(fields)


def projection(scopes,*,logged=False,minimum=4):
    body=text.projection(scopes,logged=logged).replace(condition_sql(),negative_condition())
    if logged:body=body.replace('CREATE TABLE challenge_lab.pipeline_normalized_input AS','CREATE UNLOGGED TABLE challenge_lab.pipeline_normalized_input AS',1)
    for prefix,count in (('integer_column',15),('numeric_column',30)):
        marker=_bundle(prefix,count).replace('c.','n.');assert body.count(marker)==1
        body=body.replace(marker,sparse_bundle(prefix,count,minimum),1)
    return '-- lab NULL-group proof minimum='+str(minimum)+' source_sha256='+source_identity()+'\n'+body


def component():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_template_1m')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/sparse-proof-projection-1m.json')
    args=p.parse_args();frozen=source_identity();report={'kind':'rollback_only_bitwise_equivalent_sparse_proof_bundle','official_score':False,'source_sha256':frozen,'runs':[]}
    rng=random.Random(33811);units=[None,'','0','2','bad','|','1|2','中文','\n',"'",'\\',' 2 ']
    with connect(args.database,autocommit=True) as c:
        require_lab(c);report['environment']=environment(c)
        assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'Do not overlap another benchmark'
        report['bundle_vector_checks']=[]
        for prefix,count in (('integer_column',15),('numeric_column',30)):
            rows=[tuple([None]*count),tuple(['']*count)]
            for i in range(count):
                for unit in units:
                    row=[None]*count;row[i]=unit;rows.append(tuple(row))
            rows.extend(tuple(rng.choice(units) for _ in range(count)) for _ in range(400))
            columns=','.join(prefix+'_'+str(i) for i in range(1,count+1))
            for minimum in (4,8):
                different=0
                for offset in range(0,len(rows),200):
                    block=rows[offset:offset+200]
                    values=','.join('('+','.join(['%s::text']*count)+')' for _ in block)
                    query='SELECT count(*) FILTER(WHERE ('+_bundle(prefix,count).replace('c.','n.')+') IS DISTINCT FROM ('+sparse_bundle(prefix,count,minimum)+')) FROM (VALUES '+values+') n('+columns+')'
                    different+=c.execute(query,tuple(value for row in block for value in row)).fetchone()[0]
                assert different==0
                report['bundle_vector_checks'].append({'prefix':prefix,'fields':count,'minimum':minimum,'vectors':len(rows),'bitwise_string_differences':different})
        fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])});record='ROW('+','.join(['raw_id']+fields+['pipeline_domain_safe'])+')';expected=None
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        for repeat in range(args.repeats):
            modes=['original','sparse4','sparse8'];shift=repeat%len(modes)
            for mode in modes[shift:]+modes[:shift]:
                assert source_identity()==frozen
                with c.transaction(force_rollback=True):
                    c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB'; SET LOCAL statement_timeout='90s';")
                    schema='sparse_proof_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                    if mode=='original':query=text.projection(scopes,logged=True).replace(condition_sql(),negative_condition()).replace('CREATE TABLE challenge_lab.pipeline_normalized_input AS','CREATE UNLOGGED TABLE challenge_lab.pipeline_normalized_input AS')
                    else:query=projection(scopes,logged=True,minimum=int(mode[-1]))
                    query=query.replace('challenge_lab.pipeline_normalized_input',schema+'.input')
                    started=time.perf_counter();c.execute(query);seconds=time.perf_counter()-started
                    fp=c.execute(f'SELECT count(*),sum(hash_record_extended({record},17)::numeric)::text,sum(hash_record_extended({record},97)::numeric)::text FROM {schema}.input').fetchone()
                    if expected is None:expected=fp
                    assert fp==expected
                    report['runs'].append({'repeat':repeat,'mode':mode,'seconds':seconds,'complete_text_and_proof_fingerprint':fp})
                    print(repeat,mode,round(seconds,4),flush=True);write_report(args.output,report)
        report['summary']={mode:statistics.median(row['seconds'] for row in report['runs'] if row['mode']==mode) for mode in modes};report['all_private_work_rolled_back']=True;write_report(args.output,report)


def main():
    if len(sys.argv)>1 and sys.argv[1]=='component':sys.argv.pop(1);component();return
    from bench.script_transport_probe import install
    install()
    import pipeline.parallel_normalization as parallel
    parallel.prepare_input_sql=lambda scopes,logged=False:projection(scopes,logged=logged,minimum=4)
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
