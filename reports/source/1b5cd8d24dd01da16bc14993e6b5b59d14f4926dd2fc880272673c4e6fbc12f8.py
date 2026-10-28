"""Shared rollback-only full TEXT/proof projection comparison harness."""
import argparse
import statistics
import time
import uuid
from bench.common import connect,require_lab,environment,write_report
from pipeline.parallel_normalization import settings_sql
from pipeline.recipes import EXTRACT,destinations


def component(producers,identity,default_output,*,differential=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_native_variant_d58abcf90d')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default=default_output)
    args=p.parse_args();frozen=identity()
    report={'kind':'rollback_only_complete_text_proof_projection_pair','official_score':False,'source_sha256':frozen,'runs':[],'scope':'Same native TEXT/proof CTAS, rotating order, full projection fingerprints outside timers. No full processing percentage.'}
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    record='ROW('+','.join(['raw_id']+fields+['pipeline_domain_safe'])+')';expected=None
    with connect(args.database,autocommit=True) as c:
        require_lab(c)
        assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'Do not overlap processing'
        report['environment']=environment(c)
        if differential:report['differential']=differential(c)
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        for repeat in range(args.repeats):
            modes=list(producers);shift=repeat%len(modes)
            for mode in modes[shift:]+modes[:shift]:
                assert identity()==frozen
                with c.transaction(force_rollback=True):
                    c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB'; SET LOCAL statement_timeout='90s';")
                    schema='projection_pair_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                    query=producers[mode](scopes,logged=True).replace('challenge_lab.pipeline_normalized_input',schema+'.input')
                    started=time.perf_counter();c.execute(query);seconds=time.perf_counter()-started
                    fp=c.execute(f'SELECT count(*),sum(hash_record_extended({record},17)::numeric)::text,sum(hash_record_extended({record},97)::numeric)::text FROM {schema}.input').fetchone()
                    if expected is None:expected=fp
                    assert fp==expected
                    report['runs'].append({'repeat':repeat,'mode':mode,'seconds':seconds,'complete_text_and_proof_fingerprint':fp});write_report(args.output,report)
                    print(repeat,mode,round(seconds,4),flush=True)
        report['summary']={mode:statistics.median(r['seconds'] for r in report['runs'] if r['mode']==mode) for mode in producers}
        report['all_private_work_rolled_back']=True;write_report(args.output,report)
