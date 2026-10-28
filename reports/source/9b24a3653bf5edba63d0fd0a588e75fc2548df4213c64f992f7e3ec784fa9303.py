"""Integer-exact quantization through binary64 inside the unchanged proof.

For |x|<1e6 with <=3 fractional digits, ideal x*1000 is an integer <1e9.
Binary64 decimal parsing and multiplication introduce <1e-6 absolute error
here, far below the 0.5 needed to change the native rounded int32 cast. The
final SQL integers are exact, not approximate outputs. Other inputs retain
the complete original fallback. This does not generalize to arbitrary numeric.
"""
import argparse
import hashlib
from pathlib import Path
import random
import re
import statistics
import tempfile
import time
import uuid
from psycopg import sql
from bench.common import ROOT,connect,require_lab,environment,write_report
from bench import t1_inline_probe as inline,text_stage_probe as text,calendar_proof_probe as calendar
from bench.run import prepared
from pipeline.guarded_normalization import fast_statement
from pipeline.parallel_normalization import settings_sql
from pipeline.workspace import RELATION
from tests.equivalence.oracle import protected_schema_signature,sequence_signature
from tests.recovery.bulk_dump import sequence_state


def source_identity():
    return hashlib.sha256((inline.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def expression(field):
    return "NULLIF(GREATEST((NULLIF("+field+",'')::double precision*1000::double precision)::integer,0),0)"


def statement(body):
    if not body.startswith('INSERT INTO public.table1 ('):return body
    for field in ('numeric_column_1','numeric_column_2'):
        old=inline.expression('source_row.'+field);assert body.count(old)==1
        body=body.replace(old,expression('source_row.'+field),1)
    return body


def differential(c):
    rng=random.Random(43117)
    values=[None,'','0','-0','+0','0.000','-0.000','0.001','-0.001','999999.999','-999999.999','+000001.010']
    # Every millesimal fraction across extreme and intermediate magnitudes,
    # both signs; additional uniform random values, optional + and zero padding.
    for whole in (0,1,9,10,99,100,999,1000,99999,100000,500000,999999):
        for fraction in range(1000):
            for sign in ('','-'):
                values.append(sign+str(whole)+'.'+str(fraction).zfill(3))
    for _ in range(10000):
        magnitude=rng.randrange(1000000000)
        values.append(rng.choice(('','+','-'))+str(magnitude//1000).zfill(6)+'.'+str(magnitude%1000).zfill(3))
    different=0
    for offset in range(0,len(values),5000):
        block=values[offset:offset+5000]
        query='SELECT count(*) FILTER(WHERE ('+inline.expression('x')+') IS DISTINCT FROM ('+expression('x')+')) FROM unnest(%s::text[]) n(x)'
        different+=c.execute(query,(block,)).fetchone()[0]
    assert different==0
    # Exhaust one million adjacent integer targets at the largest magnitude,
    # where binary64 ULP spacing and potential parse/multiply error are worst.
    query="SELECT count(*),count(*) FILTER(WHERE (x::numeric*1000)::integer IS DISTINCT FROM (x::double precision*1000::double precision)::integer) FROM (SELECT (n/1000)::text||'.'||lpad((n%1000)::text,3,'0') AS x FROM generate_series(999000000,999999999) n) q"
    count,diff=c.execute(query).fetchone();assert diff==0
    return {'signed_fraction_and_random_vectors':len(values),'extreme_adjacent_targets':count,'differences':different+diff,'mathematical_scope':'Finite binary64, |x|<1e6, at most three fractional digits; exact ideal target is an integer <1e9; combined absolute error <1e-6 <<0.5. Native dtoi4 rounds before int32 range checking. Unsafe values remain on original batch.'}


def component():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_native_variant_d58abcf90d')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/bounded-float-t1-components-1m.json')
    args=p.parse_args();frozen=source_identity();report={'kind':'rollback_only_integer_exact_bounded_binary64','official_score':False,'source_sha256':frozen,'runs':[]}
    parent=(ROOT/'.runtime/bounded_float').resolve();parent.mkdir(exist_ok=True)
    directory=Path(tempfile.mkdtemp(prefix='component_',dir=parent)).resolve();assert directory.parent==parent
    path=directory/'table1.bin'
    try:
        with connect(args.database,autocommit=True) as c:
            require_lab(c)
            assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'Do not overlap processing'
            report['environment']=environment(c);report['differential']=differential(c)
            before=protected_schema_signature(c);sequences=sequence_signature(c);values=sequence_state(c)
            scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
            recipe=next(r for r in prepared('guarded_copy',scopes) if r['pure_table_name']=='table1')
            base=inline.proven_trim_identity(inline.statement(text.statement(fast_statement(recipe['statements'][0],scopes,'guarded_copy'))))
            expected=None
            with c.transaction(force_rollback=True):
                c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB'; SET LOCAL enable_parallel_hash=off; SET LOCAL statement_timeout='90s';")
                schema='bounded_float_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                c.execute(calendar.projection(scopes,logged=True).replace('challenge_lab.pipeline_normalized_input',schema+'.input'))
                c.execute('ANALYZE '+schema+'.input(raw_id,text_column_1,text_column_2,text_column_3)')
                assert c.execute('SELECT count(*),bool_and(pipeline_domain_safe) FROM '+schema+'.input').fetchone()==(1000000,True)
                for repeat in range(args.repeats):
                    for mode in (('numeric','bounded_float') if repeat%2==0 else ('bounded_float','numeric')):
                        assert source_identity()==frozen
                        body=(base if mode=='numeric' else statement(base)).replace(RELATION,schema+'.input')
                        select=re.fullmatch(r'INSERT INTO public\.table1\s*\(.*?\)\s*(SELECT\s.*?);?',body,re.S)[1].rstrip(';')
                        assert not path.exists();started=time.perf_counter()
                        cur=c.execute(sql.SQL('COPY ({}) TO {} (FORMAT BINARY)').format(sql.SQL(select),sql.Literal(path.as_posix())));seconds=time.perf_counter()-started
                        size=path.stat().st_size;path.unlink()
                        fingerprint=c.execute('SELECT count(*),sum(hash_record_extended(q,17)::numeric)::text,sum(hash_record_extended(q,97)::numeric)::text FROM ('+select+') q').fetchone()
                        if expected is None:expected=fingerprint
                        assert fingerprint==expected and fingerprint[0]==1000000
                        report['runs'].append({'repeat':repeat,'mode':mode,'seconds':seconds,'binary_bytes':size,'command_tag':cur.statusmessage,'complete_typed_fingerprint':fingerprint});write_report(args.output,report)
                        print(repeat,mode,round(seconds,4),flush=True)
            assert protected_schema_signature(c)==before and sequence_signature(c)==sequences and sequence_state(c)==values
            report['summary']={mode:statistics.median(r['seconds'] for r in report['runs'] if r['mode']==mode) for mode in ('numeric','bounded_float')}
            report['all_private_work_rolled_back_no_identity_allocation']=True;write_report(args.output,report)
    finally:path.unlink(missing_ok=True);directory.rmdir()


def install():
    inline.install()
    import pipeline.guarded_normalization as guarded
    import pipeline.parallel_normalization as parallel
    original=guarded.fast_statement
    def transformed(original_sql,scopes,mode=guarded.MODE):
        body=original(original_sql,scopes,mode)
        return statement(body) if mode=='guarded_copy' and original_sql.startswith('INSERT INTO public.table1 (') else body
    guarded.fast_statement=transformed
    projection=parallel.prepare_input_sql
    parallel.prepare_input_sql=lambda scopes,logged=False:'-- lab integer-exact bounded binary64 source_sha256='+source_identity()+'\n'+projection(scopes,logged=logged)


def main():
    import sys
    if len(sys.argv)>1 and sys.argv[1]=='component':sys.argv.pop(1);component();return
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
