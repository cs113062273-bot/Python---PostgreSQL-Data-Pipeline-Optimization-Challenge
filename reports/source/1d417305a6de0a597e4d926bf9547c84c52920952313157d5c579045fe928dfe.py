"""Inline exact positive quantization after the unchanged complete proof.

At most three decimal places make value*1000 integral; six integer digits
bound its absolute value below 1e9. Thus a single int32 conversion plus native
integer clamping equals the original positive CASE/ROUND, including NULL and
signed zero. Outside that domain the complete original batch still executes.
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
from bench import calendar_proof_probe as calendar,text_stage_probe as text
from bench.run import prepared
from pipeline.guarded_normalization import fast_statement
from pipeline.parallel_normalization import settings_sql
from pipeline.workspace import RELATION
from pipeline.safe_domain import NUMERIC,literal
from tests.equivalence.oracle import protected_schema_signature,sequence_signature
from tests.recovery.bulk_dump import sequence_state


def source_identity():
    return hashlib.sha256((calendar.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def expression(field):
    return "NULLIF(GREATEST((NULLIF("+field+",'')::numeric*1000)::integer,0),0)"


def statement(body):
    if not body.startswith('INSERT INTO public.table1 ('):return body
    pattern=r"CASE\s+WHEN\s+NULLIF\(BTRIM\(source_row\.(numeric_column_[12])\),\s*''\)::numeric\s*>\s*0\s+THEN\s+ROUND\(\s*NULLIF\(BTRIM\(source_row\.\1\),\s*''\)::numeric\s*\*\s*1000\s*\)::integer\s+ELSE\s+NULL::integer\s+END"
    matches=list(re.finditer(pattern,body,re.I))
    assert len(matches)==2 and {m[1] for m in matches}=={'numeric_column_1','numeric_column_2'}
    for match in reversed(matches):body=body[:match.start()]+expression('source_row.'+match[1])+body[match.end():]
    return body


def proven_trim_identity(body):
    # The unchanged proof admits only NULL, empty or whitespace-free numeric,
    # integer, identifier and timestamp strings. BTRIM is the identity there.
    # Match only direct references in trusted canonical SQL; text fields were
    # separately normalized by the existing producer and are not part of this
    # rule. Unsafe/padded data still selects the complete original batch.
    fields={f'integer_column_{i}' for i in range(1,16)}|{f'numeric_column_{i}' for i in range(1,31)}|{'id_column_1','datetime_column_1'}
    pattern=r'\bBTRIM\s*\(\s*(source_row\.([a-z_]+\d+))\s*\)'
    return re.sub(pattern,lambda m:m[1] if m[2] in fields else m[0],body,flags=re.I)


def differential(c):
    rng=random.Random(33149)
    values=[None,'','0','-0','+0','0.000','-0.000','0.001','-0.001','999999.999','-999999.999','+000001.010']
    for _ in range(3000):
        magnitude=rng.randrange(1000000000);values.append(('-' if rng.randrange(2) else '')+str(magnitude//1000)+'.'+str(magnitude%1000).zfill(3))
    original="CASE WHEN NULLIF(BTRIM(x),'')::numeric>0 THEN ROUND(NULLIF(BTRIM(x),'')::numeric*1000)::integer ELSE NULL::integer END"
    query='SELECT count(*),count(*) FILTER(WHERE ('+original+') IS DISTINCT FROM ('+expression('x')+')) FROM unnest(%s::text[]) n(x)'
    total,different=c.execute(query,(values,)).fetchone();assert different==0
    return {'safe_numeric_vectors':total,'differences':different,'reason':'Unchanged full proof excludes whitespace, >3 fractional digits and magnitude outside int32 scaling bounds.'}


def component():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_native_variant_d58abcf90d')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/t1-inline-components-1m.json')
    args=p.parse_args();frozen=source_identity()
    report={'kind':'rollback_only_table1_inline_quantization','official_score':False,'source_sha256':frozen,'runs':[],'scope':'Same normalized/proven 1M cache and same public dimensions, rotating order. Real binary COPY TO and complete typed transform fingerprints; no final insertion/index/FK/version/commit cost. Not a full processing score.'}
    parent=(ROOT/'.runtime/t1_inline').resolve();parent.mkdir(exist_ok=True)
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
            base=text.statement(fast_statement(recipe['statements'][0],scopes,'guarded_copy'))
            expected=None
            with c.transaction(force_rollback=True):
                c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB'; SET LOCAL enable_parallel_hash=off; SET LOCAL statement_timeout='90s';")
                schema='inline_t1_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                query=calendar.projection(scopes,logged=True).replace('challenge_lab.pipeline_normalized_input',schema+'.input')
                c.execute(query);c.execute('ANALYZE '+schema+'.input(raw_id,text_column_1,text_column_2,text_column_3)')
                assert c.execute('SELECT count(*),bool_and(pipeline_domain_safe) FROM '+schema+'.input').fetchone()==(1000000,True)
                for repeat in range(args.repeats):
                    modes=['calendar','inline','inline_trim'];shift=repeat%len(modes)
                    for mode in modes[shift:]+modes[:shift]:
                        assert source_identity()==frozen
                        body=base if mode=='calendar' else statement(base)
                        if mode=='inline_trim':body=proven_trim_identity(body)
                        body=body.replace(RELATION,schema+'.input')
                        select=re.fullmatch(r'INSERT INTO public\.table1\s*\(.*?\)\s*(SELECT\s.*?);?',body,re.S)[1].rstrip(';')
                        assert not path.exists()
                        started=time.perf_counter();cur=c.execute(sql.SQL('COPY ({}) TO {} (FORMAT BINARY)').format(sql.SQL(select),sql.Literal(path.as_posix())));seconds=time.perf_counter()-started
                        size=path.stat().st_size;path.unlink()
                        fingerprint=c.execute('SELECT count(*),sum(hash_record_extended(q,17)::numeric)::text,sum(hash_record_extended(q,97)::numeric)::text FROM ('+select+') q').fetchone()
                        assert fingerprint[0]==1000000
                        if expected is None:expected=fingerprint
                        assert fingerprint==expected
                        row={'repeat':repeat,'mode':mode,'seconds':seconds,'binary_bytes':size,'command_tag':cur.statusmessage,'complete_typed_fingerprint':fingerprint}
                        if repeat==0:row['plan']=c.execute('EXPLAIN (FORMAT JSON) '+select).fetchone()[0]
                        report['runs'].append(row);write_report(args.output,report);print(repeat,mode,round(seconds,4),flush=True)
            assert protected_schema_signature(c)==before and sequence_signature(c)==sequences and sequence_state(c)==values
            report['summary']={mode:statistics.median(r['seconds'] for r in report['runs'] if r['mode']==mode) for mode in modes}
            report['all_private_work_rolled_back_no_identity_allocation']=True;write_report(args.output,report)
    finally:path.unlink(missing_ok=True);directory.rmdir()


def install():
    calendar.install()
    import pipeline.guarded_normalization as guarded
    import pipeline.parallel_normalization as parallel
    original=guarded.fast_statement
    def transformed(original_sql,scopes,mode=guarded.MODE):
        body=original(original_sql,scopes,mode)
        if mode=='guarded_copy':
            if original_sql.startswith('INSERT INTO public.table1 ('):body=statement(body)
            body=proven_trim_identity(body)
        return body
    guarded.fast_statement=transformed
    projection=parallel.prepare_input_sql
    parallel.prepare_input_sql=lambda scopes,logged=False:'-- lab table1 inline quantization source_sha256='+source_identity()+'\n'+projection(scopes,logged=logged)


def main():
    import sys
    if len(sys.argv)>1 and sys.argv[1]=='component':sys.argv.pop(1);component();return
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
