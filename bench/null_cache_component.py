"""Compare empty-string cache compaction, with all private work rolled back."""
import argparse
import hashlib
from pathlib import Path
import re
import statistics
import tempfile
import time
import uuid
from psycopg import sql
from bench import parent_hint_probe as parent,null_cache_probe as null_cache
from bench.common import ROOT,connect,environment,require_lab,write_report
from bench.run import prepared
from pipeline.recipes import EXTRACT,destinations
from pipeline.workspace import RELATION
from tests.equivalence.oracle import protected_schema_signature,sequence_signature
from tests.recovery.bulk_dump import sequence_state


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_native_variant_6c1c55a43e')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/null-cache-components-1m.json')
    args=p.parse_args();parent.install()
    import pipeline.parallel_normalization as parallel
    import pipeline.guarded_normalization as guarded
    frozen=null_cache.source_identity()
    report={'kind':'rollback_only_empty_text_cache_compaction','official_score':False,'source_sha256':frozen,'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'runs':[],
            'scope':'Rotating original/NULL cache layout in one session, full normalized cache fingerprint and all six typed transforms. No complete pipeline percentage or final load/index/FK/commit/version timing.'}
    directory=Path(tempfile.mkdtemp(prefix='null_cache_',dir=ROOT/'.runtime')).resolve();path=directory/'transform.bin';expected={}
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    record="ROW(raw_id,"+','.join("NULLIF("+key+",'')" for key in fields)+',pipeline_domain_safe)'
    try:
        with connect(args.database,autocommit=True) as c:
            require_lab(c)
            assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'Do not overlap processing'
            report['environment']=environment(c)
            before=protected_schema_signature(c);seq=sequence_signature(c);values=sequence_state(c)
            scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')];recipes=prepared('guarded_copy',scopes)
            for repeat in range(args.repeats):
                for mode in (('original','nulls') if repeat%2==0 else ('nulls','original')):
                    assert null_cache.source_identity()==frozen
                    with c.transaction(force_rollback=True):
                        c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+parallel.settings_sql()+" SET LOCAL work_mem='64MB'; SET LOCAL statement_timeout='90s';")
                        schema='null_cache_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                        query=parallel.prepare_input_sql(scopes,logged=True)
                        if mode=='nulls':query=null_cache.projection(query)
                        query=query.replace('challenge_lab.pipeline_normalized_input',schema+'.input')
                        started=time.perf_counter();c.execute(query);projection_seconds=time.perf_counter()-started
                        started=time.perf_counter();safe=c.execute('SELECT coalesce(bool_and(pipeline_domain_safe),true) FROM '+schema+'.input').fetchone()[0];proof_seconds=time.perf_counter()-started;assert safe
                        stage_bytes=c.execute('SELECT pg_total_relation_size(%s::regclass)',(schema+'.input',)).fetchone()[0]
                        c.execute('ANALYZE '+schema+'.input(raw_id,text_column_1,text_column_2,text_column_3)')
                        fp=c.execute(f'SELECT count(*),sum(hash_record_extended({record},17)::numeric)::text,sum(hash_record_extended({record},97)::numeric)::text FROM {schema}.input').fetchone()
                        expected.setdefault('projection',fp);assert fp==expected['projection']
                        row={'repeat':repeat,'mode':mode,'projection_seconds':projection_seconds,'domain_scan_seconds':proof_seconds,'stage_bytes':stage_bytes,'complete_empty_normalized_projection_fingerprint':fp,'transforms':{}}
                        for recipe in recipes:
                            table=recipe['pure_table_name'];body=guarded.fast_statement(recipe['statements'][0],scopes,'guarded_copy')
                            if mode=='nulls':body=null_cache.statement(body)
                            body=body.replace(RELATION,schema+'.input')
                            select=re.fullmatch(r'INSERT INTO public\.table[1-6]\s*\(.*?\)\s*(SELECT\s.*?);?',body,re.S)[1].rstrip(';')
                            c.execute('SET LOCAL enable_parallel_hash='+('off' if table=='table1' else 'on'))
                            assert not path.exists()
                            started=time.perf_counter();cur=c.execute(sql.SQL('COPY ({}) TO {} (FORMAT BINARY)').format(sql.SQL(select),sql.Literal(path.as_posix())));seconds=time.perf_counter()-started
                            size=path.stat().st_size;path.unlink()
                            fp=c.execute('SELECT count(*),sum(hash_record_extended(q,17)::numeric)::text,sum(hash_record_extended(q,97)::numeric)::text FROM ('+select+') q').fetchone()
                            expected.setdefault(table,fp);assert fp==expected[table]
                            row['transforms'][table]={'seconds':seconds,'binary_bytes':size,'command_tag':cur.statusmessage,'complete_typed_fingerprint':fp}
                        row['component_seconds']=projection_seconds+proof_seconds+sum(v['seconds'] for v in row['transforms'].values())
                        report['runs'].append(row);write_report(args.output,report)
                        print(repeat,mode,round(row['component_seconds'],4),'projection',round(projection_seconds,4),'stage bytes',stage_bytes,flush=True)
            assert protected_schema_signature(c)==before and sequence_signature(c)==seq and sequence_state(c)==values
            report['summary']={mode:{'median_component_seconds':statistics.median(r['component_seconds'] for r in report['runs'] if r['mode']==mode),'median_projection_seconds':statistics.median(r['projection_seconds'] for r in report['runs'] if r['mode']==mode),'stage_bytes':next(r['stage_bytes'] for r in report['runs'] if r['mode']==mode)} for mode in ('original','nulls')}
            report['all_private_work_rolled_back_no_identity_allocation']=True;write_report(args.output,report)
    finally:path.unlink(missing_ok=True);directory.rmdir()


if __name__=='__main__':main()
