"""Same-session refined/scaled SQL kernels, never a complete pipeline score."""
import argparse
import hashlib
from pathlib import Path
import re
import statistics
import tempfile
import time
import uuid
from psycopg import sql
from bench.common import ROOT,connect,require_lab,environment,write_report
from bench.run import prepared
from bench import text_stage_probe as text,scaled_int_probe as scaled
from bench.proof_pattern_probe import negative_condition
from pipeline.safe_domain import condition_sql
from pipeline.parallel_normalization import settings_sql
from pipeline.guarded_normalization import fast_statement
from pipeline.workspace import RELATION
from tests.equivalence.oracle import protected_schema_signature,sequence_signature
from tests.recovery.bulk_dump import sequence_state


def identity():
    h=hashlib.sha256(scaled.source_identity().encode());h.update(Path(__file__).read_text(encoding='utf8').encode());return h.hexdigest()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_native_variant_4f58c93343')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/scaled-refined-kernels-1m.json')
    args=p.parse_args();frozen=identity()
    report={'kind':'rollback_only_refined_scaled_sql_kernels','official_score':False,'source_sha256':frozen,'runs':[],'scope':'Same session, rotating order, projection/ANALYZE/domain aggregation plus six transforms exported to real binary files. No final COPY FROM, index/FK rebuild, metadata/versions or commit. Complete typed transform fingerprints outside timers; no native identity allocation or source/final-table writes.'}
    parent=(ROOT/'.runtime/scaled_kernels').resolve();parent.mkdir(exist_ok=True)
    directory=Path(tempfile.mkdtemp(prefix='kernels_',dir=parent)).resolve();assert directory.parent==parent
    path=directory/'transform.bin';expected={}
    try:
        with connect(args.database,autocommit=True) as c:
            require_lab(c)
            assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'Do not overlap a processing benchmark'
            report['environment']=environment(c);scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
            schema_before=protected_schema_signature(c);sequence_before=sequence_signature(c);values_before=sequence_state(c)
            recipes=prepared('guarded_copy',scopes)
            for repeat in range(args.repeats):
                for mode in (('refined','scaled') if repeat%2==0 else ('scaled','refined')):
                    assert identity()==frozen
                    with c.transaction(force_rollback=True):
                        c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB'; SET LOCAL statement_timeout='90s';")
                        schema='kernel_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                        if mode=='refined':
                            query=text.projection(scopes,logged=True).replace(condition_sql(),negative_condition()).replace('CREATE TABLE challenge_lab.pipeline_normalized_input AS','CREATE UNLOGGED TABLE challenge_lab.pipeline_normalized_input AS')
                        else:query=scaled.projection(scopes,logged=True)
                        query=query.replace('challenge_lab.pipeline_normalized_input',schema+'.input')
                        started=time.perf_counter();c.execute(query);c.execute('ANALYZE '+schema+'.input(raw_id,text_column_1,text_column_2,text_column_3)')
                        assert c.execute('SELECT bool_and(pipeline_domain_safe) FROM '+schema+'.input').fetchone()[0]
                        projection_seconds=time.perf_counter()-started
                        row={'repeat':repeat,'mode':mode,'projection_proof_analyze_seconds':projection_seconds,'transforms':{}}
                        print(repeat,mode,'projection',round(projection_seconds,4),flush=True)
                        for recipe in recipes:
                            table=recipe['pure_table_name']
                            statement=text.statement(fast_statement(recipe['statements'][0],scopes,'guarded_copy'))
                            if mode=='scaled':statement=scaled.statement(statement)
                            statement=statement.replace(RELATION,schema+'.input')
                            select=re.fullmatch(r'INSERT INTO public\.table[1-6]\s*\(.*?\)\s*(SELECT\s.*?);?',statement,re.S)[1].rstrip(';')
                            c.execute('SET LOCAL enable_parallel_hash='+('off' if table=='table1' else 'on'))
                            if repeat==0 and table in ('table1','table2'):
                                row.setdefault('plans',{})[table]=c.execute('EXPLAIN (FORMAT JSON) '+select).fetchone()[0]
                            assert not path.exists()
                            started=time.perf_counter();cur=c.execute(sql.SQL('COPY ({}) TO {} (FORMAT BINARY)').format(sql.SQL(select),sql.Literal(path.as_posix())));seconds=time.perf_counter()-started
                            size=path.stat().st_size;path.unlink()
                            fp=c.execute('SELECT count(*),sum(hash_record_extended(q,17)::numeric)::text,sum(hash_record_extended(q,97)::numeric)::text FROM ('+select+') q').fetchone()
                            if table not in expected:expected[table]=fp
                            assert fp==expected[table],(repeat,mode,table,fp,expected[table])
                            row['transforms'][table]={'seconds':seconds,'binary_bytes':size,'command_tag':cur.statusmessage,'full_typed_transform_fingerprint':fp}
                            print(repeat,mode,table,round(seconds,4),flush=True)
                        row['kernel_seconds']=projection_seconds+sum(t['seconds'] for t in row['transforms'].values())
                        report['runs'].append(row);write_report(args.output,report)
                    assert protected_schema_signature(c)==schema_before and sequence_signature(c)==sequence_before and sequence_state(c)==values_before
            report['summary']={mode:{'median_kernel_seconds':statistics.median(r['kernel_seconds'] for r in report['runs'] if r['mode']==mode),'individual_kernel_seconds':[r['kernel_seconds'] for r in report['runs'] if r['mode']==mode]} for mode in ('refined','scaled')}
            report['all_private_work_rolled_back_and_native_sequences_unchanged']=True;write_report(args.output,report)
    finally:path.unlink(missing_ok=True);directory.rmdir()


if __name__=='__main__':main()
