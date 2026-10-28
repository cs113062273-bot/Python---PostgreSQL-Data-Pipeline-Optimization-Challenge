"""Rollback-only same-session cache layout and all six native COPY transforms."""
import argparse
import hashlib
from pathlib import Path
import re
import statistics
import tempfile
import time
import uuid
from psycopg import sql
from bench import parent_hint_probe as parent, projection_layout_probe as layout
from bench.common import ROOT, connect, environment, require_lab, write_report
from bench.run import prepared
from pipeline.recipes import EXTRACT, destinations
from pipeline.workspace import RELATION
from tests.equivalence.oracle import protected_schema_signature, sequence_signature
from tests.recovery.bulk_dump import sequence_state


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_native_variant_6c1c55a43e')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/layout-components-1m.json')
    args=p.parse_args();parent.install()
    import pipeline.parallel_normalization as parallel
    import pipeline.guarded_normalization as guarded
    frozen=layout.source_identity();driver_sha=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    report={'kind':'rollback_only_cache_column_layout','official_score':False,'source_sha256':frozen,'driver_sha256':driver_sha,'runs':[],
            'scope':'Same session, rotating physical cache layouts, same named TEXT/proof values and six native binary COPY TO transforms. No COPY FROM, final indexes/FK/commit/versions; not a complete processing score.'}
    directory=Path(tempfile.mkdtemp(prefix='layout_',dir=ROOT/'.runtime')).resolve();path=directory/'transform.bin'
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    record='ROW('+','.join(['raw_id']+fields+['pipeline_domain_safe'])+')';expected={}
    try:
        with connect(args.database,autocommit=True) as c:
            require_lab(c)
            assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'Do not overlap processing'
            report['environment']=environment(c)
            before=protected_schema_signature(c);seq=sequence_signature(c);values=sequence_state(c)
            scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
            recipes=prepared('guarded_copy',scopes)
            for repeat in range(args.repeats):
                modes=['original','proof_first','text_first'];shift=repeat%len(modes)
                for mode in modes[shift:]+modes[:shift]:
                    assert layout.source_identity()==frozen
                    with c.transaction(force_rollback=True):
                        c.execute('LOCK TABLE public.raw_data IN SHARE MODE;'+parallel.settings_sql()+" SET LOCAL work_mem='64MB'; SET LOCAL statement_timeout='90s';")
                        schema='layout_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                        query=layout.reorder(parallel.prepare_input_sql(scopes,logged=True),mode).replace('challenge_lab.pipeline_normalized_input',schema+'.input')
                        started=time.perf_counter();c.execute(query);projection_seconds=time.perf_counter()-started
                        started=time.perf_counter();safe=c.execute('SELECT coalesce(bool_and(pipeline_domain_safe),true) FROM '+schema+'.input').fetchone()[0];proof_seconds=time.perf_counter()-started
                        assert safe
                        c.execute('ANALYZE '+schema+'.input(raw_id,text_column_1,text_column_2,text_column_3)')
                        fp=c.execute(f'SELECT count(*),sum(hash_record_extended({record},17)::numeric)::text,sum(hash_record_extended({record},97)::numeric)::text FROM {schema}.input').fetchone()
                        expected.setdefault('projection',fp);assert fp==expected['projection']
                        row={'repeat':repeat,'mode':mode,'projection_seconds':projection_seconds,'domain_scan_seconds':proof_seconds,'complete_named_projection_fingerprint':fp,'transforms':{}}
                        for recipe in recipes:
                            table=recipe['pure_table_name']
                            body=guarded.fast_statement(recipe['statements'][0],scopes,'guarded_copy').replace(RELATION,schema+'.input')
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
                        print(repeat,mode,round(row['component_seconds'],4),'projection',round(projection_seconds,4),'proof scan',round(proof_seconds,4),flush=True)
            assert protected_schema_signature(c)==before and sequence_signature(c)==seq and sequence_state(c)==values
            report['summary']={mode:{'median_component_seconds':statistics.median(r['component_seconds'] for r in report['runs'] if r['mode']==mode),'median_domain_scan_seconds':statistics.median(r['domain_scan_seconds'] for r in report['runs'] if r['mode']==mode)} for mode in modes}
            report['all_private_work_rolled_back_no_identity_allocation']=True;write_report(args.output,report)
    finally:
        path.unlink(missing_ok=True);directory.rmdir()


if __name__=='__main__':main()
