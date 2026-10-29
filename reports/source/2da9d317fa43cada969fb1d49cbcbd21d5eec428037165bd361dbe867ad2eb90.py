"""Normalize plus native table1 ingest and keys; three rotating exact modes.

This is a component experiment, with real normalization cost included. Private
temporary target, no final FK/commit/counters/versions; never a service score.
"""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import statistics
import tempfile
import time
from psycopg import sql
from bench import native_c_lab as lab,native_main_lab as typed,native_stamp_lab as stamp,native_compact_sql as compact,slim_copy_probe as slim
from bench.common import ROOT,require_lab,write_report
from bench.run import prepared
from pipeline.normalization import source_sql
from pipeline.parallel_normalization import settings_sql
from tests.equivalence.oracle import protected_schema_signature,sequence_signature


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--database',required=True);p.add_argument('--repeats',type=int,default=3);p.add_argument('--output',required=True);args=p.parse_args()
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    assert shutil.disk_usage(ROOT).free>2*1024**3
    report={'kind':'normalization_plus_table1_three_mode_component','database':args.database,'runs':[],'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'scope':'Real per-trial UNLOGGED normalization CTAS, proof checks, native identity table1 transform/ingest and temp PK/UNIQUE rebuild. All nonidentity native values/multiplicities compared outside timers. No final FK, durable commit, counters or versions. Not a complete-service percentage.'}
    parent=ROOT/'.runtime/native_stamp_component';parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='trial_',dir=parent) as directory,lab.connect(args.database,autocommit=True) as c:
        require_lab(c);before=protected_schema_signature(c);sequences=sequence_signature(c)
        with c.transaction(force_rollback=True):
            c.execute("SET LOCAL work_mem='128MB'; SET LOCAL maintenance_work_mem='128MB'; SET LOCAL statement_timeout='180s'")
            normalize13,axes13,build13=stamp.install_compact(c)
            # stamp.install_compact already installed the old pure extractor.
            old=json_old_build();normalize12,axes12=install_old_compact(c,old)
            loader,loadbuild=typed.install(c,'reports/native-main-typed-build.json')
            report.update({'normalizer13':build13,'normalizer12':old,'typed_loader':loadbuild})
            scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
            recipe=next(r['statements'][0] for r in prepared('baseline',scopes) if r['pure_table_name']=='table1')
            expected=c.execute('SELECT count(*) FROM public.table1').fetchone()[0]
            relation='native_lab.main_trial_cache'
            modes=('original_binary','stamp_binary','stamp_direct')
            queries={}
            for mode in modes[:2]:
                body=compact.statement(recipe,scopes,axes12,relation=relation) if mode=='original_binary' else stamp.statement(recipe,scopes,axes13,relation=relation)
                body,_=slim.compact(body,'table1');columns,expressions,distinct,suffix=slim.split_projection(body,'table1')
                queries[mode]=(columns,'SELECT '+','.join(expressions)+' '+suffix)
            for repeat in range(args.repeats):
                for mode in modes[repeat%3:]+modes[:repeat%3]:
                    with c.transaction(force_rollback=True):
                        c.execute('CREATE TEMP TABLE stamp_main_target (LIKE public.table1 INCLUDING ALL) ON COMMIT DROP')
                        restores=[]
                        for name,definition in c.execute("SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='pg_temp.stamp_main_target'::regclass AND contype IN ('p','u')").fetchall():
                            restores.append(sql.SQL('ALTER TABLE pg_temp.stamp_main_target ADD CONSTRAINT {} '+definition).format(sql.Identifier(name)))
                            c.execute(sql.SQL('ALTER TABLE pg_temp.stamp_main_target DROP CONSTRAINT {}').format(sql.Identifier(name)))
                        sequence=c.execute("SELECT pg_get_serial_sequence('pg_temp.stamp_main_target','id_column_1')").fetchone()[0]
                        c.execute('ALTER SEQUENCE '+sequence+' CACHE 1000');c.execute(settings_sql())
                        begin=time.perf_counter()
                        normalizer=normalize12 if mode=='original_binary' else normalize13
                        definition=lab.compact_definition() if mode=='original_binary' else stamp.compact_definition()
                        c.execute('CREATE UNLOGGED TABLE '+relation+' AS SELECT s.raw_id,n.* FROM '+source_sql(scopes)+" s CROSS JOIN LATERAL "+normalizer+"(s.raw->'values') n("+definition+')')
                        if mode=='stamp_direct':assert c.execute(typed.code_domain(relation)).fetchone()==(True,)
                        else:assert c.execute('SELECT coalesce(bool_and(pipeline_domain_safe),true) FROM '+relation).fetchone()==(True,)
                        c.execute('ANALYZE '+relation+'(raw_id,text_column_1,text_column_2,text_column_3)')
                        normalization=time.perf_counter()-begin
                        start=time.perf_counter();binary_bytes=0
                        if mode=='stamp_direct':
                            count=c.execute('SELECT '+loader+"(%s::regclass,'pg_temp.stamp_main_target','public.table3')",(relation,)).fetchone()[0]
                        else:
                            columns,query=queries[mode];path=(Path(directory)/'table1.bin').resolve();assert not path.exists()
                            try:
                                exported=c.execute(sql.SQL('COPY ('+query+') TO {} (FORMAT BINARY)').format(sql.Literal(path.as_posix()))).rowcount
                                binary_bytes=path.stat().st_size
                                count=c.execute(sql.SQL('COPY pg_temp.stamp_main_target ('+','.join(columns)+') FROM {} (FORMAT BINARY)').format(sql.Literal(path.as_posix()))).rowcount
                                assert count==exported
                            finally:path.unlink(missing_ok=True)
                        ingest=time.perf_counter()-start;assert count==expected
                        start=time.perf_counter()
                        for restore in restores:c.execute(restore)
                        indexes=time.perf_counter()-start
                        attrs=[r[0] for r in c.execute("SELECT attname FROM pg_attribute WHERE attrelid='public.table1'::regclass AND attnum>0 AND NOT attisdropped AND attname<>'id_column_1' ORDER BY attnum")]
                        fields=','.join(attrs)
                        assert not c.execute('SELECT EXISTS((SELECT '+fields+' FROM public.table1 EXCEPT ALL SELECT '+fields+' FROM pg_temp.stamp_main_target) UNION ALL (SELECT '+fields+' FROM pg_temp.stamp_main_target EXCEPT ALL SELECT '+fields+' FROM public.table1))').fetchone()[0]
                        report['runs'].append({'repeat':repeat,'mode':mode,'normalization_seconds':normalization,'ingest_seconds':ingest,'index_seconds':indexes,'seconds':normalization+ingest+indexes,'rows':count,'binary_bytes':binary_bytes,'all_native_nonidentity_values_and_multiplicities_exact':True,'native_identity_not_supplied':True})
                        write_report(output,report);print(repeat,mode,round(normalization+ingest+indexes,6),'exact rows',count,flush=True)
        assert protected_schema_signature(c)==before and sequence_signature(c)==sequences
    report['summary']={mode:statistics.median(r['seconds'] for r in report['runs'] if r['mode']==mode) for mode in modes}
    report['passed']=True;report['all_private_work_rolled_back']=True;write_report(output,report);print(report['summary'],flush=True)


def json_old_build():
    build=json.loads((ROOT/'reports/native-c-probe-build.json').read_text())
    assert hashlib.sha256((ROOT/'native/challenge_probe.c').read_bytes()).hexdigest()==build['source_sha256']
    assert hashlib.sha256((ROOT/build['library']).read_bytes()).hexdigest()==build['library_sha256']
    return build


def install_old_compact(c,build):
    library=(ROOT/build['library']).resolve();suffix=build['source_sha256'][:16]
    normal='compact12_'+suffix;axes='axes_'+suffix
    c.execute(sql.SQL("CREATE FUNCTION {}(jsonb) RETURNS record AS {},'challenge_compact12' LANGUAGE C IMMUTABLE PARALLEL SAFE CALLED ON NULL INPUT").format(sql.Identifier('native_lab',normal),sql.Literal(library.as_posix())))
    c.execute(sql.SQL("CREATE FUNCTION {}(bytea) RETURNS TABLE(position_number int2,weight int4,axis_kind text,gap int4) AS {},'challenge_axis_rows' LANGUAGE C IMMUTABLE PARALLEL SAFE CALLED ON NULL INPUT").format(sql.Identifier('native_lab',axes),sql.Literal(library.as_posix())))
    return 'native_lab.'+normal,'native_lab.'+axes


if __name__=='__main__':main()
