"""Rotating native binary-file versus direct typed axis COPY, all rows exact."""
import argparse
import hashlib
from pathlib import Path
import shutil
import statistics
import tempfile
import time
from psycopg import sql
from bench import native_c_lab as lab,native_axis_lab as axis,native_compact_sql as compact,slim_copy_probe as slim
from bench.common import ROOT,write_report,require_lab
from bench.run import prepared
from pipeline.normalization import source_sql
from pipeline.parallel_normalization import settings_sql
from tests.equivalence.oracle import protected_schema_signature,sequence_signature


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--database',required=True);p.add_argument('--repeats',type=int,default=3);p.add_argument('--output',required=True);args=p.parse_args()
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    assert shutil.disk_usage(ROOT).free>1536*1024**2
    report={'kind':'native_axis_typed_vs_binary_copy_component','database':args.database,'runs':[],
            'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'scope':'Axis transformation plus native identity/CHECK/WAL ingestion into a temporary isolated receiving relation; shared warmed compact cache and actual committed parents. Every output value and multiplicity compares by full EXCEPT ALL outside timers. Trials roll back. No final index/FK/commit/counter/version or full-service percentage claim.'}
    parent=ROOT/'.runtime/native_axis_component';parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='trial_',dir=parent) as directory,lab.connect(args.database,autocommit=True) as c:
        require_lab(c);before=protected_schema_signature(c);sequences=sequence_signature(c)
        with c.transaction(force_rollback=True):
            c.execute("SET LOCAL work_mem='128MB'; SET LOCAL statement_timeout='180s'")
            normalize,axes,normbuild=lab.install_compact(c);loader,build=axis.install(c)
            report.update({'normalizer_build':normbuild,'loader_build':build})
            scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
            relation='native_lab.axis_component_cache'
            c.execute(settings_sql())
            c.execute('CREATE UNLOGGED TABLE '+relation+' AS SELECT s.raw_id,n.* FROM '+source_sql(scopes)+" s CROSS JOIN LATERAL "+normalize+"(s.raw->'values') n("+lab.compact_definition()+')')
            assert c.execute('SELECT count(*),bool_and(pipeline_domain_safe) FROM '+relation).fetchone()==(1000000,True)
            c.execute('ANALYZE '+relation+'(raw_id,text_column_1,text_column_2,text_column_3)')
            original=next(r['statements'][0] for r in prepared('baseline',scopes) if r['pure_table_name']=='table2')
            statement,_=slim.compact(compact.statement(original,scopes,axes,relation=relation),'table2')
            columns,expressions,distinct,suffix=slim.split_projection(statement,'table2')
            query='SELECT '+','.join(expressions)+' '+suffix
            expected=c.execute('SELECT count(*) FROM public.table2').fetchone()[0]
            for repeat in range(args.repeats):
                for mode in (('binary_file','typed_source') if repeat%2==0 else ('typed_source','binary_file')):
                    with c.transaction(force_rollback=True):
                        c.execute('CREATE TEMP TABLE axis_component_target (LIKE public.table2 INCLUDING ALL) ON COMMIT DROP')
                        # Native test target keeps the original CHECK/defaults;
                        # rebuildable indexes are absent in both timed modes.
                        for name,kind in c.execute("SELECT conname,contype FROM pg_constraint WHERE conrelid='pg_temp.axis_component_target'::regclass AND contype IN ('p','u')").fetchall():
                            c.execute(sql.SQL('ALTER TABLE pg_temp.axis_component_target DROP CONSTRAINT {}').format(sql.Identifier(name)))
                        seq=c.execute("SELECT pg_get_serial_sequence('pg_temp.axis_component_target','id_column_1')").fetchone()[0]
                        c.execute('ALTER SEQUENCE '+seq+' CACHE 1000')
                        c.execute(settings_sql())
                        start=time.perf_counter();binary_bytes=0
                        if mode=='binary_file':
                            path=(Path(directory)/'table2.bin').resolve();assert not path.exists()
                            try:
                                exported=c.execute(sql.SQL('COPY ('+query+') TO {} (FORMAT BINARY)').format(sql.Literal(path.as_posix()))).rowcount
                                binary_bytes=path.stat().st_size
                                inserted=c.execute(sql.SQL('COPY pg_temp.axis_component_target ('+','.join(columns)+') FROM {} (FORMAT BINARY)').format(sql.Literal(path.as_posix()))).rowcount
                                assert exported==inserted
                            finally:path.unlink(missing_ok=True)
                        else:inserted=c.execute('SELECT '+loader+"(%s::regclass,'public.table1','pg_temp.axis_component_target')",(relation,)).fetchone()[0]
                        elapsed=time.perf_counter()-start;assert inserted==expected
                        keys='id_column_2,integer_column_1,integer_column_2,text_column_1,integer_column_3,integer_column_4,integer_column_5,integer_column_6'
                        assert not c.execute('SELECT EXISTS((SELECT '+keys+' FROM public.table2 EXCEPT ALL SELECT '+keys+' FROM pg_temp.axis_component_target) UNION ALL (SELECT '+keys+' FROM pg_temp.axis_component_target EXCEPT ALL SELECT '+keys+' FROM public.table2))').fetchone()[0]
                        row={'repeat':repeat,'mode':mode,'seconds':elapsed,'rows':inserted,'binary_bytes':binary_bytes,'every_nonidentity_field_and_multiplicity_exact':True,'native_identity_not_supplied':True}
                        report['runs'].append(row);write_report(output,report)
                        print(repeat,mode,round(elapsed,6),'exact rows',inserted,flush=True)
        assert protected_schema_signature(c)==before and sequence_signature(c)==sequences
    report['summary']={mode:statistics.median(r['seconds'] for r in report['runs'] if r['mode']==mode) for mode in ('binary_file','typed_source')}
    report['all_passed']=True;report['all_private_work_rolled_back']=True;write_report(output,report);print(report['summary'],flush=True)


if __name__=='__main__':main()
