"""Actual table5/6/2 binary files versus shared typed COPY, rotating exact runs."""
import argparse
import hashlib
from pathlib import Path
import shutil
import statistics
import tempfile
import time
from psycopg import sql
from bench import native_c_lab as lab,native_details_lab as details,native_compact_sql as compact,slim_copy_probe as slim
from bench.common import ROOT,write_report,require_lab
from bench.run import prepared
from pipeline.normalization import source_sql
from pipeline.parallel_normalization import settings_sql
from tests.equivalence.oracle import protected_schema_signature,sequence_signature


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--database',required=True);p.add_argument('--repeats',type=int,default=3);p.add_argument('--output',required=True);p.add_argument('--build-report',default='reports/native-details-ingest-build.json');p.add_argument('--work-mem-mb',type=int,choices=[128,256,512],default=128);p.add_argument('--include-indexes',action='store_true');args=p.parse_args()
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    assert shutil.disk_usage(ROOT).free>1536*1024**2
    report={'kind':'native_three_details_typed_vs_binary_component','database':args.database,'runs':[],
            'includes_actual_native_index_builds':args.include_indexes,
            'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'scope':'Table5 then table6 then table2 transformations and actual native identity/CHECK COPY insertion; shared warmed compact cache and committed original parents/dimensions. All nonidentity values/multiplicities compared by full EXCEPT ALL outside timers. Private temporary destinations; optional native PK/UNIQUE/partial-index rebuilds are controlled by includes_actual_native_index_builds. No public FK validation, durable commit, counters or versions; not a full-service percentage.'}
    parent=ROOT/'.runtime/native_details_component';parent.mkdir(exist_ok=True)
    tables=('table5','table6','table2')
    with tempfile.TemporaryDirectory(prefix='trial_',dir=parent) as directory,lab.connect(args.database,autocommit=True) as c:
        require_lab(c);before=protected_schema_signature(c);sequences=sequence_signature(c)
        with c.transaction(force_rollback=True):
            c.execute("SET LOCAL work_mem='"+str(args.work_mem_mb)+"MB'; SET LOCAL statement_timeout='180s'")
            normalize,axes,normbuild=lab.install_compact(c);loader,build=details.install(c,args.build_report)
            report.update({'normalizer_build':normbuild,'loader_build':build})
            scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
            relation='native_lab.details_component_cache';c.execute(settings_sql())
            c.execute('CREATE UNLOGGED TABLE '+relation+' AS SELECT s.raw_id,n.* FROM '+source_sql(scopes)+" s CROSS JOIN LATERAL "+normalize+"(s.raw->'values') n("+lab.compact_definition()+')')
            assert c.execute('SELECT count(*) FROM '+relation).fetchone()==(1000000,) and c.execute(details.code_domain(relation)).fetchone()==(True,)
            c.execute('ANALYZE '+relation+'(raw_id,text_column_1,text_column_2,text_column_3)')
            recipes={r['pure_table_name']:r['statements'][0] for r in prepared('baseline',scopes)};queries={};expected={}
            for table in tables:
                statement,_=slim.compact(compact.statement(recipes[table],scopes,axes,relation=relation),table)
                columns,expressions,distinct,suffix=slim.split_projection(statement,table)
                queries[table]=(columns,'SELECT '+','.join(expressions)+' '+suffix)
                expected[table]=c.execute('SELECT count(*) FROM public.'+table).fetchone()[0]
            for repeat in range(args.repeats):
                for mode in (('binary_files','typed_shared') if repeat%2==0 else ('typed_shared','binary_files')):
                    with c.transaction(force_rollback=True):
                        restores=[]
                        for table in tables:
                            target='details_'+table
                            c.execute('CREATE TEMP TABLE '+target+' (LIKE public.'+table+' INCLUDING ALL) ON COMMIT DROP')
                            for name,definition in c.execute('SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=%s::regclass AND contype IN (\'p\',\'u\')',('pg_temp.'+target,)).fetchall():
                                restores.append(sql.SQL('ALTER TABLE {} ADD CONSTRAINT {} '+definition).format(sql.Identifier('pg_temp',target),sql.Identifier(name)))
                                c.execute(sql.SQL('ALTER TABLE {} DROP CONSTRAINT {}').format(sql.Identifier('pg_temp',target),sql.Identifier(name)))
                            for name,definition in c.execute("SELECT x.relname,pg_get_indexdef(x.oid) FROM pg_index i JOIN pg_class x ON x.oid=i.indexrelid WHERE i.indrelid=%s::regclass AND pg_get_expr(i.indpred,i.indrelid)='(id_column_3 IS NULL)'",('pg_temp.'+target,)).fetchall():
                                restores.append(definition);c.execute(sql.SQL('DROP INDEX {}').format(sql.Identifier('pg_temp',name)))
                            seq=c.execute('SELECT pg_get_serial_sequence(%s,\'id_column_1\')',('pg_temp.'+target,)).fetchone()[0]
                            c.execute('ALTER SEQUENCE '+seq+' CACHE 1000')
                        c.execute(settings_sql())
                        start=time.perf_counter();binary_bytes=0;counts={}
                        if mode=='binary_files':
                            for table in tables:
                                columns,query=queries[table];path=(Path(directory)/(table+'.bin')).resolve();assert not path.exists()
                                try:
                                    exported=c.execute(sql.SQL('COPY ('+query+') TO {} (FORMAT BINARY)').format(sql.Literal(path.as_posix()))).rowcount
                                    binary_bytes+=path.stat().st_size
                                    inserted=c.execute(sql.SQL('COPY pg_temp.details_'+table+' ('+','.join(columns)+') FROM {} (FORMAT BINARY)').format(sql.Literal(path.as_posix()))).rowcount
                                    assert exported==inserted;counts[table]=inserted
                                finally:path.unlink(missing_ok=True)
                        else:
                            actual=c.execute('SELECT '+loader+"(%s::regclass,'public.table1','pg_temp.details_table5','pg_temp.details_table6','pg_temp.details_table2','public.table4')",(relation,)).fetchone()[0]
                            assert len(actual)==3;counts=dict(zip(tables,actual))
                        ingest_seconds=time.perf_counter()-start;assert counts==expected
                        index_start=time.perf_counter()
                        if args.include_indexes:
                            c.execute("SET LOCAL maintenance_work_mem='128MB'")
                            for restore in restores:c.execute(restore)
                        index_seconds=time.perf_counter()-index_start if args.include_indexes else 0
                        elapsed=ingest_seconds+index_seconds
                        for table in tables:
                            attrs=[r[0] for r in c.execute('SELECT attname FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped AND attname<>\'id_column_1\' ORDER BY attnum',('public.'+table,)).fetchall()]
                            keys=','.join(attrs)
                            assert not c.execute('SELECT EXISTS((SELECT '+keys+' FROM public.'+table+' EXCEPT ALL SELECT '+keys+' FROM pg_temp.details_'+table+') UNION ALL (SELECT '+keys+' FROM pg_temp.details_'+table+' EXCEPT ALL SELECT '+keys+' FROM public.'+table+'))').fetchone()[0]
                        report['runs'].append({'repeat':repeat,'mode':mode,'seconds':elapsed,'ingest_seconds':ingest_seconds,'index_seconds':index_seconds,'rows':counts,'binary_bytes':binary_bytes,'all_three_native_outputs_exact':True,'native_identity_not_supplied':True});write_report(output,report)
                        print(repeat,mode,round(elapsed,6),'exact rows',counts,flush=True)
        assert protected_schema_signature(c)==before and sequence_signature(c)==sequences
    report['summary']={mode:statistics.median(r['seconds'] for r in report['runs'] if r['mode']==mode) for mode in ('binary_files','typed_shared')}
    report['all_passed']=True;report['all_private_work_rolled_back']=True;write_report(output,report);print(report['summary'],flush=True)


if __name__=='__main__':main()
