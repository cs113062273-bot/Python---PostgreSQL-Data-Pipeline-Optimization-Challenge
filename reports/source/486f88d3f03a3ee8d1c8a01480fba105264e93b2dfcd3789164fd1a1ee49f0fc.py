"""Native compact COPY-export worker choices with exact binary row replay."""
import argparse
import hashlib
from pathlib import Path
import shutil
import statistics
import tempfile
import time
from psycopg import sql
from bench import native_c_lab as lab,native_compact_sql as compact,slim_copy_probe as slim
from bench.common import ROOT,write_report,require_lab
from bench.run import prepared
from pipeline.parallel_normalization import settings_sql
from pipeline.normalization import source_sql
from tests.equivalence.oracle import protected_schema_signature,sequence_signature


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',required=True);p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--tables',nargs='+',choices=['table1','table2','table5','table6'],default=['table1','table2','table5','table6'])
    p.add_argument('--output',required=True);args=p.parse_args()
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    report={'kind':'native_compact_binary_export_component','database':args.database,'runs':[],
            'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'scope':'COPY SELECT to private binary file only, shared warmed native compact cache and already populated parent tables. Replay each file into native typed temp rows and bidirectional EXCEPT ALL outside timers. No final identity/COPY ingest/index/FK/commit/versions or full-service percentage claimed.'}
    parent=ROOT/'.runtime/native_compact_exports';parent.mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='trial_',dir=parent) as directory,lab.connect(args.database,autocommit=True) as c:
        assert shutil.disk_usage(ROOT).free>1536*1024**2,'Retain room for bounded cache/reference/replay'
        require_lab(c);before=protected_schema_signature(c);sequences=sequence_signature(c)
        with c.transaction(force_rollback=True):
            c.execute("SET LOCAL statement_timeout='180s'; SET LOCAL work_mem='256MB'")
            normalizer,axes,build=lab.install_compact(c);report['build']=build
            scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
            c.execute(settings_sql())
            relation='native_lab.native_compact_case'
            c.execute('CREATE UNLOGGED TABLE '+relation+' AS SELECT s.raw_id,n.* FROM '+source_sql(scopes)+" s CROSS JOIN LATERAL "+normalizer+"(s.raw->'values') n("+lab.compact_definition()+')')
            assert c.execute('SELECT count(*),bool_and(pipeline_domain_safe) FROM '+relation).fetchone()==(1000000,True)
            c.execute('ANALYZE '+relation)
            recipes={r['pure_table_name']:r['statements'][0] for r in prepared('baseline',scopes)}
            for table in args.tables:
                statement,_=slim.compact(compact.statement(recipes[table],scopes,axes,relation=relation),table)
                columns,expressions,distinct,suffix=slim.split_projection(statement,table)
                query='SELECT '+('DISTINCT ' if distinct else '')+','.join(expressions)+' '+suffix
                c.execute(settings_sql(0))
                c.execute('CREATE TEMP TABLE export_reference ('+','.join(columns)+') ON COMMIT DROP AS '+query)
                expected=c.execute('SELECT count(*) FROM pg_temp.export_reference').fetchone()[0]
                for repeat in range(args.repeats):
                    order=[8,0,2,4];shift=repeat%4
                    for workers in order[shift:]+order[:shift]:
                        # Abort each creation subtransaction so PostgreSQL can
                        # unlink that trial's storage now, rather than defer
                        # every dropped relfilenode until the outer rollback.
                        with c.transaction(force_rollback=True):
                            assert shutil.disk_usage(ROOT).free>512*1024**2
                            c.execute(settings_sql(workers))
                            c.execute('CREATE TEMP TABLE export_replay (LIKE pg_temp.export_reference) ON COMMIT DROP')
                            path=(Path(directory)/(table+'.bin')).resolve();assert path.parent==Path(directory).resolve() and not path.exists()
                            try:
                                start=time.perf_counter()
                                result=c.execute(sql.SQL('COPY ('+query+') TO {} (FORMAT BINARY)').format(sql.Literal(path.as_posix())))
                                seconds=time.perf_counter()-start
                                assert result.rowcount==expected,(table,workers,result.rowcount,expected)
                                size=path.stat().st_size
                                c.execute(sql.SQL('COPY pg_temp.export_replay FROM {} (FORMAT BINARY)').format(sql.Literal(path.as_posix())))
                            finally:path.unlink(missing_ok=True)
                            assert not c.execute('SELECT EXISTS((TABLE pg_temp.export_reference EXCEPT ALL TABLE pg_temp.export_replay) UNION ALL (TABLE pg_temp.export_replay EXCEPT ALL TABLE pg_temp.export_reference))').fetchone()[0]
                        row={'table':table,'repeat':repeat,'workers':workers,'export_seconds':seconds,'rows':expected,'binary_bytes':size,'exact_native_typed_rows':True}
                        report['runs'].append(row);write_report(output,report)
                        print(table,repeat,workers,round(seconds,5),'exact rows',expected,flush=True)
                c.execute('DROP TABLE pg_temp.export_reference')
        assert protected_schema_signature(c)==before and sequence_signature(c)==sequences
    report['summary']={table:{str(w):statistics.median(r['export_seconds'] for r in report['runs'] if r['table']==table and r['workers']==w) for w in (0,2,4,8)} for table in args.tables}
    report['all_passed']=True;report['all_private_work_rolled_back']=True;write_report(output,report);print(report['summary'],flush=True)


if __name__=='__main__':main()
