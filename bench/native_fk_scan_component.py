"""Read-only shared parent set validation versus six ordinary FK validations.

Everything including DDL and deliberate orphan probes rolls back. No catalog
validation flag is set by the prototype; this is not a pipeline score.
"""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import time

import psycopg
from psycopg import sql
from bench import native_c_lab as lab
from bench.common import ROOT,require_lab,write_report
from tests.equivalence.oracle import protected_schema_signature,sequence_signature

TABLES=('raw_data','table1','table3','table4','table2','table5','table6')
FKS=(('table1','id_column_3'),('table1','id_column_4'),('table2','id_column_2'),
     ('table5','id_column_2'),('table6','id_column_2'),('table6','id_column_3'))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',required=True);p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',required=True);p.add_argument('--build-report',default='reports/native-fk-scan-build.json');p.add_argument('--complete',action='store_true')
    args=p.parse_args();output=(ROOT/args.output).resolve()
    assert output.is_relative_to(ROOT/'reports') and not output.exists()
    build=json.loads((ROOT/args.build_report).read_text());assert build['returncode']==0
    source=(ROOT/build['source']).resolve();library=(ROOT/build['library']).resolve()
    assert source.is_relative_to(ROOT/'native') and library.is_relative_to(ROOT/'.runtime/native_build')
    assert hashlib.sha256(source.read_bytes()).hexdigest()==build['source_sha256']
    assert hashlib.sha256(library.read_bytes()).hexdigest()==build['library_sha256']
    report={'kind':'shared_keys_fk_validation_component' if args.complete else 'read_only_shared_keys_fk_component','scope':'Component only; all data and DDL rolled back. Native constraint creation excluded from both scan timers. Catalog validation flags complete only after all six actual relationship scans.' if args.complete else 'Component only; all data and DDL rolled back. Prototype never writes catalog validation flags. Native constraint creation excluded from both scan timers.',
            'database':args.database,'build':build,'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'runs':[],'negative_cases':[]}
    with lab.connect(args.database,autocommit=True) as c:
        require_lab(c);before=protected_schema_signature(c);sequences=sequence_signature(c)
        original=[]
        for table,column in FKS:
            rows=c.execute("SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=%s::regclass AND contype='f' AND conkey=ARRAY[(SELECT attnum FROM pg_attribute WHERE attrelid=%s::regclass AND attname=%s)]::smallint[]",('public.'+table,'public.'+table,column)).fetchall()
            assert len(rows)==1;original.append((table,column,*rows[0]))
        expected=[c.execute(sql.SQL('SELECT count({}) FROM {}').format(sql.Identifier(column),sql.Identifier('public',table))).fetchone()[0] for table,column in FKS]
        with c.transaction(force_rollback=True):
            c.execute('CREATE SCHEMA native_fk_lab')
            c.execute(sql.SQL("CREATE FUNCTION native_fk_lab.scan(regclass,regclass,regclass,regclass,regclass,regclass,regclass) RETURNS bigint[] AS {},{} LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT SECURITY INVOKER").format(sql.Literal(library.as_posix()),sql.Literal('challenge_fk_validate' if args.complete else 'challenge_fk_scan')))
            call='SELECT native_fk_lab.scan('+','.join("'public."+t+"'::regclass" for t in TABLES)+')'
            c.execute("SET LOCAL work_mem='128MB'; SET LOCAL maintenance_work_mem='128MB'; SET LOCAL statement_timeout='180s'")
            assert c.execute(call).fetchone()[0]==expected
            for repeat in range(args.repeats):
                for mode in (('native','shared_read_only') if repeat%2==0 else ('shared_read_only','native')):
                    with c.transaction(force_rollback=True):
                        for table,column,name,definition in original:
                            c.execute(sql.SQL('ALTER TABLE {} DROP CONSTRAINT {}').format(sql.Identifier('public',table),sql.Identifier(name)))
                            c.execute(sql.SQL('ALTER TABLE {} ADD CONSTRAINT {} '+definition+' NOT VALID').format(sql.Identifier('public',table),sql.Identifier(name)))
                        start=time.perf_counter()
                        if mode=='native':
                            for table,column,name,definition in original:
                                c.execute(sql.SQL('ALTER TABLE {} VALIDATE CONSTRAINT {}').format(sql.Identifier('public',table),sql.Identifier(name)))
                        else: assert c.execute(call).fetchone()[0]==expected
                        elapsed=time.perf_counter()-start
                        validated=c.execute("SELECT count(*) FROM pg_constraint WHERE connamespace='public'::regnamespace AND contype='f' AND convalidated").fetchone()[0]
                        assert validated==(6 if mode=='native' or args.complete else 0)
                        report['runs'].append({'repeat':repeat,'mode':mode,'seconds':elapsed,'actual_checked_keys':expected,'validated_catalog_rows_after':validated})
                        write_report(output,report);print(repeat,mode,elapsed,flush=True)
            for selected,(table,column,name,definition) in enumerate(original):
                diagnostics={}
                for mode in ('native','shared_read_only'):
                    try:
                        with c.transaction(force_rollback=True):
                            c.execute(sql.SQL('ALTER TABLE {} DROP CONSTRAINT {}').format(sql.Identifier('public',table),sql.Identifier(name)))
                            # Trigger is absent only inside this rollback-only test fixture.
                            c.execute(sql.SQL('UPDATE {} SET {}=-1 WHERE ctid=(SELECT ctid FROM {} LIMIT 1)').format(sql.Identifier('public',table),sql.Identifier(column),sql.Identifier('public',table)))
                            c.execute(sql.SQL('ALTER TABLE {} ADD CONSTRAINT {} '+definition+' NOT VALID').format(sql.Identifier('public',table),sql.Identifier(name)))
                            if mode=='native':c.execute(sql.SQL('ALTER TABLE {} VALIDATE CONSTRAINT {}').format(sql.Identifier('public',table),sql.Identifier(name)))
                            else:c.execute(call)
                            raise AssertionError('Orphan was accepted')
                    except psycopg.Error as error:
                        assert error.sqlstate=='23503'
                        diagnostics[mode]={'sqlstate':error.sqlstate,'primary':error.diag.message_primary,'detail':error.diag.message_detail,'constraint':error.diag.constraint_name,'table':error.diag.table_name,'schema':error.diag.schema_name}
                assert diagnostics['native']==diagnostics['shared_read_only'],diagnostics
                report['negative_cases'].append({'table':table,'column':column,'diagnostics':diagnostics,'exact':True});write_report(output,report)
            with c.transaction(force_rollback=True):
                c.execute("SET LOCAL work_mem='64kB'")
                assert c.execute(call).fetchone()[0] is None
            report['bounded_memory_fallback_passed']=True
        assert protected_schema_signature(c)==before and sequence_signature(c)==sequences
    report['summary']={mode:statistics.median(r['seconds'] for r in report['runs'] if r['mode']==mode) for mode in ('native','shared_read_only')}
    report['all_passed']=True;report['all_private_work_rolled_back']=True;write_report(output,report);print(report['summary'],flush=True)


if __name__=='__main__':main()
