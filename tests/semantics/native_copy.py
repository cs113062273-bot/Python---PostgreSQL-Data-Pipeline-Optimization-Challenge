"""Run actual captured native psql batches against mutated official fixtures.

This includes COPY command receipts and managed counters, unlike the SQL-only
fuzzer. Failures must leave the complete graph/counters/schema rolled back.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import uuid
from psycopg.types.json import Jsonb
from bench.common import connect,ROOT,write_report,environment
from bench.fixture import create_fixture
from bench.control import seed_initial_versions
from pipeline.local_service import LocalService
from pipeline.bulk_fks import BulkFKMixin
from pipeline.bulk_indexes import BulkIndexMixin
from pipeline.guarded_normalization import GuardedNormalizationMixin
from pipeline.recipes import destinations,bulk_unique_tables,ORDER,compiler_sha256
from pipeline.upstream import PostgresAdminService
from tests.semantics.fuzz import cases,SETTINGS
from tests.equivalence.oracle import canonical,protected_schema_signature,sequence_signature


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--count',type=int,default=150)
    p.add_argument('--seed',type=int,default=718)
    p.add_argument('--output',default='reports/semantics-native-copy.json')
    args=p.parse_args()
    identity=compiler_sha256()
    db='lab_native_copy_fuzz_'+uuid.uuid4().hex[:8]
    create_fixture(db)
    seed_initial_versions(db)
    scripts={}
    class CaptureLocal(LocalService):
        def run_remote_command(self,command,stdin_text=None,**kwargs):
            if isinstance(stdin_text,str) and 'BEGIN ISOLATION LEVEL REPEATABLE READ;' in stdin_text and 'pgdm_cross_table_hash_manifest' in stdin_text:
                scripts[self.bulk_recipe_mode]=stdin_text
            return super().run_remote_command(command,stdin_text=stdin_text,**kwargs)
    class Service(BulkFKMixin,BulkIndexMixin,GuardedNormalizationMixin,CaptureLocal):pass
    def reset(c):
        c.execute('TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY')
        c.execute("UPDATE public.pgdm_table_row_counts SET row_count=0 WHERE schema_name='public' AND table_name=ANY(%s)",(list(ORDER),))
    report={'kind':'actual_psql_native_copy_differential','database':db,'compiler_sha256':identity,'seed':args.seed,'cases':[],'mismatches':[],
            'policy':'Same acceptance, all six canonical outputs and native counters; same first SQLSTATE/primary message; exact protected schema/sequence options and workspace restoration after every success/error. Deliberately mutated fixtures, never ranking data.'}
    with connect(db,autocommit=True) as c:
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data')]
        report['environment']=environment(c)
        for mode in ['baseline','guarded_copy']:
            service=Service()
            service.bulk_recipe_mode=mode
            service.bulk_foreign_keys=mode!='baseline'
            service.bulk_unique_tables=bulk_unique_tables(mode)
            service.guarded_normalization=mode!='baseline'
            timing=PostgresAdminService.create_cross_table_expansion_timing_report(db,'public.raw_data',scopes,[d['table_name'] for d in destinations()])
            service.execute_cross_table_expansion(db,'public.raw_data',scopes,destinations(mode),timing_report=timing)
            reset(c)
        raw_id,original=c.execute('SELECT raw_id,raw FROM public.raw_data ORDER BY raw_id LIMIT 1').fetchone()
        schema=protected_schema_signature(c);sequences=sequence_signature(c)
        base=ROOT/'.runtime'/'native_copy_fuzz';base.mkdir(parents=True,exist_ok=True)
        for index,case in enumerate(cases(args.seed,args.count)):
            if compiler_sha256()!=identity:raise RuntimeError('Native compiler changed during differential cohort')
            raw=copy.deepcopy(original)
            if 'shape' in case:raw['values']=case['shape']
            elif case.get('missing_values'):raw.pop('values',None)
            else:
                for key,value in case.get('edits',[]):raw['values'][key]=value
                raw['values'].pop(case.get('remove'),None)
            c.execute('UPDATE public.raw_data SET raw=%s WHERE raw_id=%s',(Jsonb(raw),raw_id))
            outcomes={}
            for mode in ['baseline','guarded_copy']:
                reset(c)
                # Trusted native settings, same in both executions. No data
                # literals are reformatted into a psql meta-command.
                from psycopg import sql
                prefix='\n'.join(sql.SQL('SELECT pg_catalog.set_config({}, {}, false);').format(sql.Literal(k),sql.Literal(v)).as_string(c) for k,v in SETTINGS[case.get('settings',0)].items())
                directory=Path(tempfile.mkdtemp(prefix='trial_',dir=base)).resolve()
                assert directory.parent==base.resolve()
                try:
                    result=subprocess.run([os.environ.get('CHALLENGE_PSQL',str(ROOT/'.runtime/postgresql/pgsql/bin/psql.exe')),'-X','-q','-A','-t','-w','-v','ON_ERROR_STOP=1','-v','VERBOSITY=verbose','-d',db],input=prefix+'\n'+scripts[mode],text=True,encoding='utf-8',errors='replace',capture_output=True,cwd=directory)
                finally:
                    for table in ORDER:(directory/(table+'.bin')).unlink(missing_ok=True)
                    directory.rmdir()
                assert protected_schema_signature(c)==schema,(index,mode,'schema')
                assert sequence_signature(c)==sequences,(index,mode,'sequence options')
                assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()[0] is None
                actual={table:c.execute(f'SELECT count(*) FROM public.{table}').fetchone()[0] for table in ORDER}
                counters=dict(c.execute("SELECT table_name,row_count FROM public.pgdm_table_row_counts WHERE schema_name='public'").fetchall())
                assert actual==counters,(index,mode,'counter receipt',actual,counters)
                if result.returncode:
                    match=re.search(r'ERROR:\s+([0-9A-Z]{5}):\s*(.*?)(?=\n(?:DETAIL:|HINT:|CONTEXT:|LOCATION:)|\Z)',result.stderr,re.S)
                    if not match:raise AssertionError(result.stderr)
                    assert all(n==0 for n in actual.values()),'Failed native batch committed a prefix'
                    outcomes[mode]={'sqlstate':match[1],'primary':match[2].strip(),'rolled_back':True}
                else:
                    outputs={table:[r[0] for r in c.execute('SELECT value::text FROM ('+canonical(table)+') t ORDER BY value::text')] for table in ORDER}
                    outcomes[mode]={'sqlstate':'00000','outputs':outputs,'counter_match':True}
            mismatch=outcomes['baseline']!=outcomes['guarded_copy']
            row={'index':index,'case':case,'sqlstates':{m:o['sqlstate'] for m,o in outcomes.items()},'equivalent':not mismatch}
            report['cases'].append(row)
            if mismatch:report['mismatches'].append({'index':index,'case':case,'outcomes':outcomes})
            if index%25==0:print('Native COPY case',index,'differences',len(report['mismatches']),flush=True)
            write_report(args.output,report)
        reset(c)
        c.execute('UPDATE public.raw_data SET raw=%s WHERE raw_id=%s',(Jsonb(original),raw_id))
    report['all_passed']=not report['mismatches']
    write_report(args.output,report)
    print('Native psql COPY:',len(report['cases']),'cases;',len(report['mismatches']),'differences',flush=True)
    if report['mismatches']:raise SystemExit(1)


if __name__=='__main__':main()
