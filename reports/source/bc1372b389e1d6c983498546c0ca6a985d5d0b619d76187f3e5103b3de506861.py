"""Fresh original/native pair; actual baseline establishes the separate oracle.

Input generation/cloning and exact comparison occur outside processing timers.
Both measured runs include native service preflight, transformations, durable
commit, counters, constraints and six version registrations. Local evidence only.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import uuid
from psycopg import sql
from bench import native_c_lab as lab, native_compact_probe as candidate
from bench.common import ROOT, write_report
from bench.source_snapshot import snapshot
from pipeline.checkpoints import source_digest
from pipeline.recipes import compiler_sha256, ORDER


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--rows',type=int,choices=[100000,1000000],default=100000)
    p.add_argument('--output',required=True)
    p.add_argument('--candidate',choices=['compact','minimal','details','fk','fk_stream','fk_current','main','main_compact'],default='compact')
    args=p.parse_args()
    candidate_module='bench.native_compact_probe'
    if args.candidate=='minimal':
        global candidate
        from bench import native_minimal_probe as candidate
        candidate_module='bench.native_minimal_probe'
    elif args.candidate=='details':
        from bench import native_details_probe as candidate
        candidate_module='bench.native_details_probe'
    elif args.candidate=='fk':
        from bench import native_fk_probe as candidate
        candidate_module='bench.native_fk_probe'
    elif args.candidate=='fk_stream':
        from bench import native_fk_stream_probe as candidate
        candidate_module='bench.native_fk_stream_probe'
    elif args.candidate=='fk_current':
        from bench import native_fk_current_probe as candidate
        candidate_module='bench.native_fk_current_probe'
    elif args.candidate=='main':
        from bench import native_main_probe as candidate
        candidate_module='bench.native_main_probe'
    elif args.candidate=='main_compact':
        from bench import native_main_compact_probe as candidate
        candidate_module='bench.native_main_compact_probe'
    output=(ROOT/args.output).resolve()
    assert output.is_relative_to(ROOT/'reports') and not output.exists()
    directory=output.with_suffix('');directory.mkdir(exist_ok=False)
    assert shutil.disk_usage(ROOT).free > args.rows*7000, 'Retain room for input, outputs, WAL and verification'
    env={**os.environ,'PGHOST':'127.0.0.1','PGPORT':str(lab.PORT),'PGUSER':'native_probe',
         'CHALLENGE_DSN':'host=127.0.0.1 port=55433 user=native_probe',
         'CHALLENGE_PSQL':str(ROOT/'.runtime/postgresql/pgsql/bin/psql.exe')}
    for key in ('CHALLENGE_BACKEND_CPU_AFFINITY','CHALLENGE_RECORD_LEADER_CPU'):
        env.pop(key,None)
    def identity():
        return {'hook_sha256':candidate.source_identity(),'compiler_sha256':compiler_sha256(),
                'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                'service_sha256':hashlib.sha256((ROOT/'bench/service_run.py').read_bytes()).hexdigest(),
                'generator_sha256':hashlib.sha256((ROOT/'app/generate_database.py').read_bytes()).hexdigest()}
    frozen=identity()
    baseline='lab_native_official_'+uuid.uuid4().hex[:10]
    compact='lab_native_compact_'+uuid.uuid4().hex[:10]
    report={'kind':'fresh_native_compact_complete_processing_pair','official_score':False,
            'candidate_implementation':args.candidate,
            'official_eligibility_unproven':True,'timestamp_utc':datetime.now(timezone.utc).isoformat(),
            'rows':args.rows,'official_database':baseline,'candidate_database':compact,
            'disk_free_bytes_before_generation':shutil.disk_usage(ROOT).free,
            'implementation_identity':frozen,'source_snapshot':snapshot(),'runs':[],
            'method':'Fresh unchanged generator input; candidate FILE_COPY before any destination work. Actual unchanged baseline without destination warmup establishes original outputs. Candidate compared by all native typed values and multiplicities in six tables after processing timer.',
            'timing_scope':'Complete service wall including preparation/preflight/proof/transient extension/COPY/index and FK validation/commit/counters/six versions. Generation, clone, exact oracle comparison and software installation excluded.',
            'limitations':['single ordered AB pair; not a repeated robust result','Windows local psql; no Linux/SSH/GUI qualification','trusted native implementation official eligibility unproven']}
    write_report(output,report)
    print('Generating fresh official input',baseline,args.rows,flush=True)
    lab.fixture(baseline,rows=args.rows)
    with lab.connect('postgres',autocommit=True) as admin:
        admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {} STRATEGY FILE_COPY').format(sql.Identifier(compact),sql.Identifier(baseline)))
    inputs=[]
    for name in (baseline,compact):
        with lab.connect(name,autocommit=True) as c:
            assert all(c.execute('SELECT count(*) FROM public.'+table).fetchone()[0]==0 for table in ORDER)
            assert c.execute('SELECT count(*) FROM public.raw_data').fetchone()[0]==args.rows
            assert c.execute("SELECT to_regclass('public.pgdm_table_row_counts')").fetchone()[0] is None
            scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
            inputs.append(source_digest(c,scopes))
    assert inputs[0]==inputs[1]
    report['identical_input_digest']=inputs[0];write_report(output,report)
    for mode,name,module,extra in [('official',baseline,'bench.service_run',['--establish-reference']),
                                    ('native_compact',compact,candidate_module,['--reference',baseline])]:
        assert identity()==frozen
        with lab.connect('postgres',autocommit=True) as admin:
            assert not admin.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>%s",(admin.info.backend_pid,)).fetchall(), 'No overlapping native jobs during timing'
        path=directory/(mode+'.json')
        command=[sys.executable,'-u','-m',module,'--database',name,'--mode','baseline' if mode=='official' else 'guarded_copy','--output',str(path),*extra]
        disk_free_before_service=shutil.disk_usage(ROOT).free
        print('Complete native service',mode,name,flush=True)
        with path.with_suffix('.log').open('x',encoding='utf8') as log:
            result=subprocess.run(command,cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT)
        assert result.returncode==0,(mode,result.returncode,str(path.with_suffix('.log')))
        assert identity()==frozen,'Source changed during the pair'
        child=json.loads(path.read_text(encoding='utf8'))
        assert len(child['versions'])==6
        for field in ('counter_reconciliation','protected_schema_equivalent','sequence_options_and_ownership_equivalent','workspace_namespace_restored'):
            assert child[field] is True
        if mode=='official':
            assert child['unmodified_reference_established'] is True and child['exact_six_tables'] is False
        else:
            assert child['exact_six_tables'] is True and child['execution_architecture']['safe_domain'] is True
            key={'compact':'native_compact_hook_sha256','minimal':'native_minimal_hook_sha256','details':'native_details_hook_sha256','fk':'native_fk_hook_sha256','fk_stream':'native_fk_stream_hook_sha256','fk_current':'native_fk_current_hook_sha256','main':'native_main_hook_sha256','main_compact':'native_main_hook_sha256'}[args.candidate]
            assert child[key]==frozen['hook_sha256']
            if args.candidate in ('main','main_compact'):
                assert child['local_backend_diagnostics']['actual_binary_files']==2
                assert child['local_backend_diagnostics']['typed_main_affected'] not in (None,'fallback')
                assert child['local_backend_diagnostics']['shared_fk_checked'] not in (None,'fallback')
            if args.candidate in ('details','fk','fk_stream','fk_current'):
                assert child['local_backend_diagnostics']['actual_binary_files']==3,'Shared details must actually run, not silently benchmark its fallback'
            if args.candidate in ('fk','fk_stream','fk_current'):
                checked=child['local_backend_diagnostics']['shared_fk_checked']
                assert checked not in (None,'fallback') and len(checked.split(','))==6,'Full shared FK scan must actually run'
            assert len(child['exact_comparison_details'])==6
        assert child['destination_warmup_before_measured_run'] is False
        with lab.connect(name,autocommit=True) as c:
            assert source_digest(c,scopes)==inputs[0]
            assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()[0] is None
            from bench.native_compact_probe import package
            assert not c.execute('SELECT extname FROM pg_extension WHERE extname=%s',(package()['extension'],)).fetchall()
            if args.candidate in ('details','fk','fk_stream','fk_current','main','main_compact'):
                from bench.native_details_probe import package as details_package
                assert not c.execute('SELECT extname FROM pg_extension WHERE extname=%s',(details_package()['extension'],)).fetchall()
            if args.candidate in ('fk','fk_stream','fk_current'):assert not c.execute('SELECT extname FROM pg_extension WHERE extname=%s',(candidate.package()['extension'],)).fetchall()
            if args.candidate in ('main','main_compact'):
                for info in (candidate.package(),candidate.package('stamp'),candidate.base.package()):
                    assert not c.execute('SELECT extname FROM pg_extension WHERE extname=%s',(info['extension'],)).fetchall()
                if mode!='official':assert int(child['local_backend_diagnostics']['typed_main_affected'])==c.execute('SELECT count(*) FROM public.table1').fetchone()[0]
                if args.candidate=='main_compact':assert not c.execute('SELECT extname FROM pg_extension WHERE extname=%s',(candidate.package('memo')['extension'],)).fetchall()
        row={'mode':mode,'database':name,'report':str(path.relative_to(ROOT)),
             'disk_free_bytes_before_service':disk_free_before_service,
             'wall_seconds':child['wall_seconds'],
             'native_measured_seconds':sum(i['seconds'] for i in child['timing_report']['items'] if i.get('include_in_total',True)),
             'versions':len(child['versions']),'exact_six_tables':child['exact_six_tables'],
             'unmodified_reference_established':child['unmodified_reference_established'],'source_unchanged':True}
        report['runs'].append(row);write_report(output,report)
        print(mode,row['wall_seconds'],'complete wall seconds',flush=True)
    official,current=report['runs']
    report['summary']={'official_wall_seconds':official['wall_seconds'],'candidate_wall_seconds':current['wall_seconds'],
                       'reduction_percent':100*(1-current['wall_seconds']/official['wall_seconds']),
                       'threshold_90_seconds':official['wall_seconds']*.1,
                       'additional_seconds_required':max(0,current['wall_seconds']-official['wall_seconds']*.1),
                       'single_pair_only':True,'exact_six_outputs_and_checks_passed':True}
    write_report(output,report);print(json.dumps(report['summary'],indent=2),flush=True)


if __name__=='__main__':main()
