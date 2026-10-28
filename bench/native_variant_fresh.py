"""Fresh paired complete-service research variants, never a synthetic score."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys
import uuid

from psycopg import sql
from bench.common import ROOT,connect,environment,write_report
from pipeline.recipes import compiler_sha256
from bench.unlogged_stage_probe import source_identity
from bench.server_copy_probe import source_identity as server_identity
from bench.fast_transport_probe import source_identity as fast_identity
from bench.refined_copy_probe import source_identity as refined_identity
from bench.scalar_copy_probe import source_identity as scalar_identity
from bench.early_copy_probe import source_identity as early_identity
from bench.projectset_probe import source_identity as projectset_identity
from bench.source_snapshot import snapshot as source_snapshot
from bench.script_transport_probe import source_identity as script_identity
from bench.scaled_int_probe import source_identity as scaled_identity
from bench.memo_axes_probe import source_identity as memo_identity
from bench.calendar_proof_probe import source_identity as calendar_identity
from bench.backend_cpu_probe import source_identity as cpu_identity

VARIANTS={
    'official':('bench.service_run',[],'baseline'),
    'guarded_copy':('bench.service_run',[],'guarded_copy'),
    'unlogged_copy':('bench.unlogged_stage_probe',['service'],'guarded_copy'),
    'server_copy':('bench.server_copy_probe',[],'guarded_copy'),
    'fast_copy':('bench.fast_transport_probe',[],'guarded_copy'),
    'refined_copy':('bench.refined_copy_probe',['service'],'guarded_copy'),
    'scalar_copy':('bench.scalar_copy_probe',['service'],'guarded_copy'),
    'early_copy':('bench.early_copy_probe',[],'guarded_copy'),
    'projectset_copy':('bench.projectset_probe',['service'],'guarded_copy'),
    'script_copy':('bench.script_transport_probe',[],'guarded_copy'),
    'scaled_copy':('bench.script_transport_probe',['scaled'],'guarded_copy'),
    'projectset_script':('bench.script_transport_probe',['projectset'],'guarded_copy'),
    'memo_copy':('bench.memo_axes_probe',['function'],'guarded_copy'),
    'calendar_copy':('bench.calendar_proof_probe',[],'guarded_copy'),
}


def identity():
    hooks=[('unlogged_hook_sha256',source_identity),('server_copy_hook_sha256',server_identity),
           ('fast_transport_hook_sha256',fast_identity),('refined_copy_hook_sha256',refined_identity),
           ('scalar_copy_hook_sha256',scalar_identity),('early_copy_hook_sha256',early_identity),
           ('projectset_hook_sha256',projectset_identity),('script_transport_hook_sha256',script_identity),
           ('scaled_int_hook_sha256',scaled_identity),('memo_hook_sha256',memo_identity),
           ('calendar_hook_sha256',calendar_identity),('backend_cpu_hook_sha256',cpu_identity)]
    result={'atomic_compiler_sha256':compiler_sha256(),**{name:function() for name,function in hooks}}
    for name,path in [('driver_sha256',Path(__file__)),('service_driver_sha256',ROOT/'bench/service_run.py'),('source_snapshot_sha256',ROOT/'bench/source_snapshot.py')]:
        result[name]=hashlib.sha256(path.read_text(encoding='utf8').encode()).hexdigest()
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--variants',nargs='+',choices=list(VARIANTS),default=['guarded_copy','unlogged_copy'])
    p.add_argument('--repeats',type=int,default=1)
    p.add_argument('--output',default='reports/native-ephemeral-stage-paired-1m.json')
    p.add_argument('--clean-verification-after-checks',action='store_true',help='Reclaim only the exact-comparison scratch schema after its validated receipt is written')
    p.add_argument('--leader-cpu',type=int,help='Separate controlled diagnostic: pin every official/candidate batch leader equally before SQL; workers unchanged')
    p.add_argument('--observe-leader-cpu',action='store_true',help='Record Windows leader CPU usage without changing SQL or affinity; setup included for every mode')
    args=p.parse_args()
    if args.repeats<1 or len(set(args.variants))!=len(args.variants):p.error('Positive repeats and distinct variants required')
    if args.observe_leader_cpu and args.leader_cpu is not None:p.error('Use separate affinity and CPU-observation cohorts')
    frozen=identity();output=Path(args.output).resolve();directory=output.with_suffix('');directory.mkdir(parents=True,exist_ok=True)
    report={'kind':'fresh_local_original_service_complete_processing','official_score':False,'timestamp_utc':datetime.now(timezone.utc).isoformat(),'template':'lab_template_1m','reference':'lab_1m','comparison_variant':args.variants[0],'implementation_identity':frozen,'runs':[],'method':'Fresh FILE_COPY per variant/run, rotating order, no affinity or destination warmup. Exact six-table comparison first run per variant; fingerprints/schema/sequence/counters/six versions every run. All preflight/proof/COPY/index/FK/commit/version work inside complete processing wall. Clone/oracle checks excluded. This compares the explicitly named variants, not an unmeasured official baseline.'}
    report['implementation_sources']=source_snapshot()
    if identity()!=frozen:raise RuntimeError('Source changed while archiving cohort implementation')
    with connect('lab_template_1m',autocommit=True) as c:report['environment']=environment(c);report['source_rows']=c.execute('SELECT count(*) FROM public.raw_data').fetchone()[0]
    env=dict(os.environ);env.pop('CHALLENGE_BACKEND_CPU_AFFINITY',None);env.pop('CHALLENGE_RECORD_LEADER_CPU',None)
    if args.observe_leader_cpu:
        env['CHALLENGE_RECORD_LEADER_CPU']='1'
        report['recorded_leader_cpu']=True
        report['method']+=' Windows leader CPU observation is identical for every mode, changes no SQL or affinity, excludes parallel-worker CPU, and includes observer setup inside complete processing wall.'
    if args.leader_cpu is not None:
        env['CHALLENGE_BACKEND_CPU_AFFINITY']=str(args.leader_cpu)
        report['diagnostic_leader_cpu']=args.leader_cpu
        report['method']=report['method'].replace('no affinity or destination warmup','identical explicit leader CPU affinity and no destination warmup')+' Leader affinity is a separately labeled local diagnostic, not an unmodified environment or submission setting. Setup remains inside complete processing wall.'
    checked=set()
    for repeat in range(args.repeats):
        shift=repeat%len(args.variants)
        for variant in args.variants[shift:]+args.variants[:shift]:
            if identity()!=frozen:raise RuntimeError('Research source changed during cohort')
            if shutil.disk_usage(ROOT).free<5*1024**3:raise RuntimeError('Retain outputs; insufficient free space for another fresh clone/oracle')
            db='lab_native_variant_'+uuid.uuid4().hex[:10]
            with connect('postgres',autocommit=True) as c:c.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {} STRATEGY FILE_COPY').format(sql.Identifier(db),sql.Identifier(report['template'])))
            module,extra,mode=VARIANTS[variant];path=directory/f'{repeat+1}-{variant}.json'
            verification='fingerprints' if variant in checked else 'exact'
            print('Fresh paired variant',repeat+1,variant,db,'starting',flush=True)
            subprocess.run([sys.executable,'-m',module,*extra,'--database',db,'--mode',mode,'--reference','lab_1m','--verification',verification,'--output',str(path)],check=True,cwd=ROOT,env=env)
            if identity()!=frozen:raise RuntimeError('Source changed; timing is not accepted')
            value=json.loads(path.read_text(encoding='utf-8'))
            assert all(value[k] for k in ['six_table_fingerprints_equivalent','counter_reconciliation','protected_schema_equivalent','sequence_options_and_ownership_equivalent','workspace_namespace_restored']) and len(value['versions'])==6
            if args.leader_cpu is not None:
                diagnostic=value['local_backend_diagnostics']
                affinity=diagnostic.get('leader_affinity_diagnostic',diagnostic)
                assert affinity['requested_cpu']==args.leader_cpu
                assert affinity['measured_affinity']==[args.leader_cpu] and affinity['state_before_sql']=='idle'
            if args.observe_leader_cpu:
                diagnostic=value['local_backend_diagnostics']
                observed=diagnostic.get('backend_cpu_diagnostics',diagnostic)
                assert observed['backend_cpu_observer'] is True and observed['state_before_sql']=='idle'
                assert observed['leader_total_cpu_seconds']>=0 and observed['batch_transport_wall_seconds']<=value['wall_seconds']+.1
            report['runs'].append({'repeat':repeat,'mode':variant,'variant':variant,'database':db,'wall_seconds':value['wall_seconds'],'native_measured_seconds':sum(i['seconds'] for i in value['timing_report']['items'] if i.get('include_in_total',True)),'exact_six_tables':value['exact_six_tables'],'six_table_fingerprints_equivalent':True,'counter_reconciliation':True,'versions':6,'verification_schema':value['verification_schema'],'report':str(path),'execution_architecture':value.get('execution_architecture')})
            checked.add(variant);write_report(output,report)
            if args.clean_verification_after_checks and value['exact_six_tables']:
                subprocess.run([sys.executable,'-m','bench.clean_verification','--reports',str(path.relative_to(ROOT)),'--apply'],check=True,cwd=ROOT,env=env)
    control=statistics.median(r['wall_seconds'] for r in report['runs'] if r['variant']==args.variants[0])
    report['summary']={variant:{'individual_wall_seconds':[r['wall_seconds'] for r in report['runs'] if r['variant']==variant],'median_wall_seconds':statistics.median(r['wall_seconds'] for r in report['runs'] if r['variant']==variant),'time_reduction_percent_vs_comparison_variant':100*(1-statistics.median(r['wall_seconds'] for r in report['runs'] if r['variant']==variant)/control)} for variant in args.variants}
    write_report(output,report);print(json.dumps(report['summary'],indent=2),flush=True)


if __name__=='__main__':main()
