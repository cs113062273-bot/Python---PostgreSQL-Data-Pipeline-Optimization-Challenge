"""Fresh original-service cohorts, including preflight/counters/versions.

Uses actual local psql, never SSH/GUI parity. Each child verifies the six exact
outputs after its complete-operation timer and retains full native reports.
"""
import argparse
from datetime import datetime,timezone
import hashlib
import json
import statistics
import subprocess
import sys
import uuid
import os
from pathlib import Path
from psycopg import sql
from bench.common import connect,ROOT,write_report,environment
from pipeline.recipes import compiler_sha256


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--template',default='lab_template_1m')
    p.add_argument('--reference',default='lab_1m')
    p.add_argument('--modes',nargs='+',choices=['baseline','guarded_prefix','guarded_parallel','guarded_ctas','guarded_copy'],default=['baseline','guarded_ctas','guarded_copy'])
    p.add_argument('--repeats',type=int,default=1)
    p.add_argument('--output',default='reports/native-fresh-1m.json')
    p.add_argument('--cpu-affinity',type=int,help='Optional identical local leader CPU pin; actual PID/affinity captured before SQL')
    args=p.parse_args()
    if args.repeats<1:p.error('repeats must be positive')
    if not args.template.startswith('lab_') or not args.reference.startswith('lab_'):p.error('Use disposable lab sources/references')
    def identity():
        return {'compiler_sha256':compiler_sha256(),'service_driver_sha256':hashlib.sha256((ROOT/'bench/service_run.py').read_bytes()).hexdigest()}
    implementation=identity()
    output=Path(args.output).resolve()
    directory=output.with_suffix('')
    directory.mkdir(parents=True,exist_ok=True)
    report={'kind':'fresh_local_original_service_complete_processing','official_score':False,'timestamp_utc':datetime.now(timezone.utc).isoformat(),'template':args.template,'reference':args.reference,'implementation_identity':implementation,'measured_backend_cpu_affinity':args.cpu_affinity,'runs':[],
            'method':'Fresh FILE_COPY clone per run; unchanged original preflight, actual counters and all six versions; no destination warmup; first run per mode uses exact six-table comparison, every run typed fingerprints/schema/counters/versions; Latin-square order; optional identical leader affinity is recorded explicitly',
            'metrics':'complete service wall includes preparation/preflight/transform/proof/staging/COPY I/O/rebuilding/validation/commit/control versions; native report totals also retained; clone/oracle excluded; Linux/SSH/GUI not claimed'}
    with connect(args.template,autocommit=True) as c:
        report['environment']=environment(c)
        report['source_rows']=c.execute('SELECT count(*) FROM public.raw_data').fetchone()[0]
    checked=set()
    child_environment=dict(os.environ)
    if args.cpu_affinity is not None:child_environment['CHALLENGE_BACKEND_CPU_AFFINITY']=str(args.cpu_affinity)
    else:child_environment.pop('CHALLENGE_BACKEND_CPU_AFFINITY',None)
    for repeat in range(args.repeats):
        shift=repeat%len(args.modes)
        for mode in args.modes[shift:]+args.modes[:shift]:
            if identity()!=implementation:raise RuntimeError('Native implementation changed during cohort')
            db='lab_native_fresh_'+uuid.uuid4().hex[:10]
            with connect('postgres',autocommit=True) as admin:
                admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {} STRATEGY FILE_COPY').format(sql.Identifier(db),sql.Identifier(args.template)))
            path=directory/f'{repeat+1}-{mode}.json'
            print(f'Native fresh {repeat+1}: {mode}: starting {db}',flush=True)
            verification='fingerprints' if mode in checked else 'exact'
            subprocess.run([sys.executable,'-m','bench.service_run','--database',db,'--mode',mode,'--reference',args.reference,'--verification',verification,'--output',str(path)],check=True,cwd=ROOT,env=child_environment)
            if identity()!=implementation:raise RuntimeError('Native source changed during execution; discard cohort')
            result=json.loads(path.read_text())
            row={'repeat':repeat,'mode':mode,'database':db,'wall_seconds':result['wall_seconds'],'native_measured_seconds':sum(i['seconds'] for i in result['timing_report']['items'] if i.get('include_in_total',True)),
                 'exact_six_tables':result['exact_six_tables'],'six_table_fingerprints_equivalent':result['six_table_fingerprints_equivalent'],'counter_reconciliation':result['counter_reconciliation'],'versions':len(result['versions']),'report':str(path),'verification_schema':result['verification_schema'],'execution_architecture':result.get('execution_architecture'),'local_backend_diagnostics':result.get('local_backend_diagnostics')}
            assert row['six_table_fingerprints_equivalent'] and row['counter_reconciliation'] and row['versions']==6
            if args.cpu_affinity is not None:assert row['local_backend_diagnostics']['measured_affinity']==[args.cpu_affinity]
            if verification=='exact':checked.add(mode)
            report['runs'].append(row)
            write_report(output,report)
    baseline=statistics.median(r['wall_seconds'] for r in report['runs'] if r['mode']=='baseline')
    report['summary']={}
    for mode in args.modes:
        values=[r['wall_seconds'] for r in report['runs'] if r['mode']==mode]
        median=statistics.median(values)
        report['summary'][mode]={'individual_wall_seconds':values,'median_wall_seconds':median,'reduction_percent':100*(1-median/baseline)}
    write_report(output,report)
    print(json.dumps(report['summary'],indent=2),flush=True)


if __name__=='__main__':main()
