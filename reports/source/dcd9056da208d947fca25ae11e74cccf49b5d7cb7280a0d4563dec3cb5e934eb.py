"""Supplementary native-service resource probe, separate from ranked timing.

Samples the real client process tree, this database's backends/workers and the
private COPY directory. Working-set sums double-count shared pages. Sampling
can miss short peaks; PostgreSQL CPU is a sampled lower bound. No OS/global
database configuration is changed.
"""
import argparse
from collections import defaultdict
import json
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

import psutil
from psycopg import sql

from bench.common import ROOT,connect,environment,require_lab,write_report
from pipeline.recipes import ORDER,compiler_sha256


def database_stats(c):
    row=c.execute("SELECT pg_database_size(current_database()),temp_files,temp_bytes,pg_current_wal_insert_lsn()::text FROM pg_stat_database WHERE datname=current_database()").fetchone()
    return dict(zip(('database_bytes','temp_files','temp_bytes','cluster_wal_lsn'),row))


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--template',default='lab_template_1m')
    p.add_argument('--reference',default='lab_1m')
    p.add_argument('--mode',choices=['baseline','guarded_ctas','guarded_copy'],default='guarded_copy')
    p.add_argument('--interval',type=float,default=.2)
    p.add_argument('--output',default='reports/native-copy-resources.json')
    args=p.parse_args()
    if not args.template.startswith('lab_') or not args.reference.startswith('lab_'):p.error('Use disposable lab sources/references')
    if args.interval<.1:p.error('Sampling more frequently than 100ms is unnecessarily intrusive')
    with connect(args.template,autocommit=True) as c:require_lab(c)
    db='lab_native_resource_'+uuid.uuid4().hex[:10]
    with connect('postgres',autocommit=True) as c:
        c.execute(sql.SQL('CREATE DATABASE {} TEMPLATE {} STRATEGY FILE_COPY').format(sql.Identifier(db),sql.Identifier(args.template)))
    output=Path(args.output).resolve()
    child_report=output.with_name(output.stem+'-service.json')
    source=compiler_sha256()
    complete=threading.Event()
    log=[]
    direct_stage=[]
    peaks=defaultdict(int)
    cpu={}
    sample_errors=defaultdict(int)
    def sample_process(pid,kind):
        try:
            proc=psutil.Process(pid)
            memory=proc.memory_info().rss
            t=proc.cpu_times()
            key=(pid,proc.create_time(),kind)
            cpu[key]=max(cpu.get(key,0),t.user+t.system)
            return memory
        except (psutil.NoSuchProcess,psutil.AccessDenied):
            sample_errors[kind+'_process_unavailable']+=1
            return 0
    with connect(db,autocommit=True) as observer:
        require_lab(observer)
        env=environment(observer)
        before=database_stats(observer)
        command=[sys.executable,'-m','bench.native_resource_worker','--database',db,'--mode',args.mode,'--reference',args.reference,'--verification','fingerprints','--output',str(child_report)]
        process=subprocess.Popen(command,cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,encoding='utf-8',errors='replace',bufsize=1)
        def read_output():
            for line in process.stdout:
                log.append(line)
                print(line.rstrip(),flush=True)
                if line.startswith('Native resource stage bytes: '):direct_stage.append(int(line.split(': ',1)[1]))
                if line.startswith('Native '+args.mode+':') and 'complete processing wall;' in line:complete.set()
        reader=threading.Thread(target=read_output,daemon=True)
        reader.start()
        started=time.monotonic();samples=0
        while process.poll() is None and not complete.is_set():
            pids=observer.execute("SELECT pid,leader_pid FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()").fetchall()
            peaks['database_backend_sum_rss_bytes']=max(peaks['database_backend_sum_rss_bytes'],sum(sample_process(pid,'postgres') for pid,_ in pids))
            peaks['parallel_workers_observed']=max(peaks['parallel_workers_observed'],sum(leader is not None for _,leader in pids))
            try:
                clients=[psutil.Process(process.pid)]+psutil.Process(process.pid).children(recursive=True)
                peaks['client_tree_sum_rss_bytes']=max(peaks['client_tree_sum_rss_bytes'],sum(sample_process(p.pid,'client') for p in clients))
            except psutil.NoSuchProcess:sample_errors['client_tree_unavailable']+=1
            # Only our fixed COPY basenames under the private diagnostic root.
            size=0
            for directory in (ROOT/'.runtime/native_copy').glob('copy_batch_*'):
                for table in ORDER:
                    try:size+=(directory/(table+'.bin')).stat().st_size
                    except FileNotFoundError:pass
                    except OSError:sample_errors['copy_file_stat_unavailable']+=1
            peaks['private_copy_files_bytes']=max(peaks['private_copy_files_bytes'],size)
            if samples%5==0:
                size=observer.execute('SELECT pg_database_size(current_database())').fetchone()[0]
                peaks['data_database_bytes']=max(peaks['data_database_bytes'],size)
            samples+=1
            complete.wait(args.interval)
        sampling_seconds=time.monotonic()-started
        processing_end=database_stats(observer)
        result_code=process.wait()
        reader.join()
        observer.execute('SELECT pg_stat_clear_snapshot()')
        after=database_stats(observer)
        wal=observer.execute('SELECT pg_wal_lsn_diff(%s::pg_lsn,%s::pg_lsn)',(processing_end['cluster_wal_lsn'],before['cluster_wal_lsn'])).fetchone()[0]
    child=json.loads(child_report.read_text()) if child_report.exists() else None
    report={'kind':'sampled_native_service_resources_unranked','official_score':False,'database':db,'mode':args.mode,'environment':env,'compiler_sha256':source,'source_unchanged':compiler_sha256()==source,'service_report':str(child_report),'service_exit_code':result_code,'processing_completion_observed':complete.is_set(),'processing_wall_seconds':child['wall_seconds'] if child else None,'sampling_interval_seconds':args.interval,'sampling_seconds':sampling_seconds,'samples':samples,'peaks':dict(peaks),'sample_errors':dict(sample_errors),'sampled_cpu_seconds_lower_bound':{kind:sum(value for (*_,k),value in cpu.items() if k==kind) for kind in ('postgres','client')},'before':before,'processing_end':processing_end,'after_verification':after,'cluster_wal_delta_bytes_until_processing_end':int(wal),'database_temp_files_delta':after['temp_files']-before['temp_files'],'database_temp_bytes_delta':after['temp_bytes']-before['temp_bytes'],'scope':'Supplementary sampled diagnostics, not a ranked time. Resource window begins at child launch and ends at its processing-complete line, including import/seed setup outside the service timer. Output verification follows outside that window. RSS sums double-count shared pages and are not physical RAM; peaks may be missed; CPU is sampled. WAL is cluster-wide and can include control/checkpoints/other background work. Temp counters read after verification can also include verification work.'}
    report['processing_database_temp_files_delta']=processing_end['temp_files']-before['temp_files']
    report['processing_database_temp_bytes_delta']=processing_end['temp_bytes']-before['temp_bytes']
    report['peaks']['logged_normalized_stage_bytes']=direct_stage[0] if len(direct_stage)==1 else None
    report['stage_measurement']='Exact pg_total_relation_size inside the owning transaction after projection, reported by a benchmark-only worker hook. Other sessions cannot see the uncommitted stage catalog. Baseline has no normalized stage.'
    write_report(output,report)
    if result_code or not report['source_unchanged'] or not complete.is_set():raise RuntimeError('Native resource run failed: '+''.join(log)[-2000:])
    assert child['six_table_fingerprints_equivalent'] and child['counter_reconciliation'] and len(child['versions'])==6
    if args.mode!='baseline' and child['execution_architecture']['safe_domain']:
        assert len(direct_stage)==1 and direct_stage[0]>0,'Expected nonempty private normalized stage'
    print('Supplementary native resource probe passed; see',output,flush=True)


if __name__=='__main__':main()
