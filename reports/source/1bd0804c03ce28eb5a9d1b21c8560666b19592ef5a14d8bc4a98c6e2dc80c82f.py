"""Restart only the isolated 55433 research cluster with explicit WAL settings.

No config file, other cluster, durability switch, backup or replication state
is changed. Caller must restore replica mode after the bounded experiment.
"""
import argparse
from datetime import datetime,timezone
from pathlib import Path
import subprocess
import time
import psycopg
from bench import native_c_lab as lab
from bench.common import ROOT,write_report


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--mode',choices=['minimal','replica'],required=True);p.add_argument('--output',required=True);args=p.parse_args()
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    directory=(ROOT/'.runtime/native_pgdata').resolve();executable=ROOT/'.runtime/postgresql/pgsql/bin/pg_ctl.exe'
    with lab.connect('postgres',autocommit=True) as c:
        assert c.info.port==55433 and c.execute('SELECT pg_is_in_recovery()').fetchone()==(False,)
        assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid()").fetchall(),'Close all native clients before restart'
        assert c.execute('SHOW archive_mode').fetchone()==('off',)
        assert not c.execute('SELECT slot_name FROM pg_replication_slots').fetchall()
        assert not c.execute('SELECT pid FROM pg_stat_replication').fetchall()
        settings=dict(c.execute("SELECT name,setting FROM pg_settings WHERE name=ANY(%s)",(['wal_level','max_wal_senders','fsync','full_page_writes','synchronous_commit'],)).fetchall())
        assert all(settings[name]=='on' for name in ('fsync','full_page_writes','synchronous_commit'))
    record={'kind':'isolated_research_wal_mode_restart','before':settings,'requested_mode':args.mode,'data_directory':str(directory.relative_to(ROOT)),
            'timestamp_utc':datetime.now(timezone.utc).isoformat(),'passed':False,'scope':'Isolated lab cluster only, no active clients/archive/replication slots. Explicit startup settings preserve fsync/full_page_writes/synchronous_commit. This is not a performance/recovery score.'}
    write_report(output,record)
    log=output.with_suffix('.pgctl.txt');server_log=ROOT/'.runtime'/('native-wal-'+args.mode+'-'+str(time.time_ns())+'.log')
    with log.open('x',encoding='utf8') as writer:
        subprocess.run([str(executable),'-D',str(directory),'-m','fast','-t','60','-w','stop'],stdout=writer,stderr=subprocess.STDOUT,check=True)
        options=f'-h 127.0.0.1 -p 55433 -c wal_level={args.mode} -c max_wal_senders={0 if args.mode=="minimal" else 10} -c fsync=on -c full_page_writes=on -c synchronous_commit=on'
        subprocess.run([str(executable),'-D',str(directory),'-l',str(server_log),'-o',options,'-W','start'],stdout=writer,stderr=subprocess.STDOUT,check=True)
    start=time.monotonic()
    while True:
        try:
            with lab.connect('postgres',autocommit=True) as c:
                observed=dict(c.execute("SELECT name,setting FROM pg_settings WHERE name=ANY(%s)",(['wal_level','max_wal_senders','fsync','full_page_writes','synchronous_commit'],)).fetchall())
            break
        except psycopg.OperationalError:
            if time.monotonic()-start>120:raise
            time.sleep(1)
    assert observed['wal_level']==args.mode and all(observed[name]=='on' for name in ('fsync','full_page_writes','synchronous_commit'))
    record.update({'after':observed,'passed':True,'server_log':str(server_log.relative_to(ROOT))});write_report(output,record)
    print('Isolated native cluster',args.mode,'ready; all durability switches on',flush=True)


if __name__=='__main__':main()
