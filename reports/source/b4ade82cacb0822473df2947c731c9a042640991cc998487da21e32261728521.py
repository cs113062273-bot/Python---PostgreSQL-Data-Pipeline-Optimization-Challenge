"""Start a separate loopback PG17 cluster for unqualified native C research."""
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import psycopg

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from bench.common import write_report


def main():
    data=(ROOT/'.runtime/native_pgdata').resolve();log=(ROOT/'.runtime/native-postgres.log').resolve()
    assert data.is_relative_to(ROOT/'.runtime') and log.is_relative_to(ROOT/'.runtime')
    binary=ROOT/'.runtime/postgresql/pgsql/bin';port=55433
    creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0)
    if not (data/'PG_VERSION').exists():
        if data.exists():raise RuntimeError('Retain incomplete isolated data directory for inspection')
        with psycopg.connect(host='127.0.0.1',port=55432,user='challenge',dbname='postgres',connect_timeout=5) as original:
            collate,ctype=original.execute("SELECT datcollate,datctype FROM pg_database WHERE datname=current_database()").fetchone()
        subprocess.run([str(binary/'initdb.exe'),'-D',str(data),'-U','native_probe','-A','trust','-E','UTF8','--lc-collate='+collate,'--lc-ctype='+ctype,'--no-instructions'],check=True,creationflags=creationflags)
    with socket.socket() as probe:
        available=probe.connect_ex(('127.0.0.1',port))!=0
    if available:
        subprocess.run([str(binary/'pg_ctl.exe'),'start','-D',str(data),'-l',str(log),'-o','-h 127.0.0.1 -p '+str(port),'-w','-t','30'],check=True,creationflags=creationflags)
    with psycopg.connect(host='127.0.0.1',port=port,user='native_probe',dbname='postgres',connect_timeout=5) as c:
        assert Path(c.execute('SHOW data_directory').fetchone()[0]).resolve()==data
        settings=dict(c.execute("SELECT name,setting FROM pg_settings WHERE name=ANY(%s)",(['fsync','full_page_writes','synchronous_commit','port','listen_addresses','server_version'],)).fetchall())
        assert all(settings[key]=='on' for key in ('fsync','full_page_writes','synchronous_commit'))
        assert settings['port']==str(port) and settings['listen_addresses']=='127.0.0.1'
        report={'kind':'separate_native_c_research_cluster','source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'data_directory':str(data),'settings':settings,'role':'native_probe','main_cluster_port_55432_unmodified':True,'application_integration':False}
        write_report(ROOT/'reports/native-c-isolated-cluster.json',report)
    print('Isolated native research cluster ready on loopback',port,flush=True)


if __name__=='__main__':main()
