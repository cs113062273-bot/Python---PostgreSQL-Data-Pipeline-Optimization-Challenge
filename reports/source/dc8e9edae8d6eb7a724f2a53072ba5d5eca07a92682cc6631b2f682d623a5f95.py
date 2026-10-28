"""Run unchanged organizer control/version SQL via local isolated transport."""
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from bench.fixture import generator
from bench import native_c_lab as lab


def main():
    with lab.connect('postgres',autocommit=True):pass
    gen=generator()
    def local_psql(args,database,query,*unused,**kwargs):
        command=[str(ROOT/'.runtime/postgresql/pgsql/bin/psql.exe'),'-h','127.0.0.1','-p',str(lab.PORT),'-U','native_probe','-d',database,'-X','-qAt','-w','-v','ON_ERROR_STOP=1']
        result=subprocess.run(command,input=query,text=True,encoding='utf8',capture_output=True)
        if result.returncode:raise RuntimeError(result.stderr)
        return result.stdout
    # Only the connection transport changes; the pinned generator's SQL,
    # deterministic data routines and seed are not edited or substituted.
    gen._run_psql=local_psql
    args=SimpleNamespace(control_database='postgres_data_manager',control_schema='app_control',admin_database='postgres',user='native_probe')
    gen._ensure_manager_control_store(args)
    with lab.connect(args.control_database,autocommit=True) as c:
        exists=c.execute('SELECT count(*) FROM app_control.table_versions WHERE database_name=%s',(gen.DATABASE_NAME,)).fetchone()[0]
    if exists==0:gen._register_manager_initial_versions(args)
    with lab.connect(args.control_database,autocommit=True) as c:
        count=c.execute("SELECT count(*) FROM app_control.table_versions WHERE database_name=%s AND version_code='0000'",(gen.DATABASE_NAME,)).fetchone()[0]
        assert count==7,count
    print('Isolated control store and seven original initial versions ready',flush=True)


if __name__=='__main__':main()
