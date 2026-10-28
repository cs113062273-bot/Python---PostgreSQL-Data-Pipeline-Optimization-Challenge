"""Actual post-success crash: minimal WAL must retain every acknowledged row.

Uses only the separately verified 55433 cluster, without concurrent clients.
Both data commit and six control versions have completed before immediate stop.
"""
import hashlib
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import uuid
import psycopg
from psycopg import sql
from bench import native_c_lab as lab,native_minimal_probe as candidate
from bench.common import ROOT,write_report
from bench.run import execute
from bench.control import seed_initial_versions
from pipeline.local_service import LocalService
from pipeline.bulk_fks import BulkFKMixin
from pipeline.bulk_indexes import BulkIndexMixin
from pipeline.guarded_normalization import GuardedNormalizationMixin
from pipeline.recipes import ORDER,destinations,bulk_unique_tables
from pipeline.checkpoints import physical_fingerprints,source_digest
from pipeline.upstream import PostgresAdminService
from tests.equivalence.oracle import snapshot,assert_exact,protected_schema_signature,sequence_signature
from tests.recovery.bulk_dump import sequence_state,history


def state(db):
    with lab.connect(db,autocommit=True) as c:
        assert c.execute('SHOW wal_level').fetchone()==('minimal',)
        assert all(c.execute('SHOW '+name).fetchone()[0]=='on' for name in ('fsync','full_page_writes','synchronous_commit'))
        assert_exact(c)
        assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()==(None,)
        sequence_logs={}
        for schema,name,increment,cycle in c.execute("SELECT schemaname,sequencename,increment_by,cycle FROM pg_sequences WHERE schemaname='public'").fetchall():
            assert increment==1 and cycle is False
            sequence_logs[schema+'.'+name]=c.execute(sql.SQL('SELECT last_value,log_cnt,is_called FROM {}').format(sql.Identifier(schema,name))).fetchone()
        return {'physical':physical_fingerprints(c),'source':source_digest(c,['group001']),
                'catalog':protected_schema_signature(c),'sequence_options':sequence_signature(c),
                'sequence_values':sequence_state(c),'sequence_wal_reservations':sequence_logs,'counters':c.execute('SELECT to_jsonb(c) FROM public.pgdm_table_row_counts c ORDER BY schema_name,table_name').fetchall()}


def main(*,expected_files=None):
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',default='reports/recovery-native-minimal-postcommit-durable.json');args=p.parse_args()
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    os.environ.update({'PGHOST':'127.0.0.1','PGPORT':'55433','PGUSER':'native_probe','CHALLENGE_DSN':'host=127.0.0.1 port=55433 user=native_probe','CHALLENGE_PSQL':str(ROOT/'.runtime/postgresql/pgsql/bin/psql.exe')})
    with lab.connect('postgres',autocommit=True) as c:
        assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid()").fetchall(),'Do not crash a cluster with other clients'
        assert c.execute('SHOW wal_level').fetchone()==('minimal',)
    db='lab_native_minimal_durable_'+uuid.uuid4().hex[:8];lab.fixture(db);seed_initial_versions(db)
    with lab.connect(db,autocommit=True) as c:
        execute(c,'baseline',['group001']);snapshot(c)
        c.execute('TRUNCATE '+','.join('public.'+t for t in ORDER)+' RESTART IDENTITY')
    frozen=candidate.source_identity();candidate.install()
    class Service(BulkFKMixin,BulkIndexMixin,GuardedNormalizationMixin,LocalService):
        bulk_foreign_keys=guarded_normalization=True
        bulk_recipe_mode='guarded_copy'
        bulk_unique_tables=bulk_unique_tables('guarded_copy')
    service=Service()
    timing=PostgresAdminService.create_cross_table_expansion_timing_report(db,'public.raw_data',['group001'],[d['table_name'] for d in destinations()])
    result=service.execute_cross_table_expansion(db,'public.raw_data',['group001'],destinations('guarded_copy'),timing_report=timing)
    diagnostics=getattr(service._expansion_timing_local,'local_backend_diagnostics',None)
    if expected_files is not None:assert diagnostics['actual_binary_files']==expected_files
    versions=service.register_cross_table_expansion_versions(result,'research','native-minimal-postcommit-crash',timing_report=timing)
    assert len(versions)==6 and result['execution_architecture']['safe_domain'] is True
    from bench.fast_transport_probe import close_sessions
    close_sessions(service)
    before=state(db)
    with lab.connect('postgres_data_manager',autocommit=True) as c:before_versions=history(c,db)
    with lab.connect('postgres',autocommit=True) as c:
        assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND pid<>pg_backend_pid()").fetchall()
    report={'kind':'native_minimal_acknowledged_commit_immediate_crash','database':db,'passed':False,'candidate_hook_sha256':frozen,
            'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'before':before,
            'six_versions_before':before_versions,'execution_architecture':result['execution_architecture'],'actual_backend_diagnostics':diagnostics,
            'scope':'64 actual rows; both data and all version commits acknowledged before actual immediate stop. Same isolated PostgreSQL cluster, not a power cut, post-data/pre-version saga or recovery performance score.'}
    write_report(output,report)
    directory=ROOT/'.runtime/native_pgdata';executable=ROOT/'.runtime/postgresql/pgsql/bin/pg_ctl.exe'
    log=ROOT/'.runtime/native-minimal-postcommit-recovery.log'
    with output.with_suffix('.pgctl.txt').open('x') as writer:
        print('Six exact outputs and versions acknowledged; immediate isolated server stop',flush=True)
        subprocess.run([str(executable),'-D',str(directory),'-m','immediate','-t','60','-w','stop'],stdout=writer,stderr=subprocess.STDOUT,check=True)
        subprocess.run([str(executable),'-D',str(directory),'-l',str(log),'-o','-h 127.0.0.1 -p 55433 -c wal_level=minimal -c max_wal_senders=0 -c fsync=on -c full_page_writes=on -c synchronous_commit=on','-W','start'],stdout=writer,stderr=subprocess.STDOUT,check=True)
    start=time.monotonic()
    while True:
        try:
            with lab.connect('postgres',autocommit=True) as c:c.execute('SELECT 1')
            break
        except psycopg.OperationalError:
            if time.monotonic()-start>180:raise
            time.sleep(1)
    after=state(db)
    # Native sequences WAL-log future reservations. A crash may consume that
    # reservation without losing any committed row. Check the recorded exact
    # bound; do not demand a gapless sequence or ignore arbitrary changes.
    before_core={k:v for k,v in before.items() if k not in ('sequence_values','sequence_wal_reservations')}
    after_core={k:v for k,v in after.items() if k not in ('sequence_values','sequence_wal_reservations')}
    assert after_core==before_core
    reservations=[]
    for name,(old_value,log_count,called) in before['sequence_wal_reservations'].items():
        value,remaining,now_called=after['sequence_wal_reservations'][name]
        assert old_value<=value<=old_value+log_count and called==now_called,(name,old_value,log_count,value)
        assert after['sequence_values'][name]==(value,now_called)
        reservations.append({'sequence':name,'before':old_value,'wal_reserved_through':old_value+log_count,'after':value,'within_preexisting_native_reservation':True})
    with lab.connect('postgres_data_manager',autocommit=True) as c:assert history(c,db)==before_versions
    assert candidate.source_identity()==frozen
    report.update({'passed':True,'all_acknowledged_physical_rows_source_counters_catalog_sequence_options_versions_retained':True,'actual_post_success_server_crash':True,'native_sequence_reservations_checked':reservations,'sequence_last_value_exact_claimed':False})
    write_report(output,report);print('Durable minimal-WAL successful commit survives immediate server crash: PASS',flush=True)


if __name__=='__main__':main()
