"""Rollback-only complete FK revalidation with bounded maintenance memory."""
import argparse
import hashlib
from pathlib import Path
import statistics
import time
from psycopg import sql
from bench.common import connect,require_lab,environment,write_report
from pipeline.bulk_fks import FOREIGN_KEYS,restore_sql
from tests.equivalence.oracle import protected_schema_signature,sequence_signature
from tests.recovery.bulk_dump import sequence_state


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_native_variant_bb5fad3206')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/fk-memory-components-1m.json')
    p.add_argument('--leader-cpu',type=int,help='Diagnostic only: identical affinity for all tested settings')
    args=p.parse_args();frozen=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    report={'kind':'rollback_only_six_real_fk_validation_memory','official_score':False,'source_sha256':frozen,'runs':[]}
    with connect(args.database,autocommit=True) as c:
        require_lab(c)
        assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'Do not overlap a benchmark'
        if args.leader_cpu is not None:
            import psutil
            backend=psutil.Process(c.info.backend_pid)
            assert 'postgres' in backend.name().lower()
            original_affinity=backend.cpu_affinity()
            assert args.leader_cpu in original_affinity
            backend.cpu_affinity([args.leader_cpu])
            report['diagnostic_affinity']={'original':original_affinity,'measured':backend.cpu_affinity(),'scope':'This session only, before any timed statement; same setting for every variant. Connection exit terminates this backend.'}
        report['environment']=environment(c)
        before=protected_schema_signature(c);sequences=sequence_signature(c);values=sequence_state(c)
        original=c.execute('SHOW maintenance_work_mem').fetchone()[0]
        for repeat in range(args.repeats):
            modes=[64,128,256];shift=repeat%len(modes)
            for memory in modes[shift:]+modes[:shift]:
                with c.transaction(force_rollback=True):
                    c.execute("SET LOCAL search_path=pg_catalog; SET LOCAL statement_timeout='90s'")
                    c.execute('SET LOCAL maintenance_work_mem='+sql.Literal(str(memory)+'MB').as_string())
                    c.execute('CREATE TEMP TABLE pipeline_bulk_foreign_keys(table_name text,constraint_name text,definition text) ON COMMIT DROP')
                    for table,name,definition in FOREIGN_KEYS:
                        actual=c.execute('SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=%s::regclass AND conname=%s',('public.'+table,name)).fetchone()[0]
                        assert actual==definition
                        c.execute('INSERT INTO pg_temp.pipeline_bulk_foreign_keys VALUES(%s,%s,%s)',(table,name,definition))
                        c.execute(sql.SQL('ALTER TABLE {} DROP CONSTRAINT {}').format(sql.Identifier('public',table),sql.Identifier(name)))
                    started=time.perf_counter();c.execute(restore_sql());seconds=time.perf_counter()-started
                    assert c.execute('SELECT count(*) FROM pg_constraint WHERE conrelid=ANY(%s::regclass[]) AND contype=\'f\' AND convalidated',(['public.'+t for t in ('table1','table2','table5','table6')],)).fetchone()[0]==6
                    report['runs'].append({'repeat':repeat,'maintenance_memory_mb':memory,'seconds':seconds,'all_six_constraints_validated':True})
                    print(repeat,memory,round(seconds,4),flush=True);write_report(args.output,report)
                assert protected_schema_signature(c)==before and sequence_signature(c)==sequences and sequence_state(c)==values
                assert c.execute('SHOW maintenance_work_mem').fetchone()[0]==original
        report['summary']={str(m):statistics.median(r['seconds'] for r in report['runs'] if r['maintenance_memory_mb']==m) for m in modes}
        report['original_schema_sequence_settings_restored']=True;write_report(args.output,report)


if __name__=='__main__':main()
