"""Read-only cost probe for actual-content source fingerprints.

The existing chunk protocol already hashes entire rows on PostgreSQL. This
compares its batched JSON/hex framing to a byte-identical TEXT COPY stream and
an explicitly different research-only binary-record framing. No protocol or
production source fingerprint is changed by this benchmark.
"""
import argparse
import hashlib
import statistics
import threading
import time

from bench.common import connect,environment,require_lab,write_report
from pipeline.checkpoints import source_digest_legacy as source_digest
from pipeline.parallel_normalization import settings_sql


JSON_COPY="""COPY (SELECT raw_id::text||':'||encode(sha256(convert_to(to_jsonb(t)::text,'UTF8')),'hex')
    FROM public.raw_data t WHERE raw_schema=ANY(%s) ORDER BY raw_id) TO STDOUT"""
BINARY_COPY="""COPY (SELECT raw_id,sha256(record_send(t))
    FROM public.raw_data t WHERE raw_schema=ANY(%s) ORDER BY raw_id) TO STDOUT (FORMAT BINARY)"""
BINARY_BARRIER_COPY="""COPY (SELECT t.raw_id,h.content_sha FROM public.raw_data t
    CROSS JOIN LATERAL unnest(ARRAY[sha256(record_send(t))]) h(content_sha)
    WHERE raw_schema=ANY(%s) ORDER BY t.raw_id) TO STDOUT (FORMAT BINARY)"""


def streamed_digest(conn,scopes,*,binary=False,barrier=False):
    result=hashlib.sha256();wire_bytes=0
    query=BINARY_BARRIER_COPY if barrier else BINARY_COPY if binary else JSON_COPY
    with conn.cursor().copy(query,(scopes,)) as copy:
        for block in copy:
            result.update(block);wire_bytes+=len(block)
    return result.hexdigest(),wire_bytes


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_template_1m')
    p.add_argument('--repeats',type=int,default=1)
    p.add_argument('--modes',nargs='+',choices=['json_batched','json_copy','json_copy_parallel','binary_copy_parallel','binary_barrier_parallel'],default=['json_batched','json_copy','json_copy_parallel','binary_copy_parallel'])
    p.add_argument('--output',default='reports/source-hash-probe-1m.json')
    args=p.parse_args()
    if args.repeats<1:p.error('Positive repeat count required')
    if any(m.startswith('json_') for m in args.modes) and 'json_batched' not in args.modes:p.error('Include json_batched to verify the legacy digest contract')
    with connect(args.database,autocommit=True) as c:
        require_lab(c)
        report={'kind':'read_only_source_content_fingerprint_components','official_score':False,'database':args.database,'environment':environment(c),'source_rows':c.execute('SELECT count(*) FROM public.raw_data').fetchone()[0],'runs':[],'scope':'Read-only same-source hash component probe; not complete pipeline timing. The legacy function already hashes actual full rows in PostgreSQL. json_copy must match its exact existing ASCII digest framing. Binary record framing is research only and requires a new versioned hash contract before production use. Parallel settings are local to each probe transaction. Worker observation is sampled.'}
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY raw_schema')]
        expected_json=expected_binary=None
        for repeat in range(args.repeats):
            shift=repeat%len(args.modes)
            for mode in args.modes[shift:]+args.modes[:shift]:
                print('Source hash',repeat+1,mode,'starting',flush=True)
                stopped=threading.Event();ready=threading.Event();observation={'max_workers':0,'error':None}
                def observe():
                    try:
                        with connect('postgres',autocommit=True) as other:
                            ready.set()
                            while not stopped.is_set():
                                n=other.execute('SELECT count(*) FROM pg_stat_activity WHERE leader_pid=%s',(c.info.backend_pid,)).fetchone()[0]
                                observation['max_workers']=max(observation['max_workers'],n)
                                stopped.wait(.1)
                    except Exception as error:
                        observation['error']=type(error).__name__;ready.set()
                observer=threading.Thread(target=observe,daemon=True);observer.start();assert ready.wait(15)
                try:
                    with c.transaction():
                        c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
                        c.execute('LOCK TABLE public.raw_data IN SHARE MODE')
                        if mode.endswith('_parallel'):
                            c.execute(settings_sql());c.execute("SET LOCAL work_mem='64MB'")
                        started=time.perf_counter()
                        if mode=='json_batched':digest=source_digest(c,scopes);wire=None
                        else:digest,wire=streamed_digest(c,scopes,binary=mode.startswith('binary_'),barrier='barrier' in mode)
                        seconds=time.perf_counter()-started
                finally:
                    stopped.set();observer.join(15)
                assert not observer.is_alive()
                if mode.startswith('json_'):
                    if expected_json is None:expected_json=digest
                    assert digest==expected_json,'Legacy JSON source fingerprint changed'
                else:
                    if expected_binary is None:expected_binary=digest
                    assert digest==expected_binary,'Binary research fingerprint changed'
                row={'repeat':repeat,'mode':mode,'seconds':seconds,'digest':digest,'copy_bytes_received':wire,'workers_observed':observation['max_workers'],'observation_error':observation['error']}
                report['runs'].append(row);write_report(args.output,report)
                check='legacy digest verified' if mode.startswith('json_') else 'separate research digest stable'
                print(f'Source hash {mode}: {seconds:.4f}s, workers {observation["max_workers"]}; {check}',flush=True)
        report['summary']={mode:{'median_seconds':statistics.median(r['seconds'] for r in report['runs'] if r['mode']==mode)} for mode in args.modes}
        report['json_variants_same_legacy_digest']=True if expected_json is not None else None
        write_report(args.output,report)


if __name__=='__main__':main()
