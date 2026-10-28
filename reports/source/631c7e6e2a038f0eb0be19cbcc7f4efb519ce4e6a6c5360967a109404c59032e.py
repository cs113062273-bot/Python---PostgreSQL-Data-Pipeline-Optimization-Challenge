"""Same-session complete native export/load component, constant-NULL fields."""
import argparse
import hashlib
from pathlib import Path
import statistics
import tempfile
import time
import uuid
from psycopg import sql
from bench.common import ROOT,connect,environment,require_lab,write_report
from bench.slim_copy_probe import compact,split_projection,source_identity,omissions
from pipeline.recipes import destinations,ORDER
from tests.equivalence.oracle import protected_schema_signature,sequence_signature
from tests.recovery.bulk_dump import sequence_state


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',default='lab_native_variant_6c1c55a43e')
    p.add_argument('--repeats',type=int,default=3)
    p.add_argument('--output',default='reports/slim-transport-components-1m.json')
    args=p.parse_args();frozen=source_identity()
    report={'kind':'rollback_only_native_binary_copy_null_column_omission','official_score':False,'source_sha256':frozen,'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'runs':[],
            'scope':'Same session, all six actual output tables exported and loaded into private logged native-layout copies. Native identities/cache/NOT NULL/CHECK, exact two typed fingerprints including omitted NULL fields, original metadata/sequence states verified after rollback. Source transformation, indexes/FKs/final commit/versions excluded; not a complete processing score.'}
    directory=Path(tempfile.mkdtemp(prefix='slim_copy_',dir=ROOT/'.runtime')).resolve();path=directory/'transfer.bin'
    headers={d['table_name'].split('.')[1]:d['sql'] for d in destinations()};removed=omissions()
    try:
        with connect(args.database,autocommit=True) as c:
            require_lab(c)
            assert not c.execute("SELECT pid FROM pg_stat_activity WHERE backend_type='client backend' AND state<>'idle' AND pid<>pg_backend_pid()").fetchall(),'Do not overlap processing'
            report['environment']=environment(c);before=protected_schema_signature(c);seq=sequence_signature(c);values=sequence_state(c)
            expected={};all_columns={}
            for table in ORDER:
                columns=[r[0] for r in c.execute("SELECT attname FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum",('public.'+table,))]
                if table!='table4':columns.remove('id_column_1')
                all_columns[table]=columns;record=sql.SQL('ROW({})').format(sql.SQL(',').join(map(sql.Identifier,columns)))
                query=sql.SQL('SELECT count(*),sum(hash_record_extended({},17)::numeric)::text,sum(hash_record_extended({},97)::numeric)::text FROM {}').format(record,record,sql.Identifier('public',table))
                expected[table]=c.execute(query).fetchone()
                for column in removed[table]:assert not c.execute(sql.SQL('SELECT 1 FROM {} WHERE {} IS NOT NULL LIMIT 1').format(sql.Identifier('public',table),sql.Identifier(column))).fetchone()
            for repeat in range(args.repeats):
                modes=['full','slim','slim_text'];shift=repeat%len(modes)
                for mode in modes[shift:]+modes[:shift]:
                    assert source_identity()==frozen
                    with c.transaction(force_rollback=True):
                        c.execute('LOCK TABLE '+','.join('public.'+t for t in ORDER)+' IN SHARE MODE');c.execute("SET LOCAL statement_timeout='90s'")
                        schema='slim_transport_'+uuid.uuid4().hex[:8];c.execute('CREATE SCHEMA '+schema)
                        row={'repeat':repeat,'mode':mode,'tables':{}}
                        for table in ORDER:
                            target=sql.Identifier(schema,table);c.execute(sql.SQL('CREATE TABLE {} (LIKE {} INCLUDING DEFAULTS INCLUDING IDENTITY INCLUDING CONSTRAINTS EXCLUDING INDEXES)').format(target,sql.Identifier('public',table)))
                            sequence=c.execute('SELECT pg_get_serial_sequence(%s,%s)',(schema+'.'+table,'id_column_1')).fetchone()[0]
                            if sequence:c.execute(sql.SQL('ALTER SEQUENCE {} CACHE 1000').format(sql.SQL(sequence)))
                            body=headers[table] if mode=='full' else compact(headers[table],table)[0]
                            columns=split_projection(body,table)[0];names=sql.SQL(',').join(map(sql.Identifier,columns))
                            assert not path.exists()
                            format_name='TEXT' if mode=='slim_text' else 'BINARY'
                            started=time.perf_counter();c.execute(sql.SQL('COPY (SELECT {} FROM {}) TO {} (FORMAT '+format_name+')').format(names,sql.Identifier('public',table),sql.Literal(path.as_posix())));export=time.perf_counter()-started
                            size=path.stat().st_size
                            started=time.perf_counter();cur=c.execute(sql.SQL('COPY {} ({}) FROM {} (FORMAT '+format_name+')').format(target,names,sql.Literal(path.as_posix())));load=time.perf_counter()-started
                            path.unlink();record=sql.SQL('ROW({})').format(sql.SQL(',').join(map(sql.Identifier,all_columns[table])))
                            actual=c.execute(sql.SQL('SELECT count(*),sum(hash_record_extended({},17)::numeric)::text,sum(hash_record_extended({},97)::numeric)::text FROM {}').format(record,record,target)).fetchone()
                            assert actual==expected[table],(table,mode,actual,expected[table])
                            row['tables'][table]={'export_seconds':export,'load_seconds':load,'file_bytes':size,'format':format_name,'actual_command_tag':cur.statusmessage,'all_nonidentity_values_fingerprint':actual,'omitted_columns':removed[table] if mode!='full' else []}
                        row['copy_seconds']=sum(v['export_seconds']+v['load_seconds'] for v in row['tables'].values());row['file_bytes']=sum(v['file_bytes'] for v in row['tables'].values())
                        report['runs'].append(row);write_report(args.output,report);print(repeat,mode,round(row['copy_seconds'],4),row['file_bytes'],'bytes',flush=True)
                    assert protected_schema_signature(c)==before and sequence_signature(c)==seq and sequence_state(c)==values
            report['summary']={mode:{'median_copy_seconds':statistics.median(r['copy_seconds'] for r in report['runs'] if r['mode']==mode),'file_bytes':next(r['file_bytes'] for r in report['runs'] if r['mode']==mode)} for mode in modes}
            report['all_private_work_rolled_back_original_sequences_unchanged']=True;write_report(args.output,report)
    finally:path.unlink(missing_ok=True);directory.rmdir()


if __name__=='__main__':main()
