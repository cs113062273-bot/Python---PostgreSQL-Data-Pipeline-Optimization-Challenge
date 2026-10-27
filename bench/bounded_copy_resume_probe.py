"""Six-table bounded COPY/resume experiment, not a managed submission mode.

Uses only a fresh disposable official-generator fixture. Native psql owns all
binary row serialization and identity allocation. Receipts/data commit together
in private logged tables. Original public tables remain empty until publication.
This does not implement production fences, counters, versions or error fallback.
"""
import argparse
from contextlib import ExitStack
import hashlib
import os
from pathlib import Path
import re
import tempfile
import uuid

from psycopg import sql

from bench.common import ROOT, connect, write_report
from bench.fixture import create_fixture
from bench.run import execute, prepared
from bench.shadow_publish_probe import sequence, state, physical
from pipeline.checkpoints import source_digest
from pipeline.guarded_normalization import fast_statement
from pipeline.native_copy import single_line_sql
from pipeline.normalization import source_sql
from pipeline.parallel_normalization import prepare_input_sql, settings_sql
from pipeline.psql_session import PsqlSession
from pipeline.recipes import ORDER
from pipeline.workspace import RELATION
from tests.equivalence.oracle import snapshot, assert_exact, assert_relationships, protected_schema_signature, sequence_signature


def file_sha(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',default='reports/bounded-native-copy-resume-primitive.json')
    args=p.parse_args()
    db='lab_bounded_copy_'+uuid.uuid4().hex[:8];create_fixture(db)
    work='bounded_'+uuid.uuid4().hex[:8]
    parent=(ROOT/'.runtime/bounded_copy_tests').resolve();parent.mkdir(parents=True,exist_ok=True)
    directory=Path(tempfile.mkdtemp(prefix='probe_',dir=parent)).resolve()
    assert directory.parent==parent
    argv=[os.environ['CHALLENGE_PSQL'],'-h',os.environ.get('PGHOST','127.0.0.1'),'-p',os.environ.get('PGPORT','55432'),'-U',os.environ.get('PGUSER','challenge'),'-d',db,'-f','-']
    scopes=['group001'];cases=[];max_stage_rows=0;max_stage_bytes=0
    def cancelled(pid,name,epoch):
        with connect(db,autocommit=True) as c:
            if c.execute('SELECT 1 FROM pg_stat_activity WHERE pid=%s AND datname=current_database() AND usename=current_user AND extract(epoch FROM backend_start)=%s::numeric',(pid,epoch)).fetchone():
                c.execute('SELECT pg_terminate_backend(%s)',(pid,))
    def new_session():return PsqlSession(argv,cwd=directory,cancel_backend=cancelled,timeout=30)
    payload_columns={};native_sequences={}
    try:
        with connect(db,autocommit=True) as c:
            # Functional mutation only: sparse negative PKs test nullable lower
            # bounds. The generator/seed and ranked workloads are untouched.
            c.execute('UPDATE public.raw_data SET raw_id=-9000000000000000000+raw_id*1000003')
            execute(c,'baseline',scopes);snapshot(c)
            c.execute('TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY')
            source_before=source_digest(c,scopes);schema_before=protected_schema_signature(c);seq_before=sequence_signature(c)
            oids={t:c.execute('SELECT %s::regclass::oid',('public.'+t,)).fetchone()[0] for t in ORDER}
            with c.transaction():
                c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(work)))
                for t in ORDER:
                    c.execute(sql.SQL('CREATE TABLE {} (LIKE {} INCLUDING ALL)').format(sql.Identifier(work,t),sql.Identifier('public',t)))
                c.execute('SET LOCAL search_path=pg_catalog')
                for t in ORDER:
                    for name,definition in c.execute("SELECT conname,pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=%s::regclass AND contype='f'",('public.'+t,)).fetchall():
                        for other in ORDER:definition=definition.replace('public.'+other,work+'.'+other)
                        c.execute(sql.SQL('ALTER TABLE {} ADD CONSTRAINT {} ').format(sql.Identifier(work,t),sql.Identifier(name))+sql.SQL(definition))
                    payload_columns[t]=[r[0] for r in c.execute("SELECT attname FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped AND attidentity='' ORDER BY attnum",(work+'.'+t,))]
                    native_sequences[t]=sequence(c,work,t)
                    if native_sequences[t]:
                        assert c.execute('SELECT seqincrement,seqcache,seqcycle FROM pg_sequence WHERE seqrelid=%s::regclass',(native_sequences[t],)).fetchone()==(1,1,False)
                c.execute(f'''CREATE TABLE {work}.receipts(
                    chunk_no integer,table_name text,lo bigint,hi bigint,source_rows integer,
                    copied_rows bigint,native_lo bigint,native_hi bigint,explicit_ids smallint[],
                    file_sha256 text,completed_at timestamptz DEFAULT clock_timestamp(),
                    PRIMARY KEY(chunk_no,table_name))''')
        statements={d['pure_table_name']:d['statements'][0] for d in prepared('baseline',scopes)}

        def verify_receipts(observer=None):
            """Slow restart verification, never a fresh-path timing shortcut."""
            with ExitStack() as stack:
                c=observer if observer is not None else stack.enter_context(connect(db,autocommit=True))
                session=None if observer is not None else stack.enter_context(new_session())
                if source_digest(c,scopes)!=source_before:raise RuntimeError('Source fingerprint changed')
                rows=c.execute(f'SELECT chunk_no,table_name,copied_rows,native_lo,native_hi,explicit_ids,file_sha256 FROM {work}.receipts ORDER BY chunk_no,table_name').fetchall()
                totals={t:0 for t in ORDER}
                for chunk,t,n,lo,hi,ids,checksum in rows:
                    columns=','.join(payload_columns[t])
                    predicate=f'id_column_1 BETWEEN {lo} AND {hi}' if n and native_sequences[t] else 'id_column_1=ANY(ARRAY['+','.join(map(str,ids or []))+']::smallint[])' if not native_sequences[t] else 'false'
                    actual,distinct=c.execute(f'SELECT count(*),count(DISTINCT id_column_1) FROM {work}.{t} WHERE {predicate}').fetchone()
                    if actual!=n or distinct!=n:raise RuntimeError('Receipt identity range changed')
                    query=f'SELECT {columns} FROM {work}.{t} WHERE {predicate} ORDER BY id_column_1'
                    if session is not None:
                        session.send(f"\\copy ({query}) TO 'verify.bin' (FORMAT BINARY)\n")
                        actual_sha=file_sha(directory/'verify.bin')
                    else:
                        # Uncommitted fault injections are visible only in
                        # their owning transaction. COPY returns raw server
                        # bytes; this does not serialize/decode Python rows.
                        h=hashlib.sha256()
                        with c.cursor().copy(f'COPY ({query}) TO STDOUT (FORMAT BINARY)') as stream:
                            for block in stream:h.update(block)
                        actual_sha=h.hexdigest()
                    if actual_sha!=checksum:raise RuntimeError('Receipt payload changed')
                    totals[t]+=n
                for t,n in totals.items():
                    if c.execute(f'SELECT count(*) FROM {work}.{t}').fetchone()[0]!=n:raise RuntimeError('Unreceipted destination rows')
                return rows

        def load_chunk(session,chunk,lo,hi,n,interrupt=False,corrupt_file=False):
            nonlocal max_stage_rows,max_stage_bytes
            session.send('BEGIN ISOLATION LEVEL REPEATABLE READ; LOCK TABLE public.raw_data IN SHARE MODE;'+settings_sql()+" SET LOCAL work_mem='64MB';")
            source=source_sql(scopes)
            bound=source.rsplit('\n)',1)[0]+f' AND raw_id<={hi}'+('' if lo is None else f' AND raw_id>{lo}')+'\n)'
            projection=prepare_input_sql(scopes,logged=True).replace('challenge_lab.pipeline_normalized_input',work+'.input').replace(source,bound)
            session.send(projection)
            count,size,safe=session.send(f"SELECT count(*)||'|'||pg_total_relation_size('{work}.input')||'|'||bool_and(pipeline_domain_safe) FROM {work}.input;").strip().split('|')
            assert int(count)==n and n<=17 and safe=='true','This fixture-only primitive refuses unsupported domains'
            max_stage_rows=max(max_stage_rows,int(count));max_stage_bytes=max(max_stage_bytes,int(size))
            for t in ORDER:
                statement=fast_statement(statements[t],scopes,'guarded_copy').replace(RELATION,work+'.input')
                for other in ORDER:statement=statement.replace('public.'+other,work+'.'+other)
                match=re.fullmatch(r'INSERT INTO '+work+r'\.'+t+r'\s*\((.*?)\)\s*(SELECT\s.*?);?',statement,re.S)
                if not match:raise RuntimeError('Unknown canonical bounded SELECT')
                columns=','.join(x.strip() for x in match[1].split(','));query=match[2].rstrip(';')
                assert columns==','.join(payload_columns[t])
                # Set union for the two global DISTINCT dimensions. This is a
                # fixture experiment, not an eligibility/error-semantics claim.
                if t in ('table3','table4'):
                    query=f'SELECT * FROM ({query}) incoming({columns}) EXCEPT SELECT {columns} FROM {work}.{t}'
                if t=='table4':query=f'SELECT * FROM ({query}) ordered_dimension ORDER BY id_column_1'
                session.send(f"\\copy ({single_line_sql(query)}) TO '{t}.bin' (FORMAT BINARY)\n")
                checksum=file_sha(directory/(t+'.bin'))
                if corrupt_file and t=='table1':
                    path=directory/(t+'.bin');payload=path.read_bytes()
                    # Fault-only byte edit, not a COPY serializer. The pinned
                    # first field is a non-NULL int4 with no FK: flip its low
                    # bit without changing any header/length/type framing.
                    assert payload[:19]==b'PGCOPY\n\xff\r\n\0'+b'\0'*8
                    assert payload[21:25]==b'\0\0\0\x04'
                    path.write_bytes(payload[:28]+bytes([payload[28]^1])+payload[29:])
                session.send(f"\\copy {work}.{t} ({columns}) FROM '{t}.bin' (FORMAT BINARY)\n")
                if file_sha(directory/(t+'.bin'))!=checksum:raise RuntimeError('COPY file changed during import')
                # Must be the first SQL after COPY: real psql command tag, not
                # an estimated SELECT count or a later overwritten ROW_COUNT.
                if native_sequences[t]:
                    high=f"CASE WHEN :'ROW_COUNT'::bigint>0 THEN currval('{native_sequences[t]}') END"
                    low=f"({high})-:'ROW_COUNT'::bigint+1";ids='NULL::smallint[]'
                else:
                    high=low='NULL::bigint'
                    ids=f'(SELECT array_agg(id_column_1 ORDER BY id_column_1) FROM {work}.{t} WHERE id_column_1 NOT IN (SELECT unnest(explicit_ids) FROM {work}.receipts WHERE table_name=\'table4\'))'
                session.send(f"INSERT INTO {work}.receipts(chunk_no,table_name,lo,hi,source_rows,copied_rows,native_lo,native_hi,explicit_ids,file_sha256) VALUES({chunk},'{t}',{'NULL' if lo is None else lo},{hi},{n},:'ROW_COUNT'::bigint,{low},{high},{ids},'{checksum}');")
                if interrupt and t=='table1':
                    cancelled(session.backend_pid,session.application_name,session.backend_start_epoch)
                    try:session.send('SELECT 1;')
                    except RuntimeError:pass
                    else:raise AssertionError('Backend termination did not interrupt native COPY chunk')
                    return
            session.send(f'DROP TABLE {work}.input RESTRICT; COMMIT;')

        def boundary(lo):
            with connect(db,autocommit=True) as c:
                assert source_digest(c,scopes)==source_before
                return c.execute('SELECT max(raw_id),count(*) FROM (SELECT raw_id FROM public.raw_data WHERE raw_schema=ANY(%s) AND (%s::bigint IS NULL OR raw_id>%s) ORDER BY raw_id LIMIT 17) bounded',(scopes,lo,lo)).fetchone()

        hi,n=boundary(None)
        with new_session() as session:load_chunk(session,1,None,hi,n)
        committed_prefix=verify_receipts()
        with connect(db,autocommit=True) as c:
            prefix=physical(c,work)
            assert all(c.execute('SELECT count(*) FROM public.'+t).fetchone()[0]==0 for t in ORDER)
        cases.append({'name':'first_bounded_chunk_commits_private_rows_and_real_six_copy_receipts','passed':True,'public_targets_still_empty':True})
        next_hi,next_n=boundary(hi)
        try:
            with new_session() as session:load_chunk(session,2,hi,next_hi,next_n,corrupt_file=True)
        except RuntimeError as error:assert str(error)=='COPY file changed during import'
        else:raise AssertionError('Valid binary corruption was accepted')
        assert verify_receipts()==committed_prefix
        with connect(db,autocommit=True) as c:assert physical(c,work)==prefix
        cases.append({'name':'valid_binary_payload_file_corruption_rolls_back_whole_inflight_chunk','passed':True})
        with new_session() as session:load_chunk(session,2,hi,next_hi,next_n,interrupt=True)
        assert verify_receipts()==committed_prefix
        with connect(db,autocommit=True) as c:
            assert physical(c,work)==prefix
            assert c.execute('SELECT to_regclass(%s)',(work+'.input',)).fetchone()[0] is None
        cases.append({'name':'actual_backend_loss_rolls_back_inflight_chunk_and_stage_preserves_committed_prefix','passed':True})
        lo=hi;chunk=2
        while True:
            hi,n=boundary(lo)
            if not n:break
            verify_receipts()
            with new_session() as session:load_chunk(session,chunk,lo,hi,n)
            lo=hi;chunk+=1
        rows=verify_receipts()
        assert rows[:6]==committed_prefix
        cases.append({'name':'reconnect_resumes_from_receipts_without_reloading_first_chunk','passed':True,'chunks':chunk-1,'source_rows':64})
        # Tamper probes roll back their own edits. Integrity is checked from
        # actual rows/files, never merely the source's supplied raw_hash.
        for kind in ('payload_same_count','native_identity_shift','unreceipted_row','source_same_raw_hash'):
            with connect(db,autocommit=True) as c, c.transaction(force_rollback=True):
                if kind=='payload_same_count':c.execute(f"UPDATE {work}.table1 SET text_column_1=coalesce(text_column_1,'')||'edited' WHERE id_column_1=(SELECT min(id_column_1) FROM {work}.table1)")
                elif kind=='native_identity_shift':
                    # FK checks remain in force; use child table with no
                    # dependents, and shift beyond every valid native range.
                    c.execute(f'UPDATE {work}.table2 SET id_column_1=id_column_1+1000000 WHERE id_column_1=(SELECT min(id_column_1) FROM {work}.table2)')
                elif kind=='unreceipted_row':c.execute(f'INSERT INTO {work}.table3(text_column_1) VALUES(\'unreceipted\')')
                else:c.execute("UPDATE public.raw_data SET raw=jsonb_set(raw,'{values,text_column_1}',to_jsonb('tampered'::text)) WHERE raw_id=(SELECT min(raw_id) FROM public.raw_data)")
                expected={'payload_same_count':'Receipt payload changed','native_identity_shift':'Receipt identity range changed','unreceipted_row':'Unreceipted destination rows','source_same_raw_hash':'Source fingerprint changed'}[kind]
                try:verify_receipts(c)
                except RuntimeError as error:assert str(error)==expected
                else:raise AssertionError('Restart verifier accepted '+kind)
            verify_receipts();cases.append({'name':'detect_'+kind,'passed':True})
        with connect(db,autocommit=True) as c:
            # Publish into the original MVCC objects in dependency order.
            with c.transaction():
                c.execute('LOCK TABLE public.raw_data IN SHARE MODE')
                assert source_digest(c,scopes)==source_before
                for t in ORDER:
                    columns=[r[0] for r in c.execute('SELECT attname FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum',('public.'+t,))]
                    names=sql.SQL(',').join(map(sql.Identifier,columns))
                    c.execute(sql.SQL('INSERT INTO {} ({}) SELECT {} FROM {}').format(sql.Identifier('public',t),names,names,sql.Identifier(work,t)))
                    old_seq=sequence(c,'public',t)
                    if old_seq:
                        previous=state(c,old_seq);loaded=state(c,native_sequences[t])
                        assert loaded[0]>=previous[0]
                        c.execute('SELECT setval(%s::regclass,%s,%s)',(old_seq,*loaded))
                c.execute(sql.SQL('DROP TABLE '+','.join('{}' for _ in ORDER)+' RESTRICT').format(*(sql.Identifier(work,t) for t in ORDER)))
                c.execute(sql.SQL('DROP TABLE {} RESTRICT').format(sql.Identifier(work,'receipts')))
                c.execute(sql.SQL('DROP SCHEMA {} RESTRICT').format(sql.Identifier(work)))
            assert_exact(c);assert_relationships(c)
            assert protected_schema_signature(c)==schema_before and sequence_signature(c)==seq_before
            assert source_digest(c,scopes)==source_before
            assert {t:c.execute('SELECT %s::regclass::oid',('public.'+t,)).fetchone()[0] for t in ORDER}==oids
        cases.append({'name':'publish_exact_six_outputs_original_objects_schema_sequence_options_and_full_fks','passed':True})
        write_report(args.output,{'kind':'bounded_native_copy_resume_primitive','database':db,'passed':True,'cases':cases,'chunks':chunk-1,'source_rows':64,'sparse_negative_source_ids':True,'chunk_rows_cap':17,'maximum_normalized_stage_rows':max_stage_rows,'maximum_stage_bytes':max_stage_bytes,'receipt_count':len(rows),'zero_row_copy_receipts':sum(r[2]==0 for r in rows),'scope':'Small functional primitive only. Actual psql COPY/native identities, committed private progress, real backend loss and restart verification. No full input error fallback, concurrent fencing, managed counters/control versions, SSH, whole-pipeline performance or 300M resource claim.'})
        print('Bounded native COPY:',len(cases),'cases PASS;',chunk-1,'committed chunks;',max_stage_rows,'maximum stage rows',flush=True)
    finally:
        for name in (*ORDER,'verify'):(directory/(name+'.bin')).unlink(missing_ok=True)
        directory.rmdir()


if __name__=='__main__':main()
