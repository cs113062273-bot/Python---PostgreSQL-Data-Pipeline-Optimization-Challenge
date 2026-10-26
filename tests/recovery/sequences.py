"""Native identity cache options restore on success, rollback and backend kill."""
import uuid
import psycopg
from bench.common import connect,write_report
from bench.fixture import create_fixture
from pipeline.bulk_sequences import prepare_sql,restore_sql,TABLES
from tests.equivalence.oracle import schema_signature


def signature(c):
    return c.execute("SELECT n.nspname,x.relname,s.seqtypid,s.seqstart,s.seqincrement,s.seqmax,s.seqmin,s.seqcache,s.seqcycle FROM pg_sequence s JOIN pg_class x ON x.oid=s.seqrelid JOIN pg_namespace n ON n.oid=x.relnamespace WHERE n.nspname='public' ORDER BY x.relname").fetchall()


def main():
    db='lab_sequence_cache_'+uuid.uuid4().hex[:8]
    create_fixture(db)
    results=[]
    with connect(db,autocommit=True) as observer:
        before=signature(observer);schema=schema_signature(observer)
        for table in TABLES:
            for change,expected in [('',1000),('CACHE 17',17),('CYCLE',1),('INCREMENT BY -1',1),('AS integer',1)]:
                with observer.transaction(force_rollback=True):
                    sequence=observer.execute('SELECT pg_get_serial_sequence(%s,%s)',('public.'+table,'id_column_1')).fetchone()[0]
                    from psycopg import sql
                    if change:observer.execute(sql.SQL('ALTER SEQUENCE {} '+change).format(sql.Identifier(*sequence.split('.'))))
                    altered=signature(observer)
                    observer.execute(prepare_sql(table))
                    cache=observer.execute('SELECT seqcache FROM pg_sequence WHERE seqrelid=%s::regclass',(sequence,)).fetchone()[0]
                    assert cache==expected,(table,change,cache)
                    observer.execute(restore_sql(table))
                    assert signature(observer)==altered,(table,change)
                assert signature(observer)==before and schema_signature(observer)==schema
                results.append({'table':table,'alteration':change or 'native','passed':True})
            for kill in (False,True):
                with connect(db,autocommit=True) as victim:
                    hit=False
                    try:
                        with victim.transaction():
                            victim.execute(prepare_sql(table))
                            victim.execute('SELECT nextval(pg_get_serial_sequence(%s,%s))',('public.'+table,'id_column_1'))
                            hit=True
                            if kill:
                                observer.execute('SELECT pg_terminate_backend(%s)',(victim.info.backend_pid,))
                                victim.execute('SELECT 1')
                            raise RuntimeError('failure after native cache allocation')
                    except (RuntimeError,psycopg.Error):
                        assert hit
                assert signature(observer)==before and schema_signature(observer)==schema
                results.append({'table':table,'failure':'backend_termination' if kill else 'exception','passed':True})
            with observer.transaction():
                observer.execute(prepare_sql(table))
                observer.execute('SELECT nextval(pg_get_serial_sequence(%s,%s))',('public.'+table,'id_column_1'))
                observer.execute(restore_sql(table))
            assert signature(observer)==before and schema_signature(observer)==schema
            results.append({'table':table,'commit_restoration':True,'passed':True})
    write_report('reports/recovery-sequence-cache.json',{'database':db,'tests':results,'all_passed':True,'scope':'Exact native sequence options/ownership schema; ordinary preallocation/rollback gaps allowed; no manual IDs or sequence rewind'})
    print('Native sequence cache metadata/rollback:',len(results),'PASS',flush=True)


if __name__=='__main__':main()
