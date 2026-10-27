"""Small real-DB publication experiments; never a submission mode.

Compare replacing table objects with copying already-native identity rows into
the original objects. Check an older RR reader, rollback, schema, sequence
options/forward state and the exact six-table oracle. Each method has its own
untouched-generator fixture and preserves the completed diagnostic database.
"""
import argparse
import hashlib
import uuid

from psycopg import sql

from bench.common import connect,write_report
from bench.fixture import create_fixture
from bench.run import execute,prepared
from pipeline.checkpoints import source_digest,normalize_catalog
from pipeline.normalization import source_sql
from pipeline.recipes import ORDER
from tests.equivalence.oracle import snapshot,assert_exact,assert_relationships,protected_schema_signature,sequence_signature


def sequence(c,schema,table):
    return c.execute('SELECT pg_get_serial_sequence(%s,%s)',(schema+'.'+table,'id_column_1')).fetchone()[0]


def state(c,name):
    parts=c.execute('SELECT n.nspname,r.relname FROM pg_class r JOIN pg_namespace n ON n.oid=r.relnamespace WHERE r.oid=%s::regclass',(name,)).fetchone()
    return c.execute(sql.SQL('SELECT last_value,is_called FROM {}').format(sql.Identifier(*parts))).fetchone()


def physical(c,schema):
    result={}
    for table in ORDER:
        h=hashlib.sha256()
        with c.cursor().copy(sql.SQL('COPY (SELECT * FROM {} ORDER BY id_column_1) TO STDOUT (FORMAT BINARY)').format(sql.Identifier(schema,table))) as copy:
            for block in copy:h.update(block)
        result[table]=h.hexdigest()
    return result


def rewrite(statement,scopes,schema):
    source=source_sql(scopes)
    if statement.count(source)!=1:raise RuntimeError('Protected source shape changed')
    before,_,after=statement.partition(source)
    # Canonical prefix/suffix contain only pinned recipe text. The complete
    # source clause, including arbitrary quoted scope values, stays untouched.
    for table in ORDER:
        before=before.replace('public.'+table,schema+'.'+table)
        after=after.replace('public.'+table,schema+'.'+table)
    return before+source+after


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',default='reports/shadow-publication-primitives.json')
    args=p.parse_args();results=[]
    for method in ('rename','copy_into_original'):
        db='lab_publish_'+uuid.uuid4().hex[:8];create_fixture(db)
        work='shadow_'+uuid.uuid4().hex[:8];archive='old_'+uuid.uuid4().hex[:8]
        with connect(db,autocommit=True) as c:
            execute(c,'baseline',['group001']);snapshot(c)
            expected_counts={t:c.execute('SELECT count(*) FROM public.'+t).fetchone()[0] for t in ORDER}
            c.execute('TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY')
            # A non-initial native state prevents assuming empty targets imply
            # sequence start values. This is fixture setup, not ID generation.
            for table in ORDER:
                native_sequence=sequence(c,'public',table)
                if native_sequence:c.execute('SELECT setval(%s::regclass,500,true)',(native_sequence,))
            schema_before=protected_schema_signature(c);seq_before=sequence_signature(c)
            source_before=source_digest(c,['group001'])
            oids={t:c.execute('SELECT %s::regclass::oid',('public.'+t,)).fetchone()[0] for t in ORDER}
            with c.transaction():
                # Require qualified relation names in server-deparsed DDL.
                c.execute('SET LOCAL search_path=pg_catalog')
                keys={t:c.execute("SELECT conname,pg_get_constraintdef(oid),contype FROM pg_constraint WHERE conrelid=%s::regclass AND contype IN ('p','u','f') ORDER BY contype,conname",('public.'+t,)).fetchall() for t in ORDER}
                indexes={t:c.execute('''SELECT pg_get_indexdef(i.indexrelid) FROM pg_index i
                    WHERE i.indrelid=%s::regclass AND NOT EXISTS(SELECT 1 FROM pg_constraint k WHERE k.conindid=i.indexrelid)''',('public.'+t,)).fetchall() for t in ORDER}
            with c.transaction():
                c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(work)))
                for table in ORDER:
                    c.execute(sql.SQL('CREATE TABLE {} (LIKE {} INCLUDING ALL EXCLUDING INDEXES)').format(sql.Identifier(work,table),sql.Identifier('public',table)))
                    native_sequence=sequence(c,'public',table)
                    if native_sequence:
                        old=state(c,native_sequence);new=state(c,sequence(c,work,table))
                        assert old[0]>=new[0],'Never rewind a sequence even in a diagnostic clone'
                        c.execute('SELECT setval(%s::regclass,%s,%s)',(sequence(c,work,table),*old))
                for table in ORDER:
                    for name,definition,kind in keys[table]:
                        if kind=='f':continue
                        c.execute(sql.SQL('ALTER TABLE {} ADD CONSTRAINT {} ').format(sql.Identifier(work,table),sql.Identifier(name))+sql.SQL(definition))
                    for (definition,) in indexes[table]:
                        marker=' ON public.'+table+' '
                        assert definition.count(marker)==1
                        c.execute(definition.replace(marker,' ON '+work+'.'+table+' ',1))
                for table in ORDER:
                    for name,definition,kind in keys[table]:
                        if kind!='f':continue
                        for parent in ORDER:definition=definition.replace('public.'+parent,work+'.'+parent)
                        c.execute(sql.SQL('ALTER TABLE {} ADD CONSTRAINT {} ').format(sql.Identifier(work,table),sql.Identifier(name))+sql.SQL(definition))
                for d in prepared('baseline',['group001']):
                    for statement in d['statements']:c.execute(rewrite(statement,['group001'],work))
            shadow_before=physical(c,work)
            def publish(fault=False):
                with c.transaction():
                    if method=='rename':
                        c.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(archive)))
                        for table in ORDER:c.execute(sql.SQL('ALTER TABLE {} SET SCHEMA {}').format(sql.Identifier('public',table),sql.Identifier(archive)))
                    for n,table in enumerate(ORDER):
                        if method=='rename':c.execute(sql.SQL('ALTER TABLE {} SET SCHEMA public').format(sql.Identifier(work,table)))
                        else:
                            columns=[r[0] for r in c.execute("SELECT attname FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum",('public.'+table,))]
                            names=sql.SQL(',').join(map(sql.Identifier,columns))
                            c.execute(sql.SQL('INSERT INTO {} ({}) SELECT {} FROM {}').format(sql.Identifier('public',table),names,names,sql.Identifier(work,table)))
                            # Transfer actual native sequence high-water state
                            # forward only; actual IDs were generated by the
                            # shadow table's native identity default, not Python.
                            native_sequence=sequence(c,'public',table)
                            if native_sequence:
                                current=state(c,native_sequence);loaded=state(c,sequence(c,work,table))
                                if loaded[0]>current[0] or loaded[0]==current[0] and loaded[1] and not current[1]:
                                    c.execute('SELECT setval(%s::regclass,%s,%s)',(native_sequence,*loaded))
                        if fault and n==2:raise RuntimeError('publication injected before commit')
                    old_schema=archive if method=='rename' else work
                    c.execute(sql.SQL('DROP TABLE '+','.join('{}' for _ in ORDER)+' RESTRICT').format(*(sql.Identifier(old_schema,t) for t in ORDER)))
                    c.execute(sql.SQL('DROP SCHEMA {} RESTRICT').format(sql.Identifier(old_schema)))
                    if method=='rename':c.execute(sql.SQL('DROP SCHEMA {} RESTRICT').format(sql.Identifier(work)))
            try:publish(fault=True)
            except RuntimeError as error:assert 'publication injected' in str(error)
            else:raise AssertionError('Publication rollback fault not reached')
            assert normalize_catalog(protected_schema_signature(c))==normalize_catalog(schema_before)
            assert sequence_signature(c)==seq_before and physical(c,work)==shadow_before
            assert all(c.execute('SELECT count(*) FROM public.'+t).fetchone()[0]==0 for t in ORDER)
            with connect(db,autocommit=True) as reader:
                reader.execute('BEGIN ISOLATION LEVEL REPEATABLE READ')
                reader.execute('SELECT count(*) FROM pg_catalog.pg_class').fetchone()
                publish()
                older_count=reader.execute('SELECT count(*) FROM public.table1').fetchone()[0]
                reader.execute('ROLLBACK')
            assert_exact(c);assert_relationships(c)
            assert normalize_catalog(protected_schema_signature(c))==normalize_catalog(schema_before)
            assert sequence_signature(c)==seq_before and source_digest(c,['group001'])==source_before
            new_oids={t:c.execute('SELECT %s::regclass::oid',('public.'+t,)).fetchone()[0] for t in ORDER}
            if method=='copy_into_original':assert new_oids==oids and older_count==0
            for t in ORDER:
                assert c.execute('SELECT count(*) FROM public.'+t).fetchone()[0]==expected_counts[t]
                native_sequence=sequence(c,'public',t)
                if native_sequence:assert state(c,native_sequence)[0]>=500+expected_counts[t]
            row={'method':method,'database':db,'publication_rollback_preserves_private_progress_and_empty_public':True,'exact_six_outputs_and_schema':True,'native_sequence_options_ownership_preserved':True,'sequence_state_forward_only':True,'source_unchanged':True,'public_object_oids_preserved':new_oids==oids,'older_rr_reader_table1_rows':older_count,'expected_old_snapshot_rows':0,'satisfies_original_rr_visibility':older_count==0,'scope':'Small publication primitive only. No native managed-counter/version integration, no whole-pipeline performance claim, no completed chunk protocol.'}
            results.append(row);write_report(args.output,{'kind':'shadow_publication_strategy_experiments','results':results})
            print(method,'exact graph/schema PASS; old RR rows',older_count,'OIDs preserved',new_oids==oids,flush=True)


if __name__=='__main__':main()
