"""Native PostgreSQL tests of the sufficient safe domain and boundary cases."""
import random
import uuid
from bench.common import connect,write_report
from bench.fixture import create_fixture
from pipeline.normalization import prepare_sql
from pipeline.safe_domain import gate_sql,condition_parts
from pipeline.guarded_normalization import EXTRA_DATA_FUNCTIONS


def main():
    database='lab_domain_'+uuid.uuid4().hex[:8]
    create_fixture(database)
    results=[]
    cases=[
        ('integer_column_2','9999',True),('integer_column_2','-9999',True),('integer_column_2','10000',False),
        ('integer_column_2','1|2',False),('integer_column_2','+',False),('integer_column_2','1\n',False),('integer_column_2','\n',False),
        ('numeric_column_3','999999.999',True),('numeric_column_3','-999999.999',True),('numeric_column_3','1000000',False),
        ('numeric_column_3','1.2345',False),('numeric_column_3','NaN',False),('numeric_column_3','Infinity',False),
        ('numeric_column_3','[]',False),('numeric_column_3','1|2',False),('numeric_column_3',' ',False),('numeric_column_3',None,True),
        ('id_column_1','1999999999',True),('id_column_1','-1999999999',True),('id_column_1','2147483648',False),
        ('datetime_column_1','0001-01-31 12:59:59+00',True),('datetime_column_1','0000-01-01 00:00:00+00',False),
        ('datetime_column_1','1900-02-29 00:00:00+00',False),('datetime_column_1','2000-02-29 00:00:00+00',True),
        ('datetime_column_1','2024-04-31 00:00:00+00',False),('datetime_column_1','2024-02-30 00:00:00+00',False),
        ('datetime_column_1','2024-01-01 24:00:00+00',False),('datetime_column_1','2024-01-01 00:00:60+00',False),
        ('datetime_column_1','9999-12-31T23:59:59.999999Z',True),('datetime_column_1','2024-06-30 00:00:00+12:59',True),
        ('datetime_column_1','2024-01-01 00:00:00\n',False),('datetime_column_1','',True),
    ]
    with connect(database,autocommit=True) as conn:
        metadata=dict(conn.execute("SELECT proname,bool_and(provolatile='i' AND NOT prosecdef) FROM pg_proc WHERE pronamespace='pg_catalog'::regnamespace AND proname=ANY(%s) GROUP BY proname",(list(EXTRA_DATA_FUNCTIONS),)).fetchall())
        assert len(metadata)==len(EXTRA_DATA_FUNCTIONS) and all(metadata.values()),metadata
        with conn.transaction(force_rollback=True):
            conn.execute(prepare_sql(['group001'],all_tables=True,defer_index=True))
            for field,value,expected in cases:
                with conn.transaction(force_rollback=True):
                    conn.execute(f'UPDATE pg_temp.pipeline_normalized_input SET {field}=%s WHERE raw_id=1',(value,))
                    safe=conn.execute(gate_sql()).fetchone()[0]
                    assert safe is expected,(field,value,safe,expected)
                    if safe and value not in (None,''):
                        if field=='datetime_column_1':conn.execute('SELECT %s::timestamptz',(value,))
                        elif field.startswith('numeric'):conn.execute('SELECT ROUND(%s::numeric*1000)::integer',(value,))
                        else:conn.execute('SELECT %s::'+('smallint' if field=='integer_column_2' else 'integer'),(value,))
                results.append({'field':field,'value':value,'accepted_domain':expected,'passed':True})
        rng=random.Random(12043)
        dates=[f'{rng.randrange(0,10000):04d}-{rng.randrange(1,15):02d}-{rng.randrange(0,36):02d} {rng.randrange(0,26):02d}:{rng.randrange(0,63):02d}:{rng.randrange(0,63):02d}+00' for _ in range(2000)]
        for style in ('ISO, MDY','SQL, DMY','German, DMY'):
            conn.execute("SELECT set_config('DateStyle',%s,false)",(style,))
            # pg_input_is_valid is used only as a test oracle, never by the
            # production gate or function-guard exception.
            rows=conn.execute('SELECT c.stamp,'+condition_parts()[3].replace('%','%%')+",pg_input_is_valid(c.stamp,'timestamptz') FROM unnest(%s::text[]) c(stamp)",(dates,)).fetchall()
            assert all(not safe or valid for _,safe,valid in rows),style
            assert any(safe for _,safe,_ in rows)
    write_report('reports/safe-domain-boundaries.json',{'kind':'native_safe_domain_tests','database':database,'boundaries':results,'random_calendar_cases':6000,'all_passed':True,'immutable_function_metadata':metadata})
    print(f'Safe domain: {len(results)} boundaries, 6000 calendar/style cases, immutable data functions: PASS',flush=True)


if __name__=='__main__':main()
