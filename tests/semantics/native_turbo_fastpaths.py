"""Differential checks for each turbo fast path against the pinned native code.

The original normalizer stays installed beside the turbo one, so every field of
every crafted payload is compared in one snapshot. Identity and FK cases drive
the actual loaders and validator through crafted relations.
"""
import itertools
import json
import uuid

from psycopg.types.json import Jsonb

from bench import native_c_lab as lab, native_turbo_probe as candidate
from bench import native_main_shared_probe as shared
from bench.common import ROOT, write_report

FIELDS = ('pipeline_domain_safe bool,pipeline_identifier int4,{middle},pipeline_axis_max int2,pipeline_numeric_1 int4,'
          'pipeline_numeric_2 int4,pipeline_integer_1 int2,text_column_1 text,text_column_2 text,text_column_3 text,'
          'pipeline_axes bytea,pipeline_axis_rows int2,pipeline_local_stamp timestamp')
COMPARED = ('pipeline_domain_safe', 'pipeline_identifier', 'pipeline_axis_max', 'pipeline_numeric_1', 'pipeline_numeric_2',
            'pipeline_integer_1', 'text_column_1', 'text_column_2', 'text_column_3', 'pipeline_axes', 'pipeline_axis_rows',
            'pipeline_local_stamp')
STAMPS = ['', None, 7, True, '2024-04-15 04:34:29.17-03', '2024-04-15 04:34:29-03', '2024-04-15 04:34:29.1-03',
          '2024-04-15 04:34:29.123456-03', '2024-04-15 04:34:29.1234567-03', '2024-04-15T04:34:29.17-03',
          '2024-04-15 04:34:29.17+00', '2024-04-15 04:34:29.17Z', '2024-04-15 04:34:29.17', '2024-04-15 04:34:29.17-03:00',
          '2024-04-15 04:34:29.17-0300', '2024-04-15 04:34:29.17-02', '2024-02-29 23:59:59.999999-03', '2023-02-29 01:00:00-03',
          '2024-13-01 00:00:00-03', '2024-04-31 00:00:00-03', '2024-04-15 24:00:00-03', '2024-04-15 23:60:00-03',
          '2024-04-15 23:59:60-03', '0001-01-01 00:00:00-03', '1000-06-15 12:00:00-03', '1899-12-31 23:59:59-03',
          '1914-01-01 02:59:59-03', '9999-12-31 23:59:59.999999-03', '2024-01-01 00:00:00.000000-03',
          # Sao Paulo summer time: the literal -03 instant renders one hour later.
          '2018-01-15 12:00:00-03', '2018-11-03 23:59:59-03', '2018-11-04 00:00:00-03', '2018-11-04 12:00:00-03',
          '2019-02-16 22:59:59-03', '2019-02-16 23:00:00-03', '2019-02-17 00:00:00-03', '2019-02-17 12:00:00-03',
          '2024-04-15 04:34:29.-03', '2024-04-15 04:34:29.17-03 ', ' 2024-04-15 04:34:29.17-03', 'not a timestamp']
TEXTS = ['', None, 'ABC', 'A', 'AB', 'ab', 'aB', 'Ab', ' AB', 'AB ', ' AB ', 'A B', '  ', ' ', '\tAB', 'AB\n', 'é', 'AÉ', 'ÉA', 'ß', 'ı', 'i',
         '853ZIC0451', 'A' * 128, 'A' * 129, 'é' * 64, 'é' * 65, ' ' + 'A' * 128, '12', '1a', 'Z9', '$', 7, 3.5, True, ['A'], {'k': 'A'}]
NUMBERS = ['', None, '1', '-1', '+1', '0', '9999', '10000', '1.5', '1.555', '999999.999', '1000000', 'x', 1, 1.5, True]


def payload(**changed):
    values = {'id_column_1': '1', 'text_column_1': 'NAME', 'text_column_2': 'AB', 'text_column_3': 'CD', 'integer_column_1': '5',
              'integer_column_2': '1', 'numeric_column_1': '1.5', 'numeric_column_2': '2.5', 'numeric_column_3': '1.25',
              'text_column_4': 'X', 'datetime_column_1': '2024-04-15 04:34:29.17-03'}
    values.update(changed)
    return values


def cases():
    for stamp in STAMPS:
        yield payload(datetime_column_1=stamp)
    for name, value in itertools.product(('text_column_1', 'text_column_2', 'text_column_3', 'text_column_4'), TEXTS):
        yield payload(**{name: value})
    for name, value in itertools.product(('id_column_1', 'integer_column_1', 'integer_column_2', 'numeric_column_1', 'numeric_column_3'), NUMBERS):
        yield payload(**{name: value})
    yield {}
    yield {'unrelated': 'x'}
    base = payload()
    yield {**base, **{'extra_key_' + str(i): 'x' for i in range(300)}}
    yield {**base, 'text_column_99': 'x', 'integer_column_16': '1', 'numeric_column_31': '1', 'id_column_2': 'x'}
    # Long objects move JEntry offsets onto their stored-offset branch.
    yield {**base, **{'pad_' + str(i): 'é' * 40 for i in range(120)}}
    yield {**base, 'text_column_17': 'x' * 3000, 'text_column_1': 'AFTER'}


def normalizer(c, checks):
    old = lab_extension(shared_package_info(), 'old')
    new = candidate.package('stamp')['extension']
    c.execute('CREATE SCHEMA turbo_new; CREATE SCHEMA turbo_old')
    c.execute('CREATE EXTENSION ' + new + ' WITH SCHEMA turbo_new')
    c.execute('CREATE EXTENSION ' + old + ' WITH SCHEMA turbo_old')
    c.execute('CREATE TEMP TABLE turbo_case(n int PRIMARY KEY,v jsonb)')
    payloads = list(cases())
    with c.cursor() as cursor:
        cursor.executemany('INSERT INTO turbo_case VALUES(%s,%s)', [(i, Jsonb(p)) for i, p in enumerate(payloads)])
    c.execute('INSERT INTO turbo_case VALUES(100000,NULL),(100001,\'[]\'),(100002,\'"x"\'),(100003,\'1\')')
    columns = ','.join('a.' + name + '::text AS a_' + name + ',b.' + name + '::text AS b_' + name for name in COMPARED)
    a = 'turbo_old.normalize(t.v) a(' + FIELDS.format(middle='pipeline_stamp text') + ')'
    b = 'turbo_new.normalize(t.v) b(' + FIELDS.format(middle='pipeline_native_flags int2') + ')'
    query = ('SELECT t.n,b.pipeline_native_flags,' + columns + ' FROM turbo_case t CROSS JOIN LATERAL ' + a + ' CROSS JOIN LATERAL ' + b + ' ORDER BY t.n')
    differences, safe, unsafe = [], 0, 0
    for row in c.execute(query).fetchall():
        flags, values = row[1], row[2:]
        for i, name in enumerate(COMPARED):
            if values[2 * i] != values[2 * i + 1]:
                differences.append({'case': row[0], 'field': name, 'old': str(values[2 * i]), 'new': str(values[2 * i + 1])})
        if values[0] == 'true':
            safe += 1
            main_bit = values[COMPARED.index('text_column_1') * 2] is not None and len(values[COMPARED.index('text_column_1') * 2].encode()) > 128
            code = values[COMPARED.index('text_column_3') * 2]
            detail_bit = code is not None and code != '' and not (len(code) == 2 and code.isascii() and code.isalpha() and code.isupper())
            if flags != (1 if main_bit else 0) + (2 if detail_bit else 0):
                differences.append({'case': row[0], 'field': 'flags', 'old': [main_bit, detail_bit], 'new': flags})
        else:
            unsafe += 1
    assert not differences, json.dumps(differences[:20], ensure_ascii=False, default=str)
    assert safe > 50 and unsafe > 20
    checks.append({'case': 'normalizer_old_versus_turbo', 'payloads': len(payloads) + 4, 'proven_safe': safe, 'unsafe': unsafe, 'differences': 0})
    print('normalizer PASS', len(payloads) + 4, 'payloads', safe, 'safe', unsafe, 'unsafe', flush=True)
    c.execute('DROP EXTENSION ' + old + ';DROP EXTENSION ' + new + ';DROP SCHEMA turbo_old;DROP SCHEMA turbo_new')


def shared_package_info():
    return json.loads((ROOT / 'reports/native-stamp-trusted-package.json').read_text())


def lab_extension(info, ignored):
    return info['extension']


def identity(c, checks):
    ext = candidate.package('main')['extension']
    c.execute('CREATE SCHEMA pgdm_turbo_id; CREATE EXTENSION ' + ext + ' WITH SCHEMA pgdm_turbo_id')
    stage = ('CREATE TEMP TABLE turbo_stage(raw_id bigint,pipeline_domain_safe bool,pipeline_identifier int4,pipeline_local_stamp timestamp,'
             'pipeline_axis_max int2,pipeline_numeric_1 int4,pipeline_numeric_2 int4,text_column_1 text,text_column_2 text)')
    c.execute(stage)
    c.execute("INSERT INTO turbo_stage SELECT g,true,g,'2024-01-01',1,1,1,'N','AA' FROM generate_series(1,5) g")
    c.execute("INSERT INTO public.table3(text_column_1,text_column_2,text_column_3,text_column_4) VALUES('N','SYNTHETIC/REFERENCE','A','A')")
    scenarios = [
        ('contiguous_from_native_start', 'CACHE 1000', None, [1, 2, 3, 4, 5], 5, None),
        ('continues_after_prior_values', 'CACHE 1000', 40, [41, 42, 43, 44, 45], 45, None),
        ('cache_one_keeps_per_row_nextval', 'CACHE 1', None, [1, 2, 3, 4, 5], 5, None),
        ('increment_two_keeps_per_row_nextval', 'CACHE 1000 INCREMENT 2', None, [1, 3, 5, 7, 9], None, None),
        ('range_short_keeps_native_exhaustion_error', 'CACHE 1000 MAXVALUE 3', None, None, None, 'reached maximum value'),
    ]
    results = []
    for name, options, start, expected, final, error in scenarios:
        c.execute('SAVEPOINT scenario')
        sequence = c.execute("SELECT pg_get_serial_sequence('public.table1','id_column_1')").fetchone()[0]
        c.execute('ALTER SEQUENCE ' + sequence + ' ' + options)
        if start is not None:
            c.execute("SELECT setval(%s,%s,true)", (sequence, start))
        try:
            count = c.execute("SELECT pgdm_turbo_id.main_load('turbo_stage','public.table1','public.table3')").fetchone()[0]
        except Exception as caught:
            assert error is not None and error in str(caught), caught
            c.execute('ROLLBACK TO SAVEPOINT scenario')
            results.append({'case': name, 'native_error': str(caught).splitlines()[0]})
            print(name, 'PASS', flush=True)
            continue
        assert error is None, name
        assert count == 5
        ids = [r[0] for r in c.execute('SELECT id_column_1 FROM public.table1 ORDER BY 1').fetchall()]
        assert ids == expected, (name, ids)
        if final is not None:
            assert c.execute('SELECT last_value,is_called FROM ' + sequence).fetchone() == (final, True), name
        results.append({'case': name, 'ids': ids})
        c.execute('ROLLBACK TO SAVEPOINT scenario')
        c.execute("SELECT setval(%s,1,false)", (sequence,))
        print(name, 'PASS', flush=True)
    # A zero-row load must not consume a sequence value.
    c.execute('SAVEPOINT scenario')
    sequence = c.execute("SELECT pg_get_serial_sequence('public.table1','id_column_1')").fetchone()[0]
    c.execute('ALTER SEQUENCE ' + sequence + ' CACHE 1000')
    c.execute('DELETE FROM turbo_stage')
    assert c.execute("SELECT pgdm_turbo_id.main_load('turbo_stage','public.table1','public.table3')").fetchone()[0] == 0
    assert c.execute('SELECT is_called FROM ' + sequence).fetchone() == (False,)
    c.execute('ROLLBACK TO SAVEPOINT scenario')
    results.append({'case': 'zero_rows_consume_nothing'})
    print('zero rows PASS', flush=True)
    c.execute('DROP EXTENSION ' + ext + ';DROP SCHEMA pgdm_turbo_id')
    c.execute("DELETE FROM public.table3;DROP TABLE turbo_stage")
    checks.append({'case': 'identity', 'scenarios': results})


FK_CHILDREN = 'public.table2,public.table5,public.table6'


def fk_validator(c, checks):
    ext = candidate.package('fk')['extension']
    arguments = "'public.raw_data','public.table1','public.table3','public.table4','public.table2','public.table5','public.table6'"
    results = []

    def add_foreign_keys():
        constraints = [('table1', 'turbo_a', 'id_column_3', 'public.raw_data(raw_id)'), ('table1', 'turbo_b', 'id_column_4', 'public.table3(id_column_1)'),
                       ('table2', 'turbo_c', 'id_column_2', 'public.table1(id_column_1)'), ('table5', 'turbo_d', 'id_column_2', 'public.table1(id_column_1)'),
                       ('table6', 'turbo_e', 'id_column_2', 'public.table1(id_column_1)'), ('table6', 'turbo_f', 'id_column_3', 'public.table4(id_column_1)')]
        for table, name, column, parent in constraints:
            c.execute(f'ALTER TABLE public.{table} ADD CONSTRAINT {name} FOREIGN KEY ({column}) REFERENCES {parent} ON UPDATE CASCADE ON DELETE CASCADE NOT VALID')

    c.execute('CREATE SCHEMA pgdm_turbo_fk; CREATE EXTENSION ' + ext + ' WITH SCHEMA pgdm_turbo_fk')
    existing = c.execute("SELECT conrelid::regclass::text,conname FROM pg_constraint WHERE contype='f' AND conrelid IN ('public.table1'::regclass,'public.table2'::regclass,'public.table5'::regclass,'public.table6'::regclass)").fetchall()
    for table, name in existing:
        c.execute(f'ALTER TABLE {table} DROP CONSTRAINT {name}')
    add_foreign_keys()
    # NOT VALID keys still fire on insert; only the validator may judge these rows.
    c.execute("SET LOCAL session_replication_role=replica")
    raw = c.execute('SELECT min(raw_id),max(raw_id) FROM public.raw_data').fetchone()
    c.execute("INSERT INTO public.table3(id_column_1,text_column_1,text_column_2,text_column_3,text_column_4) VALUES(1,'N','K','A','A')")
    c.execute("INSERT INTO public.table4(id_column_1,text_column_1,text_column_2) VALUES(1,'K','AA')")
    columns = c.execute("SELECT string_agg(quote_ident(attname),',') FROM pg_attribute WHERE attrelid='public.table1'::regclass AND attnum>0 AND attnotnull AND attidentity='' AND NOT attisdropped").fetchone()[0]
    assert columns in (None, 'id_column_3', 'id_column_2,id_column_3', 'id_column_3,id_column_4'), columns

    def child(raw_id, *, parent=1):
        c.execute('INSERT INTO public.table1(id_column_1,id_column_3,id_column_4) OVERRIDING SYSTEM VALUE VALUES(%s,%s,%s)', (parent, raw_id, 1))

    def run(expect):
        try:
            c.execute('SELECT pgdm_turbo_fk.fk_validate(' + arguments + ')').fetchone()
            return None
        except Exception as caught:
            return str(caught).splitlines()[0]

    def scenario(name, setup, expect_error):
        c.execute('SAVEPOINT scenario')
        setup()
        error = run(expect_error)
        assert (error is not None) == expect_error, (name, error)
        c.execute('ROLLBACK TO SAVEPOINT scenario')
        results.append({'case': name, 'native_violation': error})
        print(name, 'PASS', flush=True)

    scenario('existing_raw_key_accepted', lambda: child(raw[0]), False)
    scenario('missing_raw_key_rejected', lambda: child(raw[1] + 1), True)
    scenario('negative_raw_key_rejected', lambda: child(-5), True)

    def dangling_in_this_snapshot():
        # The index entry and an all-visible-looking page stay; only the heap check can reject it.
        c.commit() if False else None
        c.execute('DELETE FROM public.raw_data WHERE raw_id=%s', (raw[0],))
        child(raw[0])

    scenario('row_deleted_in_this_transaction_rejected', dangling_in_this_snapshot, True)
    scenario('table1_key_collected_for_children', lambda: (child(raw[0]), c.execute('INSERT INTO public.table6(id_column_2,id_column_3) VALUES(1,1)')), False)
    scenario('child_of_absent_table1_key_rejected', lambda: (child(raw[0]), c.execute('INSERT INTO public.table6(id_column_2,id_column_3) VALUES(2,1)')), True)
    checks.append({'case': 'fk_validator', 'scenarios': results})


def main():
    database = 'lab_turbo_fast_' + uuid.uuid4().hex[:8]
    lab.fixture(database)
    frozen = candidate.source_identity()
    checks = []
    with lab.connect(database) as c:
        normalizer(c, checks)
        c.rollback()
        c.execute('DELETE FROM public.table2;DELETE FROM public.table5;DELETE FROM public.table6;DELETE FROM public.table1;DELETE FROM public.table3;DELETE FROM public.table4')
        identity(c, checks)
        fk_validator(c, checks)
        c.rollback()
    with lab.connect(database, autocommit=True) as c:
        c.execute('VACUUM (FREEZE) public.raw_data')
        pages, visible = c.execute("SELECT relpages,relallvisible FROM pg_class WHERE oid='public.raw_data'::regclass").fetchone()
        assert pages > 0 and visible == pages, (pages, visible)
    # Second pass: an all-visible map selects the validator's index-only path.
    with lab.connect(database) as c:
        fk_validator(c, checks)
        c.rollback()
    assert candidate.source_identity() == frozen
    write_report('reports/semantics-native-turbo-fastpaths.json', {'kind': 'turbo_fast_path_differential', 'candidate_hook_sha256': frozen, 'database': database,
                 'all_passed': True, 'checks': checks,
                 'scope': 'Crafted payloads against the pinned original normalizer, actual loader identities and validator FK outcomes. Finite cases, not a universal proof.'})


if __name__ == '__main__':
    main()
