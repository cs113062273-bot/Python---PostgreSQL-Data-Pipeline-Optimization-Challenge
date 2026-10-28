"""Helpers confined to the separate unqualified C research cluster on 55433."""
import hashlib
import json
import os
from pathlib import Path
import psycopg
from psycopg import sql
from bench.common import ROOT,require_lab
from pipeline.recipes import EXTRACT,destinations

PORT=55433
FIELDS=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
assert len(FIELDS)==64
COMPACT_FIELDS=[('pipeline_domain_safe','bool'),('pipeline_identifier','int4'),('pipeline_stamp','text'),('pipeline_axis_max','int2'),('pipeline_numeric_1','int4'),('pipeline_numeric_2','int4'),('pipeline_integer_1','int2'),('text_column_1','text'),('text_column_2','text'),('text_column_3','text'),('pipeline_axes','bytea'),('pipeline_axis_rows','int2')]


def connect(database='postgres',**kwargs):
    c=psycopg.connect(host='127.0.0.1',port=PORT,user='native_probe',dbname=database,connect_timeout=5,**kwargs)
    assert Path(c.execute('SHOW data_directory').fetchone()[0]).resolve()==(ROOT/'.runtime/native_pgdata').resolve()
    return c


def fixture(database,rows=64):
    # Only this disposable child process changes its own libpq defaults.
    os.environ.update({'PGHOST':'127.0.0.1','PGPORT':str(PORT),'PGUSER':'native_probe','CHALLENGE_DSN':'host=127.0.0.1 port=55433 user=native_probe'})
    from bench.fixture import create_fixture
    create_fixture(database,rows=rows)


def install(c):
    require_lab(c)
    build=json.loads((ROOT/'reports/native-c-probe-build.json').read_text())
    assert build['returncode']==0 and build['application_integration'] is False
    assert hashlib.sha256((ROOT/'native/challenge_probe.c').read_bytes()).hexdigest()==build['source_sha256']
    library=(ROOT/build['library']).resolve();assert library.is_relative_to(ROOT/'.runtime/native_build')
    assert hashlib.sha256(library.read_bytes()).hexdigest()==build['library_sha256']
    c.execute('CREATE SCHEMA native_lab')
    extract='extract64_'+build['source_sha256'][:16];identity='identity_'+build['source_sha256'][:16]
    c.execute(sql.SQL("CREATE FUNCTION {}(pg_catalog.jsonb) RETURNS record AS {},'challenge_extract64' LANGUAGE C IMMUTABLE PARALLEL SAFE CALLED ON NULL INPUT").format(sql.Identifier('native_lab',extract),sql.Literal(library.as_posix())))
    c.execute(sql.SQL("CREATE FUNCTION {}() RETURNS pg_catalog.text AS {},'challenge_probe_identity' LANGUAGE C IMMUTABLE PARALLEL SAFE").format(sql.Identifier('native_lab',identity),sql.Literal(library.as_posix())))
    assert c.execute(sql.SQL('SELECT {}()').format(sql.Identifier('native_lab',identity))).fetchone()[0]==build['source_sha256']
    return 'native_lab.'+extract,build


def definition():
    return ','.join(field+' pg_catalog.text' for field in FIELDS)+',pipeline_domain_safe pg_catalog.bool'


def compact_definition():
    return ','.join(name+' pg_catalog.'+kind for name,kind in COMPACT_FIELDS)


def install_compact(c):
    _,build=install(c)
    compact='compact12_'+build['source_sha256'][:16];axes='axes_'+build['source_sha256'][:16]
    library=(ROOT/build['library']).resolve()
    c.execute(sql.SQL("CREATE FUNCTION {}(pg_catalog.jsonb) RETURNS record AS {},'challenge_compact12' LANGUAGE C IMMUTABLE PARALLEL SAFE CALLED ON NULL INPUT").format(sql.Identifier('native_lab',compact),sql.Literal(library.as_posix())))
    c.execute(sql.SQL("CREATE FUNCTION {}(pg_catalog.bytea) RETURNS TABLE(position_number pg_catalog.int2,weight pg_catalog.int4,axis_kind pg_catalog.text,gap pg_catalog.int4) AS {},'challenge_axis_rows' LANGUAGE C IMMUTABLE PARALLEL SAFE ROWS 14").format(sql.Identifier('native_lab',axes),sql.Literal(library.as_posix())))
    return 'native_lab.'+compact,'native_lab.'+axes,build
