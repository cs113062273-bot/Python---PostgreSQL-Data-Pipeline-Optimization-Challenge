"""Install the table1 prototype only in the separate native lab cluster."""
import hashlib
import json
from pathlib import Path
from psycopg import sql
from bench.common import ROOT,require_lab


def install(c,build_report='reports/native-main-ingest-build.json'):
    require_lab(c)
    assert Path(c.execute('SHOW data_directory').fetchone()[0]).resolve()==(ROOT/'.runtime/native_pgdata').resolve()
    receipt=(ROOT/build_report).resolve();assert receipt.is_relative_to(ROOT/'reports')
    build=json.loads(receipt.read_text())
    assert build['returncode']==0
    source=(ROOT/build['source']).resolve();assert source.is_relative_to(ROOT/'native') and source.suffix=='.c'
    assert hashlib.sha256(source.read_bytes()).hexdigest()==build['source_sha256']
    vendor=json.loads((ROOT/'vendor/postgresql-17.11/SOURCE.json').read_text())
    for item in vendor['files']:assert hashlib.sha256((ROOT/item['path']).read_bytes()).hexdigest()==item['sha256']
    library=(ROOT/build['library']).resolve();assert library.is_relative_to(ROOT/'.runtime/native_build')
    assert hashlib.sha256(library.read_bytes()).hexdigest()==build['library_sha256']
    name='main_'+build['source_sha256'][:16]
    identity='main_identity_'+build['source_sha256'][:16]
    c.execute(sql.SQL("CREATE FUNCTION {}(regclass,regclass,regclass) RETURNS bigint AS {},'challenge_main_ingest' LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT SECURITY INVOKER").format(sql.Identifier('native_lab',name),sql.Literal(library.as_posix())))
    c.execute(sql.SQL("CREATE FUNCTION {}() RETURNS text AS {},'challenge_main_identity' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER").format(sql.Identifier('native_lab',identity),sql.Literal(library.as_posix())))
    c.execute(sql.SQL('REVOKE ALL ON FUNCTION {}(regclass,regclass,regclass) FROM PUBLIC').format(sql.Identifier('native_lab',name)))
    assert c.execute(sql.SQL('SELECT {}()').format(sql.Identifier('native_lab',identity))).fetchone()==(build['source_sha256'],)
    return 'native_lab.'+name,{**build,'vendor_sources':vendor}


def code_domain(relation,dimension='public.table3'):
    # The SQL join uses LEFT/RIGHT over uppercased text. Actual dimension
    # endpoints must be single ASCII characters; longer source strings can
    # still join when their first and last characters match.
    return "SELECT current_setting('server_encoding')='UTF8' AND (SELECT collisdeterministic FROM pg_collation WHERE oid='default'::regcollation) AND (SELECT coalesce(bool_and(pipeline_domain_safe AND (text_column_1 IS NULL OR octet_length(text_column_1)<=128)),true) FROM "+relation+") AND NOT EXISTS(SELECT 1 FROM "+dimension+" WHERE text_column_2='SYNTHETIC/REFERENCE' AND numeric_column_1 IS NULL AND text_column_1 IS NOT NULL AND text_column_3 IS NOT NULL AND text_column_4 IS NOT NULL AND (octet_length(text_column_1)>128 OR octet_length(text_column_1)=0 OR octet_length(text_column_3)<>1 OR octet_length(text_column_4)<>1 OR ascii(text_column_3) NOT BETWEEN 65 AND 90 OR ascii(text_column_4) NOT BETWEEN 65 AND 90))"
