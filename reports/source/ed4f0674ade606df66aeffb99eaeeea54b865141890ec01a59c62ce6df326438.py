"""Install the details prototype only in the separate native lab cluster."""
import hashlib
import json
from pathlib import Path
from psycopg import sql
from bench.common import ROOT,require_lab


def install(c,build_report='reports/native-details-ingest-build.json'):
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
    name='details_'+build['source_sha256'][:16]
    identity='details_identity_'+build['source_sha256'][:16]
    c.execute(sql.SQL("CREATE FUNCTION {}(regclass,regclass,regclass,regclass,regclass,regclass) RETURNS bigint[] AS {},'challenge_details_ingest' LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT SECURITY INVOKER").format(sql.Identifier('native_lab',name),sql.Literal(library.as_posix())))
    c.execute(sql.SQL("CREATE FUNCTION {}() RETURNS text AS {},'challenge_details_identity' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER").format(sql.Identifier('native_lab',identity),sql.Literal(library.as_posix())))
    c.execute(sql.SQL('REVOKE ALL ON FUNCTION {}(regclass,regclass,regclass,regclass,regclass,regclass) FROM PUBLIC').format(sql.Identifier('native_lab',name)))
    assert c.execute(sql.SQL('SELECT {}()').format(sql.Identifier('native_lab',identity))).fetchone()==(build['source_sha256'],)
    return 'native_lab.'+name,{**build,'vendor_sources':vendor}


def code_domain(relation):
    # Source text has already undergone original trim/upper. Non-ASCII or
    # differently shaped valid input selects the complete original fallback.
    return "SELECT coalesce(bool_and(pipeline_domain_safe AND (text_column_3 IS NULL OR text_column_3='' OR text_column_3 ~ '^[A-Z]{2}$')),true) FROM "+relation
