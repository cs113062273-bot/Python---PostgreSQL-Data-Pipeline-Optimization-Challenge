"""Research loader installation restricted to the separate 55433 cluster."""
import hashlib
import json
from pathlib import Path
from psycopg import sql
from bench.common import ROOT,require_lab


def install(c):
    require_lab(c)
    assert Path(c.execute('SHOW data_directory').fetchone()[0]).resolve()==(ROOT/'.runtime/native_pgdata').resolve()
    build=json.loads((ROOT/'reports/native-axis-ingest-build.json').read_text())
    assert build['returncode']==0
    assert hashlib.sha256((ROOT/'native/challenge_axis_ingest.c').read_bytes()).hexdigest()==build['source_sha256']
    manifest=json.loads((ROOT/'vendor/postgresql-17.11/SOURCE.json').read_text())
    for record in manifest['files']:
        assert hashlib.sha256((ROOT/record['path']).read_bytes()).hexdigest()==record['sha256']
    library=(ROOT/build['library']).resolve()
    assert library.is_relative_to(ROOT/'.runtime/native_build')
    assert hashlib.sha256(library.read_bytes()).hexdigest()==build['library_sha256']
    function='ingest_axes_'+build['source_sha256'][:16]
    c.execute(sql.SQL("CREATE FUNCTION {}(regclass,regclass,regclass) RETURNS bigint AS {},'challenge_axis_ingest' LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT SECURITY INVOKER").format(sql.Identifier('native_lab',function),sql.Literal(library.as_posix())))
    c.execute(sql.SQL('REVOKE ALL ON FUNCTION {}(regclass,regclass,regclass) FROM PUBLIC').format(sql.Identifier('native_lab',function)))
    return 'native_lab.'+function,{**build,'vendor_sources':manifest}
