"""Source-pinned STABLE timestamp normalization, private native lab only."""
import hashlib
import json
from pathlib import Path
from psycopg import sql
from bench import native_c_lab as lab,native_compact_sql as compact
from bench.common import ROOT,require_lab


def install_compact(c):
    require_lab(c)
    assert Path(c.execute('SHOW data_directory').fetchone()[0]).resolve()==(ROOT/'.runtime/native_pgdata').resolve()
    lab.install(c)
    build=json.loads((ROOT/'reports/native-typed-stamp-build.json').read_text())
    source=(ROOT/build['source']).resolve();library=(ROOT/build['library']).resolve()
    assert source.is_relative_to(ROOT/'native') and library.is_relative_to(ROOT/'.runtime/native_build')
    assert build['returncode']==0 and hashlib.sha256(source.read_bytes()).hexdigest()==build['source_sha256']
    assert hashlib.sha256(library.read_bytes()).hexdigest()==build['library_sha256']
    suffix=build['source_sha256'][:16]
    normal='stamp13_'+suffix;axes='stamp_axes_'+suffix;identity='stamp_identity_'+suffix
    for name,result,symbol,arguments,volatility in [
        (normal,'record','challenge_compact13','jsonb','STABLE'),
        (axes,'TABLE(position_number int2,weight int4,axis_kind text,gap int4)','challenge_axis_rows','bytea','IMMUTABLE'),
        (identity,'text','challenge_probe_identity','','IMMUTABLE')]:
        c.execute(sql.SQL('CREATE FUNCTION {}('+arguments+') RETURNS '+result+' AS {},{} LANGUAGE C '+volatility+' PARALLEL SAFE CALLED ON NULL INPUT SECURITY INVOKER').format(sql.Identifier('native_lab',name),sql.Literal(library.as_posix()),sql.Literal(symbol)))
    assert c.execute('SELECT native_lab.'+identity+'()').fetchone()==(build['source_sha256'],)
    return 'native_lab.'+normal,'native_lab.'+axes,build


def compact_definition():
    return lab.compact_definition()+',pipeline_local_stamp pg_catalog.timestamp'


def projection(scopes,function,relation):
    from pipeline.normalization import source_sql
    return 'CREATE TEMP TABLE '+relation.split('.')[1]+' ON COMMIT DROP AS SELECT s.raw_id,n.* FROM '+source_sql(scopes)+" s CROSS JOIN LATERAL "+function+"(s.raw->'values') n("+compact_definition()+');'


def statement(original,scopes,axes,relation='pg_temp.native_compact_case'):
    # The axis frame ABI is unchanged. The compiler's identifier guard only
    # recognizes its old native-lab spelling, so retag the verified symbol.
    old_axes='native_lab.axes_'+axes.rsplit('_',1)[1]
    body=compact.statement(original,scopes,old_axes,relation=relation).replace(old_axes,axes)
    needle="source_row.pipeline_stamp::timestamptz AT TIME ZONE 'America/Sao_Paulo'"
    if 'INSERT INTO public.table1' in body:
        assert body.count(needle)==1
        body=body.replace(needle,'source_row.pipeline_local_stamp')
    return body
