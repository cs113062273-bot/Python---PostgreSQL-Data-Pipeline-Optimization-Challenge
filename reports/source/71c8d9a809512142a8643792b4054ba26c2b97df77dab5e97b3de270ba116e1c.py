"""Isolated typed-stage research; never changes the default compiler.

Per-row conservative domain proof precedes every typed expression through a
native single-element-array barrier. A false global proof still executes the
unaltered original SQL. All counters, COPY tags, keys, FKs and versions remain.
Invoke `service` with bench.service_run arguments, or `semantics` with the
native differential driver's arguments. Actual batch SQL archives this hook's
source SHA so compiler-plus-batch provenance remains reviewable.
"""
import hashlib
from pathlib import Path
import re
import sys

from pipeline.parallel_normalization import prepare_input_sql as original_projection
from pipeline.safe_domain import condition_sql


def source_identity():
    return hashlib.sha256(Path(__file__).read_text(encoding='utf-8').encode()).hexdigest()


def typed_projection(scopes,*,logged=False):
    from pipeline.recipes import EXTRACT,destinations
    original=original_projection(scopes,logged=logged)
    target='SELECT s.raw_id,n.*,'+condition_sql()+' AS pipeline_domain_safe'
    assert original.count(target)==1
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    typed=[]
    for name in fields:
        kind='smallint' if name.startswith('integer_column_') else 'numeric' if name.startswith('numeric_column_') else 'integer' if name=='id_column_1' else 'timestamptz' if name=='datetime_column_1' else None
        typed.append(f"CASE WHEN proof.safe THEN NULLIF(BTRIM(n.{name}),'')::{kind} END AS {name}" if kind else 'n.'+name)
    original=original.replace(target,'SELECT s.raw_id,'+','.join(typed)+',proof.safe AS pipeline_domain_safe',1)
    assert original.endswith(';')
    original=original[:-1]+'\nCROSS JOIN LATERAL pg_catalog.unnest(ARRAY['+condition_sql()+']) proof(safe);'
    return '-- lab typed stage source_sha256='+source_identity()+'\n'+original


def typed_statement(statement):
    # Rewrite only the exact pinned numeric/date NULLIF(BTRIM(...),'') casts.
    # CASE selectors generated for axes contain multiple same-family columns.
    # Text trimming/casing and the native casts to the requested output types
    # remain. Unsupported expression shapes fail closed in this laboratory.
    marker='NULLIF(BTRIM('
    pieces=[];last=0;start=statement.find(marker)
    while start>=0:
        begin=start+len(marker);depth=1;i=begin
        while i<len(statement) and depth:
            if statement[i]=='(':depth+=1
            elif statement[i]==')':depth-=1
            i+=1
        if depth:raise RuntimeError('Unbalanced canonical cast expression')
        expression=statement[begin:i-1]
        suffix=re.match(r",\s*''\)::(smallint|numeric|integer|timestamptz)\b",statement[i:])
        if not suffix:raise RuntimeError('Unknown canonical NULLIF cast')
        fields=re.findall(r'source_row\.([a-z_]+\d+)',expression)
        if not fields or any(not (f.startswith(('integer_column_','numeric_column_')) or f in ('id_column_1','datetime_column_1')) for f in fields):
            raise RuntimeError('Unknown typed-stage field')
        end=i+suffix.end()
        pieces.extend((statement[last:start],'('+expression+')::'+suffix[1]))
        last=end;start=statement.find(marker,last)
    pieces.append(statement[last:])
    return ''.join(pieces)


def install():
    import pipeline.parallel_normalization as projection
    import pipeline.guarded_normalization as guarded
    projection.prepare_input_sql=typed_projection
    original=guarded.fast_statement
    def patched(statement,scopes,mode=guarded.MODE):
        return typed_statement(original(statement,scopes,mode))
    guarded.fast_statement=patched


def main():
    if len(sys.argv)<2 or sys.argv[1] not in ('service','semantics'):
        raise SystemExit('Use service or semantics followed by driver arguments')
    action=sys.argv.pop(1);install()
    if action=='service':
        from bench.service_run import main as driver
    else:
        from tests.semantics.native_copy import main as driver
    driver()


if __name__=='__main__':main()
