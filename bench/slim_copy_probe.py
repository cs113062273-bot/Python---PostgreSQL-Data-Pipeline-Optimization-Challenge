"""Omit only canonical constant-NULL fields from binary COPY transport.

The complete original INSERT remains the fallback. The fast-path guard also
requires no defaults for omitted columns, so omitted COPY fields receive the
same native NULLs. Identity defaults, row receipts and constraint validation
remain native. No predicate, cast of source data, or nonconstant field is lost.
"""
import hashlib
from pathlib import Path
import re
from bench import null_cache_probe as base
from pipeline.recipes import destinations


def source_identity():
    return hashlib.sha256((base.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def split_projection(statement,table):
    match=re.match(r'INSERT INTO public\.'+re.escape(table)+r'\s*\((.*?)\)\s*SELECT\s+(DISTINCT\s+)?',statement,re.S)
    if not match:raise RuntimeError('Unknown canonical COPY INSERT/SELECT')
    columns=[s.strip() for s in match[1].split(',')]
    assert all(re.fullmatch('[a-z][a-z_0-9]*',column) for column in columns)
    start=i=match.end();depth=0;quote=None;expressions=[]
    while i<len(statement):
        ch=statement[i]
        if quote:
            if ch==quote:
                if i+1<len(statement) and statement[i+1]==quote:i+=1
                else:quote=None
        elif ch in ("'",'"'):quote=ch
        elif ch=='(':depth+=1
        elif ch==')':depth-=1;assert depth>=0
        elif depth==0 and ch==',':expressions.append(statement[start:i].strip());start=i+1
        elif depth==0 and statement[i:i+4].upper()=='FROM' and (i==0 or statement[i-1].isspace()) and statement[i+4].isspace():
            expressions.append(statement[start:i].strip())
            assert len(expressions)==len(columns)
            return columns,expressions,bool(match[2]),statement[i:]
        i+=1
    raise RuntimeError('Canonical top-level FROM missing')


def compact(statement,table):
    columns,expressions,distinct,suffix=split_projection(statement,table)
    omitted=[column for column,expr in zip(columns,expressions) if re.fullmatch(r'NULL\s*::\s*(?:character varying|numeric|integer|smallint|bigint)',expr,re.I)]
    if not omitted:return statement,[]
    kept=[(col,expr) for col,expr in zip(columns,expressions) if col not in omitted];assert kept
    return 'INSERT INTO public.'+table+' ('+','.join(col for col,expr in kept)+') SELECT '+('DISTINCT ' if distinct else '')+','.join(expr for col,expr in kept)+' '+suffix,omitted


def omissions():
    return {d['table_name'].split('.')[1]:compact(d['sql'],d['table_name'].split('.')[1])[1] for d in destinations()}


def default_guard_sql():
    predicates=[]
    for table,columns in omissions().items():
        if columns:
            names=','.join("'"+name+"'" for name in columns)
            predicates.append("NOT EXISTS(SELECT 1 FROM pg_catalog.pg_attrdef d JOIN pg_catalog.pg_attribute a ON a.attrelid=d.adrelid AND a.attnum=d.adnum WHERE d.adrelid='public."+table+"'::regclass AND a.attname IN ("+names+'))')
    return 'UPDATE pg_temp.pipeline_normalization_domain SET safe=safe AND ('+' AND '.join(predicates)+');'


def install():
    base.install()
    import pipeline.native_copy as native
    import pipeline.parallel_normalization as parallel
    original_copy=native.copy_sql;original_guard=native.layout_guard_sql;original_projection=parallel.prepare_input_sql
    expected=omissions()
    def copied(statement,table):
        body,removed=compact(statement,table)
        assert removed==expected[table],'Transformed constant-NULL projection changed'
        return original_copy(body,table)
    native.copy_sql=copied
    native.layout_guard_sql=lambda:original_guard()+'\n'+default_guard_sql()
    parallel.prepare_input_sql=lambda scopes,logged=False:'-- lab slim binary COPY source_sha256='+source_identity()+'\n'+original_projection(scopes,logged=logged)


def main():
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
