"""Reuse total native trim/case transformations without altering source JSON."""
import hashlib
from pathlib import Path
import re
from pipeline.parallel_normalization import prepare_input_sql
from pipeline.recipes import EXTRACT,destinations


def source_identity():return hashlib.sha256(Path(__file__).read_text(encoding='utf-8').encode()).hexdigest()


def projection(scopes,*,logged=False):
    body=prepare_input_sql(scopes,logged=logged)
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    for field in fields:
        if not field.startswith('text_column_'):continue
        expression="p.payload->>'"+field+"'"
        assert body.count(expression)==1
        normalized='pg_catalog.btrim('+expression+')'
        if field!='text_column_1':normalized='pg_catalog.upper('+normalized+')'
        body=body.replace(expression,normalized,1)
    return '-- lab normalized text source_sha256='+source_identity()+'\n'+body


def statement(body):
    # Work only on trusted compiler expressions. Remove a wrapper iff every
    # source field it reads was normalized with that exact operation. Numeric
    # BTRIM calls and all native casts/filters remain. Descending edits preserve
    # offsets and cover the CASE selector used by table2 as well as simple keys.
    for function in ('BTRIM','UPPER'):
        matches=list(re.finditer(r'\b'+function+r'\s*\(',body,re.I))
        for match in reversed(matches):
            start=match.end();i=start;depth=1;quote=None
            while i<len(body) and depth:
                ch=body[i]
                if quote:
                    if ch==quote:
                        if i+1<len(body) and body[i+1]==quote:i+=1
                        else:quote=None
                elif ch in ("'",'"'):quote=ch
                elif ch=='(':depth+=1
                elif ch==')':depth-=1
                i+=1
            if depth:raise RuntimeError('Unbalanced trusted text expression')
            expression=body[start:i-1]
            fields=re.findall(r'source_row\.([a-z_]+\d+)',expression)
            if not fields or any(not f.startswith('text_column_') for f in fields):continue
            if function=='UPPER' and 'text_column_1' in fields:continue
            body=body[:match.start()]+'('+expression+')'+body[i:]
    return body
