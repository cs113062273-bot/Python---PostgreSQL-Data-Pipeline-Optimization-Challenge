"""Proved <=3-decimal numerics become exact native scaled int32 once.

The existing sufficient proof bounds magnitude to 999999.999. Multiplication
by 1000 is therefore integral and fits int32, so original ROUND(...*1000)
is unchanged. Every conversion stays inside CASE on a once-evaluated native
ProjectSet proof. Unsafe input keeps the exact original whole-batch fallback.
Native identities and all final validation remain untouched.
"""
import hashlib
from pathlib import Path
import re
from bench import refined_copy_probe as refined,text_stage_probe as text
from bench.proof_pattern_probe import negative_condition
from pipeline.recipes import EXTRACT,destinations


def source_identity():
    digest=hashlib.sha256(refined.source_identity().encode());digest.update(Path(__file__).read_text(encoding='utf-8').encode());return digest.hexdigest()


def projection(scopes,*,logged=False):
    body=text.projection(scopes,logged=logged)
    marker='\n      FROM '
    assert body.count(marker)==1
    source=body[body.index(marker):].rstrip(';')
    inner='SELECT s.raw_id,n.*,pg_catalog.unnest(ARRAY['+negative_condition()+']) AS pipeline_domain_safe'+source
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    columns=[]
    for field in fields:
        if field.startswith('numeric_column_'):
            columns.append("CASE WHEN v.pipeline_domain_safe THEN (NULLIF(BTRIM(v."+field+"),'')::numeric*1000)::integer END AS "+field)
        else:columns.append('v.'+field)
    target='CREATE UNLOGGED TABLE challenge_lab.pipeline_normalized_input AS ' if logged else 'CREATE TEMP TABLE pipeline_normalized_input ON COMMIT DROP AS '
    return '-- lab exact scaled int32 source_sha256='+source_identity()+'\n'+target+'SELECT v.raw_id,'+','.join(columns)+',v.pipeline_domain_safe FROM ('+inner+') v;'


def call_end(body,start):
    i=start;depth=1;quote=None;commas=[]
    while i<len(body) and depth:
        ch=body[i]
        if quote:
            if ch==quote:
                if i+1<len(body) and body[i+1]==quote:i+=1
                else:quote=None
        elif ch in ("'",'"'):quote=ch
        elif ch=='(':depth+=1
        elif ch==')':depth-=1
        elif ch==',' and depth==1:commas.append(i)
        i+=1
    if depth:raise RuntimeError('Unbalanced trusted scaled-numeric expression')
    return i,commas


def numeric_fields(expression):
    fields=re.findall(r'source_row\.([a-z_]+\d+)',expression)
    return bool(fields) and all(field.startswith('numeric_column_') for field in fields)


def statement(body):
    for match in reversed(list(re.finditer(r'\bROUND\s*\(',body,re.I))):
        end,commas=call_end(body,match.end());argument=body[match.end():end-1]
        if not numeric_fields(argument):continue
        scale=re.search(r'\s*\*\s*1000\s*$',argument)
        suffix=re.match(r'\s*::integer\b',body[end:])
        if commas or scale is None or suffix is None:raise RuntimeError('Unknown canonical rounded numeric scaling')
        body=body[:match.start()]+'('+argument[:scale.start()]+')'+body[end+suffix.end():]
    for function in ('BTRIM','NULLIF'):
        for match in reversed(list(re.finditer(r'\b'+function+r'\s*\(',body,re.I))):
            end,commas=call_end(body,match.end());expression=body[match.end():end-1]
            if function=='NULLIF':
                if len(commas)!=1 or body[commas[0]+1:end-1].strip()!="''":continue
                expression=body[match.end():commas[0]]
            if not numeric_fields(expression):continue
            if function=='NULLIF':
                cast=re.match(r'\s*::numeric\b',body[end:])
                if cast is None:raise RuntimeError('Expected canonical numeric cast')
                end+=cast.end()
            body=body[:match.start()]+'('+expression+')'+body[end:]
    return body


def install():
    refined.install()
    import pipeline.parallel_normalization as parallel
    import pipeline.guarded_normalization as guarded
    parallel.prepare_input_sql=projection
    original=guarded.fast_statement
    def transformed(statement_text,scopes,mode=guarded.MODE):
        body=original(statement_text,scopes,mode)
        return statement(body) if mode=='guarded_copy' else body
    guarded.fast_statement=transformed
