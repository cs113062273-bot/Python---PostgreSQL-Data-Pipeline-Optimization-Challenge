"""Trim/empty-normalize scalar TEXT once, retaining every native cast.

The proof applies to the same BTRIM/NULLIF values as original SQL. Whitespace
can enter the fast domain only after all original bounded-cast checks pass.
No numeric/date conversion or identity allocation is moved into the cache.
"""
import hashlib
from pathlib import Path
import re
import sys
from bench import refined_copy_probe as refined,text_stage_probe as text
from bench.proof_pattern_probe import negative_condition
from pipeline.safe_domain import condition_sql
from pipeline.recipes import EXTRACT,destinations


def source_identity():
    digest=hashlib.sha256(refined.source_identity().encode())
    digest.update(Path(__file__).read_text(encoding='utf-8').encode())
    return digest.hexdigest()


def projection(scopes,*,logged=False):
    body=text.projection(scopes,logged=logged)
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    for field in fields:
        if field.startswith('text_column_'):continue
        expression="p.payload->>'"+field+"'"
        assert body.count(expression)==1
        body=body.replace(expression,"NULLIF(pg_catalog.btrim("+expression+"),'')",1)
    assert body.count(condition_sql())==1
    body=body.replace(condition_sql(),negative_condition(),1)
    if logged:body=body.replace('CREATE TABLE challenge_lab.pipeline_normalized_input AS','CREATE UNLOGGED TABLE challenge_lab.pipeline_normalized_input AS',1)
    return '-- lab normalized scalar TEXT source_sha256='+source_identity()+'\n'+body


def statement(body):
    for function in ('BTRIM','NULLIF'):
        for match in reversed(list(re.finditer(r'\b'+function+r'\s*\(',body,re.I))):
            start=match.end();i=start;depth=1;quote=None;commas=[]
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
            if depth:raise RuntimeError('Unbalanced trusted scalar expression')
            expression=body[start:i-1]
            if function=='NULLIF':
                if len(commas)!=1 or body[commas[0]+1:i-1].strip()!="''":continue
                expression=body[start:commas[0]]
            fields=re.findall(r'source_row\.([a-z_]+\d+)',expression)
            if not fields or any(f.startswith('text_column_') for f in fields):continue
            body=body[:match.start()]+'('+expression+')'+body[i:]
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


def main():
    if len(sys.argv)<2 or sys.argv[1] not in ('service','semantics'):raise SystemExit('Use service or semantics with driver arguments')
    action=sys.argv.pop(1);install()
    if action=='service':
        from bench.fast_transport_probe import install as transport
        transport()
        from bench.service_run import main as driver
    else:
        from tests.semantics.native_copy import main as driver
    driver()


if __name__=='__main__':main()
