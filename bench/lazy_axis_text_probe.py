"""Normalize axis text only after its position is selected in proven-safe SQL.

All integer/numeric/date fields still enter the unchanged global proof, even
unused positions. Only total trim/case operations on text_column_4..17 move
from the 14-wide source projection into lazy CASE branches of the native row
barrier. First three dimension text fields stay normalized in the cache.
"""
import hashlib
from pathlib import Path
import re
from bench import slim_inline_probe as base


def source_identity():
    return hashlib.sha256((base.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def projection(body):
    for index in range(4,18):
        raw="NULLIF(p.payload->>'text_column_"+str(index)+"','')"
        normalized='pg_catalog.upper(pg_catalog.btrim('+raw+'))'
        assert body.count(normalized)==1
        body=body.replace(normalized,raw,1)
    return body


def statement(body):
    if not body.startswith('INSERT INTO public.table2 ('):return body
    for index in range(4,18):
        pattern=r'(\bTHEN\s+)(source_row\.text_column_'+str(index)+r')\b'
        body,count=re.subn(pattern,lambda m:m[1]+'pg_catalog.upper(pg_catalog.btrim('+m[2]+'))',body)
        assert count==1,(index,count)
    return body


def install():
    base.install()
    import pipeline.parallel_normalization as parallel
    import pipeline.guarded_normalization as guarded
    original_projection=parallel.prepare_input_sql;original_statement=guarded.fast_statement
    parallel.prepare_input_sql=lambda scopes,logged=False:'-- lab deferred axis text normalization source_sha256='+source_identity()+'\n'+projection(original_projection(scopes,logged=logged))
    def transformed(original,scopes,mode=guarded.MODE):
        body=original_statement(original,scopes,mode)
        return statement(body) if mode=='guarded_copy' else body
    guarded.fast_statement=transformed


def main():
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
