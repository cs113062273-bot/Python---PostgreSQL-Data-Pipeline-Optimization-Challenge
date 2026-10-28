"""Disposable cache empty-string normalization; original fallback untouched.

All numeric/id/date consumers already map empty TEXT to SQL NULL. Empty text
dimension keys are filtered; empty axis types cannot match X/Y/Z. The proof
bundles coalesce both representations identically. No source row is changed.
"""
import hashlib
from pathlib import Path
import re
from bench import parent_hint_probe as parent
from pipeline.recipes import EXTRACT,destinations


def source_identity():
    return hashlib.sha256((parent.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def projection(body):
    for key in sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])}):
        expr="p.payload->>'"+key+"'"
        assert body.count(expr)==1
        body=body.replace(expr,"NULLIF("+expr+",'')",1)
    return body


def statement(body):
    # These numeric/date references can be empty only before normalization.
    # Proof excludes whitespace; text dimension/type filtering stays original.
    body=re.sub(r"NULLIF\((source_row\.(?:(?:integer|numeric)_column_[0-9]+|id_column_1|datetime_column_1)),\s*''\)",r'\1',body)
    for key in ('weight_text','gap_text'):
        body=body.replace("NULLIF(raw_axis."+key+",'')",'raw_axis.'+key)
    return body


def install():
    parent.install()
    import pipeline.parallel_normalization as parallel
    import pipeline.guarded_normalization as guarded
    original_projection=parallel.prepare_input_sql;original_statement=guarded.fast_statement
    parallel.prepare_input_sql=lambda scopes,logged=False:'-- lab empty-text cache normalization source_sha256='+source_identity()+'\n'+projection(original_projection(scopes,logged=logged))
    def transformed(original,scopes,mode=guarded.MODE):
        body=original_statement(original,scopes,mode)
        return statement(body) if mode=='guarded_copy' else body
    guarded.fast_statement=transformed


def main():
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
