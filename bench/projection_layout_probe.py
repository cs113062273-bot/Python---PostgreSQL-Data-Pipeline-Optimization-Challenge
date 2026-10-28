"""Reorder only disposable TEXT-cache columns; preserve every named value."""
import hashlib
from pathlib import Path
from bench import parent_hint_probe as parent
from pipeline.recipes import EXTRACT, destinations


def source_identity():
    return hashlib.sha256((parent.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def reorder(body, mode='proof_first'):
    if mode=='original':return body
    if mode not in ('proof_first','text_first'):raise ValueError(mode)
    marker='SELECT s.raw_id,n.*,'
    assert body.count(marker)==1
    start=body.index(marker);end=body.index(' AS pipeline_domain_safe',start)
    proof=body[start+len(marker):end]
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    if mode=='text_first':fields.sort(key=lambda name:(not name.startswith('text_column_'),name))
    columns=','.join('n.'+name for name in fields)
    return body[:start]+'SELECT s.raw_id,'+proof+' AS pipeline_domain_safe,'+columns+body[end+len(' AS pipeline_domain_safe'):]


def install():
    parent.install()
    import pipeline.parallel_normalization as parallel
    projection=parallel.prepare_input_sql
    parallel.prepare_input_sql=lambda scopes,logged=False:'-- lab proof-first cache layout source_sha256='+source_identity()+'\n'+reorder(projection(scopes,logged=logged))


def main():
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
