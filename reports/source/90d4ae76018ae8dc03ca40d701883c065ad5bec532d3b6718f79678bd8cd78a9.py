"""Isolated normalized-text/negative-proof/ephemeral-cache native worker."""
import hashlib
from pathlib import Path
import sys
from bench import text_stage_probe as text
from bench.proof_pattern_probe import negative_condition
from pipeline.safe_domain import condition_sql


def source_identity():
    digest=hashlib.sha256()
    for name in ('refined_copy_probe.py','text_stage_probe.py','proof_pattern_probe.py','fast_transport_probe.py','server_copy_probe.py'):
        digest.update(name.encode());digest.update(Path(__file__).with_name(name).read_text(encoding='utf-8').encode())
    return digest.hexdigest()


def install(*,combined=True):
    import pipeline.parallel_normalization as projection
    import pipeline.guarded_normalization as guarded
    def normalized(scopes,*,logged=False):
        body=text.projection(scopes,logged=logged)
        if combined:
            assert body.count(condition_sql())==1
            body=body.replace(condition_sql(),negative_condition(),1)
            if logged:body=body.replace('CREATE TABLE challenge_lab.pipeline_normalized_input AS','CREATE UNLOGGED TABLE challenge_lab.pipeline_normalized_input AS',1)
        return '-- lab refined COPY source_sha256='+source_identity()+'\n'+body
    projection.prepare_input_sql=normalized
    original=guarded.fast_statement
    def transformed(statement,scopes,mode=guarded.MODE):
        body=original(statement,scopes,mode)
        return text.statement(body) if mode=='guarded_copy' else body
    guarded.fast_statement=transformed
    validate=guarded.validation_destinations
    if combined:
        guarded.validation_destinations=lambda destinations:validate(destinations)+[{'statements':[],'function_references':[('pg_catalog','textregexne')]}]


def main():
    if len(sys.argv)<2 or sys.argv[1] not in ('service','semantics','text_semantics'):
        raise SystemExit('Use service, semantics or text_semantics with driver arguments')
    action=sys.argv.pop(1);install(combined=action!='text_semantics')
    if action=='service':
        from bench.fast_transport_probe import install as transport
        transport()
        from bench.service_run import main as driver
    else:
        from tests.semantics.native_copy import main as driver
    driver()


if __name__=='__main__':main()
