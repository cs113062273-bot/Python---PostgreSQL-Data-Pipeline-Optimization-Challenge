"""Independent live EXCEPT ALL/source/catalog/version audit of a native pair."""
import argparse
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
from bench import native_c_lab as lab,native_compact_probe as candidate
from bench.common import ROOT,write_report,require_lab
from bench.audit_ranked_result import exact_via_private_temp
from pipeline.checkpoints import source_digest
from pipeline.recipes import ORDER
from tests.equivalence.oracle import protected_schema_signature,sequence_signature,assert_relationships


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--cohort',required=True);p.add_argument('--output',required=True);args=p.parse_args()
    path=(ROOT/args.cohort).resolve();output=(ROOT/args.output).resolve()
    assert path.is_relative_to(ROOT/'reports') and output.is_relative_to(ROOT/'reports') and not output.exists()
    cohort=json.loads(path.read_text(encoding='utf8'));assert cohort['kind']=='fresh_native_compact_complete_processing_pair'
    report={'kind':'independent_live_native_compact_pair_audit','cohort':str(path.relative_to(ROOT)),
            'observed_at_utc':datetime.now(timezone.utc).isoformat(),'passed':False,
            'auditor_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'limits':'Independent verification after processing timers, not a new timing measurement, repeatability result or official eligibility determination.'}
    write_report(output,report)
    first,second=cohort['runs'];assert first['mode']=='official' and second['mode']=='native_compact'
    reduction=100*(1-second['wall_seconds']/first['wall_seconds'])
    assert abs(reduction-cohort['summary']['reduction_percent'])<1e-10
    assert candidate.source_identity()==cohort['implementation_identity']['hook_sha256']
    children={r['database']:json.loads((ROOT/r['report']).read_text(encoding='utf8')) for r in cohort['runs']}
    with lab.connect(first['database'],autocommit=True) as reference,lab.connect(second['database'],autocommit=True) as current:
        source_states=[];catalogs=[];sequences=[];live=[]
        for c in (reference,current):
            require_lab(c);db=c.info.dbname
            assert c.execute('SELECT count(*) FROM public.raw_data').fetchone()[0]==cohort['rows']
            scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
            digest=source_digest(c,scopes);assert digest==cohort['identical_input_digest'];source_states.append(digest)
            catalogs.append(protected_schema_signature(c));sequences.append(sequence_signature(c))
            assert c.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()==(None,)
            assert not c.execute('SELECT 1 FROM pg_extension WHERE extname=%s',(candidate.package()['extension'],)).fetchone()
            assert_relationships(c)
            for table in ORDER:
                assert c.execute('SELECT count(*) FROM public.'+table).fetchone()[0]==c.execute("SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s",(table,)).fetchone()[0]
            assert all(c.execute('SHOW '+setting).fetchone()[0]=='on' for setting in ('fsync','synchronous_commit','full_page_writes'))
            assert c.execute("SELECT bool_and(relpersistence='p') FROM pg_class WHERE relnamespace='public'::regnamespace AND relkind='r'").fetchone()==(True,)
            with lab.connect('postgres_data_manager',autocommit=True) as control:
                versions=control.execute("SELECT sql_recipe FROM app_control.table_versions WHERE database_name=%s AND version_code<>'0000'",(db,)).fetchall()
                assert len(versions)==6
                if db==second['database']:
                    architecture=children[db]['execution_architecture'];assert architecture['safe_domain'] is True
                    batch=architecture['batch_sql_sha256']
                    batch_path=ROOT/'reports/sql'/(batch+'.sql')
                    assert hashlib.sha256(batch_path.read_text(encoding='utf8').encode()).hexdigest()==batch
                    for recipe, in versions:
                        assert 'compiler_sha256='+architecture['compiler_sha256'] in recipe
                        assert 'batch_sql_sha256='+batch in recipe and 'safe_domain=true' in recipe
            live.append({'database':db,'source_digest':digest,'counters_relationships_versions_durability':True,'versions':len(versions),'transient_native_objects_absent':True})
        assert source_states[0]==source_states[1] and catalogs[0]==catalogs[1] and sequences[0]==sequences[1]
        print('Both live source/catalog/sequence/counter/version checks PASS; full EXCEPT ALL pending',flush=True)
        report['live_checks']=live;write_report(output,report)
        report['exact_comparison']=exact_via_private_temp(current,reference,comparison='except_all')
        assert len(report['exact_comparison'])==6
    report.update({'passed':True,'recorded_reduction_percent':reduction,'arithmetic_verified':True,'six_bidirectional_except_all':True,'same_protected_catalog_sequence_options':True})
    write_report(output,report);print('Native compact pair independent audit: six full EXCEPT ALL/source/catalog/versions PASS',flush=True)


if __name__=='__main__':main()
