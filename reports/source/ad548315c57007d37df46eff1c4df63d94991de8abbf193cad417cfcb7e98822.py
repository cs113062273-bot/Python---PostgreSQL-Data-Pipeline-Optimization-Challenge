"""Typed main/details failures, real default exhaustion, rollback and retry."""
import types
from bench import native_main_probe as candidate,fast_transport_probe as fast
from bench.common import write_report
from tests.recovery import refined_copy_atomic as driver


def main():
    frozen=candidate.source_identity()
    driver.refined=types.SimpleNamespace(source_identity=candidate.source_identity,install=candidate.install)
    driver.transport=types.SimpleNamespace(install=lambda:None,close_sessions=fast.close_sessions)
    def marker(point,text):
        if point=='after_main_receipt':prefix="INSERT INTO pg_temp.pipeline_copy_receipts SELECT 'table1',affected"
        elif point in ('before_main','after_main'):prefix='UPDATE pg_temp.pipeline_main_state SET affected=pgdm_pipeline_work.main_load('
        elif point=='after_details_receipts':prefix=' CROSS JOIN LATERAL (SELECT counts.table_name) names;'
        else:prefix='UPDATE pg_temp.pipeline_details_state SET counts=pgdm_pipeline_work.details_load('
        lines=[line for line in text.splitlines() if line.startswith(prefix)]
        assert len(lines)==1,(point,lines);return lines[0]
    def recorded(ignored,value):
        assert candidate.source_identity()==frozen
        if value['all_passed']:
            diag=value['retry_local_backend_diagnostics']
            assert diag['actual_binary_files']==2 and diag['typed_main_affected'] not in (None,'fallback')
            assert diag['shared_fk_checked'] not in (None,'fallback')
        value={**value,'kind':'typed_main_and_details_atomic_faults','candidate_hook_sha256':frozen,'official_eligibility_unproven':True}
        value.pop('refined_hook_sha256',None);write_report('reports/recovery-native-main-atomic.json',value)
    driver.write_report=recorded
    points=['after_projection','before_main','after_main','after_main_receipt','before_details','after_details','after_details_receipts','before_fk_restore','after_fk_restore','before_commit']
    cases=[(point,kind) for point in points for kind in ('server_exception','backend_termination')]
    cases += [('before_details','bad_frame'),('before_main','identity_exhaustion'),('copy_from_table3','missing_file'),('copy_to_table3','bad_binary')]
    driver.main(cases_override=cases,marker_override=marker)


if __name__=='__main__':main()
