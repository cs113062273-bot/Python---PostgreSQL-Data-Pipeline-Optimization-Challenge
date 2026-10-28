"""Shared native write/counter boundaries, injected frame error and retry."""
import types
from bench import native_details_probe as candidate,fast_transport_probe as fast
from bench.common import write_report
from tests.recovery import refined_copy_atomic as driver


def main():
    frozen=candidate.source_identity()
    driver.refined=types.SimpleNamespace(source_identity=candidate.source_identity,install=candidate.install)
    driver.transport=types.SimpleNamespace(install=lambda:None,close_sessions=fast.close_sessions)
    def marker(point,text):
        prefix=' CROSS JOIN LATERAL (SELECT counts.table_name) names;' if point=='after_details_receipts' else 'UPDATE pg_temp.pipeline_details_state SET counts=pgdm_pipeline_work.details_load('
        lines=[line for line in text.splitlines() if line.startswith(prefix)]
        assert len(lines)==1,(point,lines)
        return lines[0]
    def recorded(ignored,value):
        assert candidate.source_identity()==frozen
        value={**value,'kind':'shared_native_details_atomic_faults','candidate_hook_sha256':frozen,'official_eligibility_unproven':True}
        del value['refined_hook_sha256']
        write_report('reports/recovery-native-details-atomic.json',value)
    driver.write_report=recorded
    points=['after_projection','before_details','after_details','after_details_receipts','before_fk_restore','after_fk_restore','before_commit']
    cases=[(point,kind) for point in points for kind in ('server_exception','backend_termination')]
    cases += [('before_details','bad_frame'),('copy_from_table1','missing_file'),('copy_to_table1','bad_binary')]
    driver.main(cases_override=cases,marker_override=marker)


if __name__=='__main__':main()
