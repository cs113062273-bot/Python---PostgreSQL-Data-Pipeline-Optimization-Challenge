"""Actual native failures for the calendar + table2/table5 parsing worker."""
import types
from bench import calendar_proof_probe as calendar,fast_transport_probe as fast
from bench.common import write_report
from tests.recovery import refined_copy_atomic as driver


def main():
    frozen=calendar.source_identity()
    driver.refined=types.SimpleNamespace(source_identity=calendar.source_identity,install=calendar.install)
    driver.transport=types.SimpleNamespace(install=lambda:None,close_sessions=fast.close_sessions)
    def recorded(ignored,value):
        assert calendar.source_identity()==frozen
        value={**value,'kind':'calendar_table5_native_server_copy_atomic_faults','candidate_hook_sha256':frozen}
        del value['refined_hook_sha256']
        write_report('reports/recovery-calendar-table5-copy-atomic.json',value)
    driver.write_report=recorded
    driver.main()


if __name__=='__main__':main()
