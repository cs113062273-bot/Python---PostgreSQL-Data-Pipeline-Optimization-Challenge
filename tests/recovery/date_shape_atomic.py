"""Real failures, complete rollback and native retry for date shape worker."""
import types
from bench import date_shape_probe as shape,fast_transport_probe as fast
from bench.common import write_report
from tests.recovery import refined_copy_atomic as driver


def main():
    frozen=shape.source_identity()
    driver.refined=types.SimpleNamespace(source_identity=shape.source_identity,install=shape.install)
    driver.transport=types.SimpleNamespace(install=lambda:None,close_sessions=fast.close_sessions)
    def recorded(ignored,value):
        assert shape.source_identity()==frozen
        value={**value,'kind':'date_shape_native_server_copy_atomic_faults','candidate_hook_sha256':frozen}
        del value['refined_hook_sha256']
        write_report('reports/recovery-date-shape-copy-atomic.json',value)
    driver.write_report=recorded;driver.main()


if __name__=='__main__':main()
