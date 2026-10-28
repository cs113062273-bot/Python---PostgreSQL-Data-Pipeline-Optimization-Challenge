"""Actual data/metadata failures and full retry for combined native framing."""
import types
from bench import combined_metadata_probe as combined,fast_transport_probe as fast
from bench.common import write_report
from tests.recovery import refined_copy_atomic as driver


def main():
    frozen=combined.source_identity()
    driver.refined=types.SimpleNamespace(source_identity=combined.source_identity,install=combined.install)
    driver.transport=types.SimpleNamespace(install=lambda:None,close_sessions=fast.close_sessions)
    def recorded(ignored,value):
        assert combined.source_identity()==frozen
        value={**value,'kind':'combined_shape_native_server_copy_atomic_faults','candidate_hook_sha256':frozen}
        del value['refined_hook_sha256']
        write_report('reports/recovery-combined-shape-copy-atomic.json',value)
    driver.write_report=recorded;driver.main()


if __name__=='__main__':main()
