"""Actual native faults and exact retry of slim binary COPY."""
import types
from bench import slim_copy_probe as candidate,fast_transport_probe as fast
from bench.common import write_report
from tests.recovery import refined_copy_atomic as driver


def main():
    frozen=candidate.source_identity()
    driver.refined=types.SimpleNamespace(source_identity=candidate.source_identity,install=candidate.install)
    driver.transport=types.SimpleNamespace(install=lambda:None,close_sessions=fast.close_sessions)
    def recorded(ignored,value):
        assert candidate.source_identity()==frozen
        value={**value,'kind':'slim_native_server_copy_atomic_faults','candidate_hook_sha256':frozen}
        del value['refined_hook_sha256']
        write_report('reports/recovery-slim-copy-atomic.json',value)
    driver.write_report=recorded;driver.main()


if __name__=='__main__':main()
