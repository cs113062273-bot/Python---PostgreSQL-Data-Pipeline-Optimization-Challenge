"""Actual pre-commit failures and exact retry for native per-value reuse."""
import types
from bench import memo_axes_probe as memo,fast_transport_probe as fast
from bench.common import write_report
from tests.recovery import refined_copy_atomic as driver


def main():
    frozen=memo.source_identity()
    driver.refined=types.SimpleNamespace(source_identity=memo.source_identity,install=memo.install)
    driver.transport=types.SimpleNamespace(install=lambda:None,close_sessions=fast.close_sessions)
    def recorded(ignored,value):
        assert memo.source_identity()==frozen
        value={**value,'kind':'memo_native_server_copy_atomic_faults','candidate_hook_sha256':frozen}
        del value['refined_hook_sha256']
        write_report('reports/recovery-memo-copy-atomic.json',value)
    driver.write_report=recorded
    driver.main()


if __name__=='__main__':main()
