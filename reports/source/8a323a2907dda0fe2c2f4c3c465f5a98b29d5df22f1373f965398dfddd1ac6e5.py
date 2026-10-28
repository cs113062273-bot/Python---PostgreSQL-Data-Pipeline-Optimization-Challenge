"""Actual pre-commit failures and exact retry for inline quantization worker."""
import types
from bench import t1_inline_probe as inline,fast_transport_probe as fast
from bench.common import write_report
from tests.recovery import refined_copy_atomic as driver


def main():
    frozen=inline.source_identity()
    driver.refined=types.SimpleNamespace(source_identity=inline.source_identity,install=inline.install)
    driver.transport=types.SimpleNamespace(install=lambda:None,close_sessions=fast.close_sessions)
    def recorded(ignored,value):
        assert inline.source_identity()==frozen
        value={**value,'kind':'inline_native_server_copy_atomic_faults','candidate_hook_sha256':frozen}
        del value['refined_hook_sha256']
        write_report('reports/recovery-t1-inline-copy-atomic.json',value)
    driver.write_report=recorded
    driver.main()


if __name__=='__main__':main()
