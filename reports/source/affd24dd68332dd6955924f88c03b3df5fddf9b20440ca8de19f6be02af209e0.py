"""Real file/batch failures and exact retry with one-batch temporary hints."""
import types
from bench import temporary_hint_probe as hint,fast_transport_probe as fast
from bench.common import write_report
from tests.recovery import refined_copy_atomic as driver


def main():
    frozen=hint.source_identity()
    driver.refined=types.SimpleNamespace(source_identity=hint.source_identity,install=hint.install)
    driver.transport=types.SimpleNamespace(install=lambda:None,close_sessions=fast.close_sessions)
    def recorded(ignored,value):
        assert hint.source_identity()==frozen
        value={**value,'kind':'temporary_hint_native_server_copy_atomic_faults','candidate_hook_sha256':frozen}
        del value['refined_hook_sha256']
        write_report('reports/recovery-temporary-hint-copy-atomic.json',value)
    driver.write_report=recorded;driver.main()


if __name__=='__main__':main()
