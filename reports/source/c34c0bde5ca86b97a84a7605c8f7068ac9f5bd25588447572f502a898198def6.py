"""Full native fault/retry suite for scaled data and both metadata transports."""
import hashlib
import types
from bench import scaled_int_probe as scaled,script_transport_probe as script,fast_transport_probe as fast
from bench.common import write_report
from tests.recovery import refined_copy_atomic as driver


def identity():return hashlib.sha256((scaled.source_identity()+script.source_identity()).encode()).hexdigest()


def main():
    driver.refined=types.SimpleNamespace(source_identity=identity,install=lambda:script.install('scaled'))
    driver.transport=types.SimpleNamespace(install=lambda:None,close_sessions=fast.close_sessions)
    def recorded(ignored,value):
        value={**value,'kind':'scaled_int_and_native_metadata_stdin_atomic_faults','candidate_hook_sha256':value['refined_hook_sha256'],'scaled_data_hook_sha256':scaled.source_identity(),'metadata_stdin_hook_sha256':script.source_identity()}
        del value['refined_hook_sha256']
        write_report('reports/recovery-scaled-script-copy-atomic.json',value)
    driver.write_report=recorded
    driver.main()


if __name__=='__main__':main()
