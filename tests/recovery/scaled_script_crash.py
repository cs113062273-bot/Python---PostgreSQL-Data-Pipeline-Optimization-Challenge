"""Private server crash and cold-worker retry of the scaled native pipeline."""
import sys
from bench import scaled_int_probe as scaled,script_transport_probe as script
from bench.common import write_report
from tests.recovery import native_copy_crash as driver


def main():
    scaled_sha=scaled.source_identity();script_sha=script.source_identity()
    script.install('scaled')
    def recorded(ignored,value):
        assert scaled.source_identity()==scaled_sha and script.source_identity()==script_sha
        write_report('reports/recovery-scaled-script-server-crash.json',{**value,'scaled_data_hook_sha256':scaled_sha,'metadata_stdin_hook_sha256':script_sha})
    driver.write_report=recorded
    if '--fresh-service-retry' not in sys.argv:sys.argv.append('--fresh-service-retry')
    driver.main()


if __name__=='__main__':main()
