"""Apply the complete native fault/retry suite to scalar normalization."""
from bench import scalar_copy_probe as scalar
from bench.common import write_report
from tests.recovery import refined_copy_atomic as driver


def main():
    driver.refined=scalar
    def recorded(ignored,value):
        value={**value,'kind':'scalar_native_server_copy_atomic_faults','scalar_hook_sha256':value['refined_hook_sha256']}
        del value['refined_hook_sha256']
        write_report('reports/recovery-scalar-copy-atomic.json',value)
    driver.write_report=recorded
    driver.main()


if __name__=='__main__':main()
