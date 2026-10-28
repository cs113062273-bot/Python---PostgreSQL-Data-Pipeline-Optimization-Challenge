"""Native scaled-integer normalization differential with frozen source identity."""
from bench import scaled_int_probe as scaled
from bench.common import write_report
from tests.semantics import native_copy as driver


def main():
    identity=scaled.source_identity();scaled.install()
    def recorded(path,value):
        assert scaled.source_identity()==identity,'Scaled source changed during native differential run'
        write_report(path,{**value,'scaled_hook_sha256':identity})
    driver.write_report=recorded
    driver.main()


if __name__=='__main__':main()
