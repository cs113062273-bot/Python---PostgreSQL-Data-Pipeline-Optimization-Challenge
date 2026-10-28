"""Original-vs-native differential for the equivalent Gregorian proof worker."""
from bench import calendar_proof_probe as calendar
from bench.common import write_report
from tests.semantics import native_copy as driver


def main():
    frozen=calendar.source_identity();calendar.install()
    def recorded(path,value):
        assert calendar.source_identity()==frozen
        write_report(path,{**value,'calendar_hook_sha256':frozen})
    driver.write_report=recorded
    driver.main()


if __name__=='__main__':main()
