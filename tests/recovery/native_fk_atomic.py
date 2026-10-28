"""Shared FK completion and typed details writes roll back on actual faults."""
from bench import native_fk_probe as candidate
from bench.common import write_report
from tests.recovery import native_details_atomic as driver


def main():
    driver.candidate=candidate
    driver.write_report=lambda path,value:write_report(str(path).replace('native-details','native-fk'),value)
    driver.main()


if __name__=='__main__':main()
