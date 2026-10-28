"""Actual pre-commit faults after native durable empty-target TRUNCATE."""
from bench import native_minimal_probe as candidate
from bench.common import write_report
from tests.recovery import slim_copy_atomic as driver


def main():
    driver.candidate=candidate
    driver.write_report=lambda path,value:write_report(str(path).replace('slim-copy','native-minimal'),{**value,'requires_server_configuration_for_wal_skip':True,'official_eligibility_unproven':True})
    driver.main()


if __name__=='__main__':main()
