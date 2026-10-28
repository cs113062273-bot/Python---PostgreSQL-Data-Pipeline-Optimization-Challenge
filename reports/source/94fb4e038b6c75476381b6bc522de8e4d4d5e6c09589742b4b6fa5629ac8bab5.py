"""Actual native data/DDL/file/backend faults and exact versioned retry."""
from bench import native_compact_probe as candidate
from bench.common import write_report
from tests.recovery import slim_copy_atomic as driver


def main():
    driver.candidate=candidate
    driver.write_report=lambda path,value:write_report(str(path).replace('slim-copy','native-compact'),{**value,'trusted_native_module':True,'official_eligibility_unproven':True})
    driver.main()


if __name__=='__main__':main()
