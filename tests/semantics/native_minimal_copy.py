"""Actual COPY/error/layout/fallback parity with durable empty-target reset."""
from bench import native_minimal_probe as candidate
from bench.common import write_report
from tests.semantics import lazy_axis_text as driver


def main():
    driver.candidate=candidate
    driver.write_report=lambda path,value:write_report(str(path).replace('lazy-axis','native-minimal'),{**value,'requires_server_configuration_for_wal_skip':True,'official_eligibility_unproven':True})
    driver.main()


if __name__=='__main__':main()
