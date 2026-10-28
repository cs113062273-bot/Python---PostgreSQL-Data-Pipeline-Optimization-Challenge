"""Actual service output/error/layout replay with shared native FK validation."""
from bench import native_fk_probe as candidate
from bench.common import write_report
from tests.semantics import lazy_axis_text as driver


def main():
    driver.candidate=candidate
    driver.write_report=lambda path,value:write_report(str(path).replace('lazy-axis','native-fk'),{**value,'native_shared_fk_validation':True,'official_eligibility_unproven':True})
    driver.main()


if __name__=='__main__':main()
