"""Actual native service COPY/output/error/layout checks of shared details."""
from bench import native_details_probe as candidate
from bench.common import write_report
from tests.semantics import lazy_axis_text as driver


def main():
    driver.candidate=candidate
    driver.write_report=lambda path,value:write_report(str(path).replace('lazy-axis','native-details'),{**value,'shared_native_details':True,'official_eligibility_unproven':True})
    driver.main()


if __name__=='__main__':main()
