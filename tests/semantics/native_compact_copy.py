"""Actual native COPY/counter/error checks of the transient trusted module."""
from bench import native_compact_probe as candidate
from bench.common import write_report
from tests.semantics import lazy_axis_text as driver


def main():
    driver.candidate=candidate
    driver.write_report=lambda path,value:write_report(str(path).replace('lazy-axis','native-compact'),{**value,'trusted_native_module':True,'official_eligibility_unproven':True})
    driver.main()


if __name__=='__main__':main()
