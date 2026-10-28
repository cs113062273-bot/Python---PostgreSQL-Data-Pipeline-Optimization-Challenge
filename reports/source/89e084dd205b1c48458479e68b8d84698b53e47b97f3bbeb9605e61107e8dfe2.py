"""Actual native faults and complete exact retry on the inline composition."""
from bench import slim_inline_probe as candidate
from bench.common import write_report
from tests.recovery import slim_copy_atomic as driver


def main():
    driver.candidate=candidate
    driver.write_report=lambda path,value:write_report(str(path).replace('slim-copy','slim-inline'),value)
    driver.main()


if __name__=='__main__':main()
