"""Whole-service native error/output/layout replay for typed table1 COPY."""
from bench import native_main_probe as candidate
from bench.common import write_report
from tests.semantics import native_fk_copy as driver


def main():
    driver.candidate=candidate
    driver.write_report=lambda path,value:write_report(str(path).replace('native-fk','native-main'),value)
    driver.main()


if __name__=='__main__':main()
