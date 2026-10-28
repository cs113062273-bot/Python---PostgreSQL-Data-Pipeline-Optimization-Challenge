"""Run native semantic/boundary/layout tests on the inline-base composition."""
from bench import slim_inline_probe as candidate
from bench.common import write_report
from tests.semantics import slim_copy as driver,copy_layout


def main():
    driver.candidate=candidate
    def recorded(path,value):
        write_report(str(path).replace('slim-copy','slim-inline'),value)
    driver.write_report=recorded;copy_layout.write_report=recorded
    driver.main()


if __name__=='__main__':main()
