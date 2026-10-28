"""Actual rollback and server-crash replay of the single-pass FK package."""
import sys
from bench import native_fk_current_probe as candidate
from bench.common import write_report


def main():
    mode=sys.argv.pop(1)
    if mode=='atomic':from tests.recovery import native_fk_atomic as driver
    elif mode=='crash':from tests.recovery import native_fk_crash as driver
    elif mode=='durable':from tests.recovery import native_fk_durable as driver
    else:raise ValueError(mode)
    driver.candidate=candidate
    if mode!='durable':driver.write_report=lambda path,value:write_report(str(path).replace('native-fk','native-fk-current'),value)
    driver.main()


if __name__=='__main__':main()
