"""Catch fractional-weight rounding traps and Gregorian fallback boundaries."""
import hashlib
from pathlib import Path
import sys
from bench import calendar_proof_probe as calendar
from bench.common import write_report
from tests.semantics import native_copy as driver


def main():
    frozen=calendar.source_identity();calendar.install()
    units=['0.0004','0.04','0.05','0.1','1.01','-0.0001','-0','0','+0001','+9999','10000',' 2 ',None,'','NaN','Infinity','1e-3',True,{},[]]
    trials=[{'name':'table5-rounding-boundary','edits':[('integer_column_1',value)],'settings':setting} for value in units for setting in (0,3)]
    for stamp in ('1900-02-29 00:00:00','2000-02-29 00:00:00','0000-01-01 00:00:00','2000-04-31 00:00:00'):
        trials.extend({'name':'gregorian-fallback-boundary','edits':[('datetime_column_1',stamp)],'settings':setting} for setting in (0,3))
    for edits in [[('id_column_1','bad'),('integer_column_1','0.0004'),('numeric_column_29','bad')],[('integer_column_1','bad'),('datetime_column_1','bad')]]:
        trials.extend({'name':'table5-multi-error','edits':edits,'settings':setting} for setting in range(4))
    driver.cases=lambda seed,count:iter(trials)
    def recorded(path,value):
        assert calendar.source_identity()==frozen
        write_report(path,{**value,'calendar_hook_sha256':frozen,'boundary_driver_sha256':hashlib.sha256(Path(__file__).read_text(encoding='utf8').encode()).hexdigest(),'purpose':'Native output/counter/error equality for fractional values that round to zero, conservative proof boundaries, malformed calendar input and competing errors. Fixtures only.'})
    driver.write_report=recorded
    sys.argv=[sys.argv[0],'--count','0','--transport','server','--output','reports/semantics-calendar-table5-boundaries-56.json']
    driver.main()


if __name__=='__main__':main()
