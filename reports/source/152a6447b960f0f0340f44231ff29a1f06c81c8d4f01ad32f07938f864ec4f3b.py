"""Padded/empty/NULL scalar inputs against the unchanged native official batch."""
import sys
from bench import scalar_copy_probe as scalar
from pipeline.recipes import EXTRACT,destinations
from tests.semantics import native_copy as driver


def main():
    identity=scalar.source_identity();scalar.install()
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    trials=[{'name':'unchanged','edits':[]}]
    for field in fields:
        if field=='datetime_column_1':units=[' 2000-02-29T12:34:56+00 ',' 1900-02-29T12:34:56+00 ',' ',None]
        elif field.startswith('text_column_'):units=[' x ',' aB ']
        else:units=[' 2 ',' ',' +9999 ',None,' | ']
        for unit in units:trials.append({'name':'scalar-padding-null-boundary','edits':[(field,unit)],'settings':len(trials)%4})
    driver.cases=lambda seed,count:iter(trials)
    original_write=driver.write_report
    def checked_write(path,value):
        assert scalar.source_identity()==identity,'Scalar source changed during differential run'
        original_write(path,{**value,'scalar_hook_sha256':identity,'purpose':'Per-field padded/empty/NULL/invalid delimiters, unchanged native output/error/counter checks; four planner settings. Test fixtures only.'})
    driver.write_report=checked_write
    sys.argv=[sys.argv[0],'--count','0','--transport','server','--output','reports/semantics-scalar-copy-padding.json']
    driver.main()


if __name__=='__main__':main()
