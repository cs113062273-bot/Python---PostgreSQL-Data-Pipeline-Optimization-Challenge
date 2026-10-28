"""Native whole-batch fallback around quantization and trim-identity bounds."""
import sys
from bench import t1_inline_probe as inline
from bench.common import write_report
from tests.semantics import native_copy as driver


def main():
    frozen=inline.source_identity();inline.install()
    cases=[]
    for field in ['numeric_column_1','numeric_column_2']:
        for value in [None,'','0','-0','0.000','-0.000','0.001','-0.001','0.0001','-0.0001','999999.999','-999999.999','1000000','2147483.648','NaN','Infinity',' 0.001 ','1e-4',{},[]]:
            cases.append({'name':'quantization-bound','edits':[(field,value)],'settings':len(cases)%4})
    for field in ['integer_column_1','integer_column_2','numeric_column_3','id_column_1','datetime_column_1']:
        for value in [' ', '\t2\t',' 2 ',None,'','bad']:
            cases.append({'name':'trim-identity-fallback','edits':[(field,value)],'settings':len(cases)%4})
    for edits in [[('numeric_column_1','0.0001'),('numeric_column_2','bad')],[('integer_column_1',' 2 '),('id_column_1','bad')],[('datetime_column_1','bad'),('numeric_column_29','bad')]]:
        for setting in range(4):cases.append({'name':'multiple-errors','edits':edits,'settings':setting})
    driver.cases=lambda seed,count:iter(cases)
    def recorded(path,value):
        assert inline.source_identity()==frozen
        write_report(path,{**value,'inline_hook_sha256':frozen,'purpose':'Native whole-batch output/counter/error equality, including precision outside proof, overflow, signed zero, padded casts and competing errors; fixtures only.'})
    driver.write_report=recorded
    sys.argv=[sys.argv[0],'--count','0','--transport','server','--output','reports/semantics-t1-inline-boundaries-82.json']
    driver.main()


if __name__=='__main__':main()
