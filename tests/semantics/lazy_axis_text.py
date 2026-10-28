"""Native text normalization at every axis, plus malformed/error fallbacks."""
import sys
from bench import lazy_axis_text_probe as candidate
from bench.common import write_report
from tests.semantics import slim_copy as driver,copy_layout,native_copy


def main():
    driver.candidate=candidate
    def recorded(path,value):write_report(str(path).replace('slim-copy','lazy-axis'),value)
    driver.write_report=recorded;copy_layout.write_report=recorded
    if len(sys.argv)>1 and sys.argv[1]=='axes':
        sys.argv.pop(1);cases=[]
        for position in range(1,15):
            for value in [' x ','y',' z ',None,'','\tX\n',{'k':'x'},['y']]:
                edits=[('integer_column_'+str(position+1),str(position)),('text_column_'+str(position+3),value),('numeric_column_'+str(2*position+1),'0.001'),('numeric_column_'+str(2*position+2),'0')]
                cases.append({'name':'axis-text-selection','edits':edits,'settings':len(cases)%4})
        native_copy.cases=lambda seed,count:iter(cases)
        sys.argv=[sys.argv[0],'--count','0','--transport','server','--output','reports/semantics-lazy-axis-text-112.json']
    driver.main()


if __name__=='__main__':main()
