"""Reuse the full six-transform rollback harness for deferred axis text."""
import copy
import sys
from bench import null_cache_component as driver,lazy_axis_text_probe as lazy,slim_inline_probe as base
from bench.common import write_report


def main():
    driver.parent=base;driver.null_cache=lazy
    def recorded(path,value):
        result=copy.deepcopy(value);result['kind']='rollback_only_deferred_axis_text_normalization'
        names={'original':'normalized_cache','nulls':'deferred_axis_text'}
        for row in result['runs']:row['mode']=names[row['mode']]
        if 'summary' in result:result['summary']={names[k]:v for k,v in result['summary'].items()}
        result['scope']='Same-session generated 1M corpus, same six native COPY transforms and full typed fingerprints. Cached axis strings are already normalized on this corpus; general text/output/error equality requires the separate native semantic tests. No full-pipeline score; all private work rolls back.'
        write_report(path,result)
    driver.write_report=recorded
    if '--output' not in sys.argv:sys.argv.extend(['--output','reports/lazy-axis-components-1m.json'])
    driver.main()


if __name__=='__main__':main()
