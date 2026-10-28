"""Observe actual nested native pipeline plans; never a performance score.

Private superuser-only auto_explain is loaded into the one data session.
No global configuration changes. The original complete service and its real
commits/versions run; profiler prefix is bound into the archived actual SQL.
Instrumentation overhead makes all times here diagnostic only.
"""
import hashlib
import json
from pathlib import Path
import re
from bench import combined_metadata_probe as candidate
from bench.common import ROOT,write_report
from pipeline.native_copy import MARKER


def source_identity():
    return hashlib.sha256((candidate.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def extract(stderr):
    result=[];decoder=json.JSONDecoder()
    for match in re.finditer(r'duration:\s+([0-9.]+) ms\s+plan:\s*',stderr):
        remainder=stderr[match.end():]
        try:value,used=decoder.raw_decode(remainder)
        except json.JSONDecodeError:continue
        entries=value if isinstance(value,list) else [value]
        for entry in entries:
            if isinstance(entry,dict) and 'Plan' in entry:result.append({'instrumented_duration_ms':float(match[1]),**entry})
    return result


def nodes(plan):
    yield plan
    for child in plan.get('Plans',[]):yield from nodes(child)


def main():
    candidate.install()
    import pipeline.local_diagnostics as diagnostic
    from bench import service_run
    frozen=source_identity();original=diagnostic.run_psql;captured=[]
    prefix="""LOAD 'auto_explain';
SET auto_explain.log_min_duration=0;
SET auto_explain.log_analyze=on;
SET auto_explain.log_timing=off;
SET auto_explain.log_buffers=on;
SET auto_explain.log_wal=on;
SET auto_explain.log_settings=on;
SET auto_explain.log_nested_statements=on;
SET auto_explain.log_format='json';
SET auto_explain.log_level='notice';
SET auto_explain.log_parameter_max_length=0;
SET client_min_messages=notice;
"""
    def profiled(args,text,*,cwd,timeout,cpu=None):
        if isinstance(text,str) and MARKER in text:
            assert source_identity()==frozen
            text='-- lab nested-plan diagnostic source_sha256='+frozen+'\n'+prefix+text
            result=original(args,text,cwd=cwd,timeout=timeout,cpu=cpu)
            database=args[args.index('-d')+1];assert database.startswith('lab_')
            path=ROOT/'reports/profiles'/('nested-'+database+'.json')
            plans=extract(result.stderr)
            assert plans,'Native nested plans were not returned'
            selected=[]
            for plan in plans:
                query=plan.get('Query Text','')
                if any(marker in query for marker in ('CREATE UNLOGGED TABLE','COPY (SELECT','SELECT m.id_column_1','SELECT main_row.id_column_1')):
                    selected.append({'query':query,'instrumented_duration_ms':plan['instrumented_duration_ms'],'workers_planned':max((n.get('Workers Planned',0) for n in nodes(plan['Plan'])),default=0),'workers_launched':max((n.get('Workers Launched',0) for n in nodes(plan['Plan'])),default=0),'plan_node_types':[n['Node Type'] for n in nodes(plan['Plan'])]})
            write_report(path,{'kind':'actual_native_service_nested_auto_explain','source_sha256':frozen,'official_score':False,'profile_instrumentation_changes_timing':True,'plans':plans,'selected_summary':selected,'stderr':result.stderr,'scope':'Actual data-session SPI/DO/COPY query plans, all original integrity/version work. Superuser-only private profiling, not a required optimization or reproducible score. No global setting changes.'})
            captured.append(str(path.relative_to(ROOT)))
            return result
        return original(args,text,cwd=cwd,timeout=timeout,cpu=cpu)
    diagnostic.run_psql=profiled
    def recorded(path,value):
        assert source_identity()==frozen and captured
        write_report(path,{**value,'performance_score_eligible':False,'nested_plan_profile_reports':captured,'profile_source_sha256':frozen,'profile_instrumentation_changes_timing':True})
    service_run.write_report=recorded
    service_run.main()


if __name__=='__main__':main()
