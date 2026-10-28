"""Unranked all-TEXT record extraction research; no application integration.

jsonb_to_record is STABLE and the unchanged recipe validator rejects it. This
component deliberately runs only private rollback SQL, as the earlier record
experiments did. Performance cannot establish eligibility or justify weakening
the validator. All result columns are explicitly pg_catalog.text; non-object
payloads are mapped to {} to preserve the operators' all-NULL result.
"""
import hashlib
from pathlib import Path
import sys
from bench import calendar_proof_probe as calendar,null_cache_probe as null_cache,projection_pair
from bench.common import write_report
from pipeline.recipes import EXTRACT,destinations


def source_identity():
    return hashlib.sha256((calendar.source_identity()+Path(null_cache.__file__).read_text(encoding='utf8')+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def operators(scopes,*,logged=False):
    return null_cache.projection(calendar.projection(scopes,logged=logged))


def record(scopes,*,logged=False):
    body=operators(scopes,logged=logged)
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    marker='CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW('
    assert body.count(marker)==2 # raw columns plus the numeric proof bundles
    for key in fields:
        original="p.payload->>'"+key+"'";assert body.count(original)==1
        body=body.replace(original,'text_record.'+key,1)
    definition=','.join(key+' pg_catalog.text' for key in fields)
    extraction="CROSS JOIN LATERAL pg_catalog.jsonb_to_record(CASE WHEN pg_catalog.jsonb_typeof(p.payload)='object' THEN p.payload ELSE '{}'::pg_catalog.jsonb END) text_record("+definition+')\n'
    return '-- lab unranked all-TEXT record research source_sha256='+source_identity()+'\n'+body.replace(marker,extraction+marker,1)


def differential(c):
    from psycopg.types.json import Jsonb
    values=[None,{},[],[1,2],True,False,1,'x',{'x':None},{'x':''},{'x':'null'},{'x':'a\nb\tc\rd\\N'},{'x':{}},{'x':{'b':2,'a':[None,True]}},{'x':[None,1,'x']},{'x':False},{'x':0},{'x':1.25}]
    rows=[]
    for value in values:
        original,converted=c.execute("SELECT p.payload->>'x',r.x FROM (SELECT %s::jsonb AS payload) p CROSS JOIN LATERAL pg_catalog.jsonb_to_record(CASE WHEN pg_catalog.jsonb_typeof(p.payload)='object' THEN p.payload ELSE '{}'::jsonb END) r(x pg_catalog.text)",(Jsonb(value),)).fetchone()
        assert original==converted,(value,original,converted)
        rows.append({'input':value,'operator':original,'all_text_record':converted})
    return {'json_shape_cases':rows,'differences':0,'scope':'Total extraction of missing/null/scalar/array/object and escaped text only; not full SQL error-order equivalence or application-function guard acceptance.'}


def main():
    original_write=projection_pair.write_report
    def recorded(path,value):
        original_write(path,{**value,'kind':'unranked_rollback_all_text_record_projection','application_function_guard_parity':False,'eligibility_unproven':True,'selected_for_application':False})
    projection_pair.write_report=recorded
    projection_pair.component({'operators':operators,'all_text_record':record},source_identity,'reports/text-record-projection-1m.json',differential=differential)


if __name__=='__main__':main()
