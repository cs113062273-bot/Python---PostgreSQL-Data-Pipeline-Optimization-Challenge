"""Differential native JSONB extraction/proof; not full-pipeline eligibility."""
import copy
import argparse
import hashlib
from pathlib import Path
import random
import uuid
from psycopg import sql
from psycopg.types.json import Jsonb
from bench import native_c_lab as lab
from bench.common import ROOT,write_report
from pipeline.safe_domain import _bundle,condition_sql
from tests.semantics.fuzz import cases


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',default='reports/native-c-extraction-semantics-v3.json');args=parser.parse_args()
    db='lab_native_c_'+uuid.uuid4().hex[:8];lab.fixture(db)
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    report={'kind':'isolated_native_c_extraction_and_proof_differential','database':db,'port':lab.PORT,'all_passed':False,'application_integration':False,'eligibility_unproven':True,'phases':[],'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()};write_report(output,report)
    with lab.connect(db,autocommit=True) as c:
        function,build=lab.install(c);report['build']=build;report['phases'].append('Native library loaded and exact source identity returned');write_report(output,report)
        original=c.execute('SELECT raw->\'values\' FROM public.raw_data ORDER BY raw_id LIMIT 1').fetchone()[0]
        bundles=[_bundle('integer_column',15).replace('c.','n.'),_bundle('numeric_column',30).replace('c.','n.'),"coalesce(n.id_column_1,'')","coalesce(n.datetime_column_1,'')"]
        compare='SELECT ROW('+','.join('n.'+field for field in lab.FIELDS)+') IS NOT DISTINCT FROM ROW('+','.join("p.payload->>'"+field+"'" for field in lab.FIELDS)+'),n.pipeline_domain_safe IS NOT DISTINCT FROM ('+condition_sql().replace('%','%%')+'),n.pipeline_domain_safe FROM (SELECT %s::jsonb payload) p CROSS JOIN LATERAL '+function+'(p.payload) n('+lab.definition()+') CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW('+','.join(bundles)+')]) c(integers text,numerics text,identifier text,stamp text)'
        checks=0;safe=0
        def check(value,sql_null=False):
            nonlocal checks,safe
            values_equal,proof_equal,accepted=c.execute(compare,(None if sql_null else Jsonb(value),)).fetchone()
            assert values_equal and proof_equal,(checks,value,values_equal,proof_equal,accepted)
            checks+=1;safe+=bool(accepted)
        check(None,sql_null=True)
        for value in [None,{},[],[1,2],0,True,False,'x',{'unknown':1},{'integer_column_01':'bad'}]:check(value)
        report['phases'].append('SQL NULL and every JSON root shape match all 64 operator values and proof');write_report(output,report)
        units=[None,'','0','-0','+0','1','-9999','999999.999','1000000','12345','1.0001','1e2','NaN','Infinity',' 2 ','\tX\n','x\\N','null','中文',{},[1,None],True,False,0,1.25]
        for field in lab.FIELDS:
            for unit in units:
                payload=copy.deepcopy(original);payload[field]=unit;check(payload)
        report['phases'].append('Every extracted field checked against scalar, null, Unicode, nested and malformed values');report['individual_field_vectors']=len(lab.FIELDS)*len(units);write_report(output,report)
        for case in cases(91827,600):
            payload=copy.deepcopy(original)
            if 'shape' in case:payload=case['shape']
            elif case.get('missing_values'):payload=None
            else:
                for key,value in case.get('edits',[]):payload[key]=value
                payload.pop(case.get('remove'),None)
            check(payload)
        dates=['0000-01-01 00:00:00','0001-01-01 00:00:00','9999-12-31T23:59:59.123456+12:59','2000-02-29 00:00:00Z','1900-02-29 00:00:00','2000-02-30 00:00:00','2000-12-31 24:00:00','2000-12-31 00:00:60','2000-12-31 00:00:00+13','2000-12-31 00:00:00-00','2000-12-31 00:00:00+1230','2000-12-31 00:00:00.','2000-12-31 00:00:00.0000000','2000-12-31 00:00:00+12:','2000-12-31 00:00:00\n']
        rng=random.Random(27177)
        for _ in range(2000):dates.append(f'{rng.randrange(10000):04}-{rng.randrange(14):02}-{rng.randrange(33):02} '+f'{rng.randrange(26):02}:{rng.randrange(62):02}:{rng.randrange(62):02}'+rng.choice(['','Z','z','+03','-12:59','.123456+12','.0000000','+13','\n']))
        for stamp in dates:payload=copy.deepcopy(original);payload['datetime_column_1']=stamp;check(payload)
        report['date_vectors']=len(dates)
        invalid=[]
        for label,defs in [('nontext',lab.definition().replace('id_column_1 pg_catalog.text','id_column_1 pg_catalog.int4',1)),('unknown',lab.definition().replace('id_column_1 pg_catalog.text','unknown pg_catalog.text',1)),('missingproof',','.join(field+' pg_catalog.text' for field in lab.FIELDS))]:
            try:c.execute('SELECT * FROM '+function+"('{}'::jsonb) n("+defs+')').fetchall()
            except Exception as error:
                assert getattr(error,'sqlstate',None)=='XX000',error
                invalid.append({'shape':label,'rejected_sqlstate':error.sqlstate})
            else:raise AssertionError('Unsafe native output descriptor accepted: '+label)
        report.update({'all_passed':True,'total_payload_vectors':checks,'proof_safe_vectors':safe,'extraction_differences':0,'proof_differences':0,'unsafe_output_descriptors_rejected':invalid,'scope':'Pure argument extraction and sufficient-domain proof, all 64 TEXT values compared directly with original operators. No SQL casts/transforms, managed counters/versions, dump/crash recovery, whole-pipeline percentage or application guard acceptance is proved.'});write_report(output,report)
        print('Native C extraction/proof:',checks,'payloads, zero differences;',len(invalid),'unsafe descriptors rejected',flush=True)


if __name__=='__main__':main()
