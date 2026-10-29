"""All six original outputs/first errors against compact native transforms."""
import argparse
import copy
import hashlib
import uuid
from pathlib import Path
from psycopg import sql
from psycopg.types.json import Jsonb
from bench import native_c_lab as lab,native_stamp_lab as compact
from bench.common import ROOT,write_report
from bench.run import prepared
from tests.equivalence.oracle import canonical,protected_schema_signature,sequence_signature
from tests.semantics.fuzz import cases,SETTINGS


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--count',type=int,default=250);p.add_argument('--output',default='reports/native-typed-stamp-semantics.json');args=p.parse_args()
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    db='lab_native_stamp_'+uuid.uuid4().hex[:8];lab.fixture(db)
    report={'kind':'unqualified_native_typed_stamp_six_table_differential','database':db,'port':lab.PORT,'application_integration':False,'eligibility_unproven':True,'all_passed':False,'cases':[],'mismatches':[],'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    with lab.connect(db,autocommit=True) as c:
        normalizer,axes,build=compact.install_compact(c);report['build']=build;write_report(output,report)
        raw_id,original=c.execute('SELECT raw_id,raw FROM public.raw_data ORDER BY raw_id LIMIT 1').fetchone()
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')];recipes=prepared('baseline',scopes)
        schema=protected_schema_signature(c);seq=sequence_signature(c)
        vectors=list(cases(718,args.count))
        for position in range(1,15):
            for kind in [' x ','y',' z ',None,'','\tX\n',{'k':'x'},['y']]:vectors.append({'name':'axis-text-selection','edits':[('integer_column_'+str(position+1),str(position)),('text_column_'+str(position+3),kind),('numeric_column_'+str(2*position+1),'0.001'),('numeric_column_'+str(2*position+2),'0')],'settings':len(vectors)%4})
        for value in [None,'','0','-0','0.001','-0.001','0.0001','999999.999','-999999.999','1000000','NaN','Infinity',' 0.001 ']:
            for field in ['numeric_column_1','numeric_column_2','numeric_column_3','numeric_column_4','numeric_column_29','numeric_column_30']:vectors.append({'name':'scaled-int-boundary','edits':[(field,value)],'settings':len(vectors)%4})
        for value in ['0001-01-01T00:00:00Z','9999-12-31T23:59:59.999999-12:59','2024-02-29T23:59:59-12:30','2023-02-29T00:00:00Z','2024-11-03T01:30:00','2024-03-10T02:30:00','2018-11-04T00:30:00','2018-02-18T00:30:00','2024-01-01T00:00:00+13:00','2024-01-01T00:00:00+00:01',None,'',' ','bad','infinity','-infinity']:
            for setting in range(4):vectors.append({'name':'native-STABLE-stamp','edits':[('datetime_column_1',value)],'settings':setting})
        for index,case in enumerate(vectors):
            raw=copy.deepcopy(original)
            if 'shape' in case:raw['values']=case['shape']
            elif case.get('missing_values'):raw.pop('values',None)
            else:
                for key,value in case.get('edits',[]):raw['values'][key]=value
                raw['values'].pop(case.get('remove'),None)
            c.execute('UPDATE public.raw_data SET raw=%s WHERE raw_id=%s',(Jsonb(raw),raw_id));outcomes={};fast=None
            for mode in ['original','native_compact']:
                c.execute('TRUNCATE public.table2,public.table5,public.table6,public.table1,public.table3,public.table4 RESTART IDENTITY')
                try:
                    with c.transaction(force_rollback=True):
                        for key,value in SETTINGS[case.get('settings',0)].items():c.execute(sql.SQL('SET LOCAL {} TO {}').format(sql.Identifier(key),sql.Literal(value)))
                        if mode=='native_compact':
                            c.execute(compact.projection(scopes,normalizer,'pg_temp.native_compact_case'))
                            fast=c.execute('SELECT coalesce(bool_and(pipeline_domain_safe),true) FROM pg_temp.native_compact_case').fetchone()[0]
                        for recipe in recipes:
                            original_sql=recipe['statements'][0]
                            c.execute(compact.statement(original_sql,scopes,axes) if mode=='native_compact' and fast else original_sql)
                        outcomes[mode]={'sqlstate':'00000','tables':{table:[r[0] for r in c.execute('SELECT value::text FROM ('+canonical(table)+') v ORDER BY value::text')] for table in ['table3','table4','table1','table5','table6','table2']}}
                except Exception as error:outcomes[mode]={'sqlstate':getattr(error,'sqlstate',None),'primary':getattr(getattr(error,'diag',None),'message_primary',str(error))}
                assert protected_schema_signature(c)==schema and sequence_signature(c)==seq
                assert all(c.execute('SELECT count(*) FROM public.'+table).fetchone()[0]==0 for table in ['table3','table4','table1','table5','table6','table2'])
            equal=outcomes['original']==outcomes['native_compact'];report['cases'].append({'index':index,'case':case,'fast_domain':fast,'equivalent':equal,'sqlstates':{key:value['sqlstate'] for key,value in outcomes.items()}})
            if not equal:report['mismatches'].append({'index':index,'case':case,'outcomes':outcomes})
            if index%50==0:print('Compact native',index,'differences',len(report['mismatches']),flush=True);write_report(output,report)
            if not equal:write_report(output,report);raise AssertionError('Native compact mismatch; retain complete case and outputs')
        c.execute('UPDATE public.raw_data SET raw=%s WHERE raw_id=%s',(Jsonb(original),raw_id))
    report.update({'all_passed':True,'scope':'Actual original six INSERTs and native identities/constraints versus compact cache and six transformed INSERTs. Same first SQLSTATE/message and all canonical values, including 14 positions and scaling boundaries. This is not managed-counter/version/recovery or full-service timing qualification.'});write_report(output,report)
    print('Compact native:',len(report['cases']),'six-table/error cases; zero differences',flush=True)


if __name__=='__main__':main()
