"""Original recipes versus one typed table5/table6/table2 native COPY call."""
import argparse
import copy
import hashlib
from pathlib import Path
import uuid
from psycopg import sql
from psycopg.types.json import Jsonb
from bench import native_c_lab as lab,native_details_lab as details,native_compact_sql as compact
from bench.common import ROOT,write_report
from bench.run import prepared
from tests.semantics.fuzz import cases,SETTINGS
from tests.equivalence.oracle import canonical,protected_schema_signature,sequence_signature


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--count',type=int,default=200);p.add_argument('--output',required=True);p.add_argument('--build-report',default='reports/native-details-ingest-build.json');args=p.parse_args()
    output=(ROOT/args.output).resolve();assert output.is_relative_to(ROOT/'reports') and not output.exists()
    db='lab_native_details_'+uuid.uuid4().hex[:8];lab.fixture(db)
    report={'kind':'native_typed_three_details_semantics','database':db,'passed':False,'cases':[],
            'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            'scope':'Mutated 64-row official fixtures; native identities/indexes/CHECK/FKs active. Six full canonical outputs and first SQLSTATE/message. No counters/version/recovery/performance/package or official eligibility claim.'}
    with lab.connect(db,autocommit=True) as c:
        normalize,axes,normbuild=lab.install_compact(c);loader,build=details.install(c,args.build_report)
        report.update({'build':build,'normalizer_build':normbuild});write_report(output,report)
        raw_id,original=c.execute('SELECT raw_id,raw FROM public.raw_data ORDER BY raw_id LIMIT 1').fetchone()
        scopes=[r[0] for r in c.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        recipes=prepared('baseline',scopes);before=protected_schema_signature(c);sequences=sequence_signature(c)
        vectors=list(cases(718,args.count))
        for pos in range(1,15):
            for kind in (' x ','Y',None,{},'\tX\n'):
                vectors.append({'name':'details-axis-position','edits':[(f'integer_column_{pos+1}',str(pos)),(f'text_column_{pos+3}',kind),(f'numeric_column_{2*pos+1}','0'),(f'numeric_column_{2*pos+2}','0.001')]})
        for value in [None,'',' ', 'AA',' ab ','Zz','ÅB','KA','ſx',{},[],True,'X','ABC','\nAB']:
            vectors.append({'name':'details-reference-domain','edits':[('text_column_3',value)]})
        for value in [None,'','0','-1','1','9999','1.1','+0001',' 2 ','bad']:
            vectors.append({'name':'details-numeric-round','edits':[('integer_column_1',value)]})
        for index,case in enumerate(vectors):
            raw=copy.deepcopy(original)
            if 'shape' in case:raw['values']=case['shape']
            elif case.get('missing_values'):raw.pop('values',None)
            else:
                for key,value in case.get('edits',[]):raw['values'][key]=value
                raw['values'].pop(case.get('remove'),None)
            c.execute('UPDATE public.raw_data SET raw=%s WHERE raw_id=%s',(Jsonb(raw),raw_id))
            outcomes={};safe=None
            for mode in ('original','typed_details'):
                try:
                    with c.transaction(force_rollback=True):
                        c.execute("SET LOCAL work_mem='64MB'")
                        for key,value in SETTINGS[case.get('settings',0)].items():c.execute(sql.SQL('SET LOCAL {} TO {}').format(sql.Identifier(key),sql.Literal(value)))
                        if mode=='typed_details':
                            c.execute(compact.projection(scopes,normalize,'pg_temp.native_compact_case'))
                            safe=c.execute(details.code_domain('pg_temp.native_compact_case')).fetchone()[0]
                        for i,recipe in enumerate(recipes):
                            if mode=='typed_details' and safe and i==3:
                                counts=c.execute('SELECT '+loader+"('pg_temp.native_compact_case','public.table1','public.table5','public.table6','public.table2','public.table4')").fetchone()[0]
                                assert counts==[c.execute('SELECT count(*) FROM public.'+t).fetchone()[0] for t in ('table5','table6','table2')]
                                break
                            c.execute(recipe['statements'][0])
                        outcomes[mode]={'sqlstate':'00000','tables':{t:[r[0] for r in c.execute('SELECT value::text FROM ('+canonical(t)+') v ORDER BY value::text')] for t in ('table3','table4','table1','table5','table6','table2')}}
                except Exception as error:outcomes[mode]={'sqlstate':getattr(error,'sqlstate',None),'primary':getattr(getattr(error,'diag',None),'message_primary',str(error))}
                assert protected_schema_signature(c)==before and sequence_signature(c)==sequences
            assert outcomes['original']==outcomes['typed_details'],(index,case,outcomes)
            report['cases'].append({'index':index,'name':case['name'],'fast_domain':safe,'equivalent':True,'sqlstate':outcomes['original']['sqlstate']})
            if index%25==0:print('Details COPY',index,'zero differences',flush=True);write_report(output,report)
        c.execute('UPDATE public.raw_data SET raw=%s WHERE raw_id=%s',(Jsonb(original),raw_id))
    report['passed']=True;write_report(output,report);print('Typed three-details COPY:',len(vectors),'cases PASS',flush=True)


if __name__=='__main__':main()
