"""Both exact bag comparators must reject deliberately wrong native rows."""
import uuid
from bench.common import connect,write_report
from bench.fixture import create_fixture
from bench.run import execute
from bench.audit_ranked_result import exact_via_private_temp
from pipeline.checkpoints import physical_fingerprints


def main():
    db='lab_tagged_control_'+uuid.uuid4().hex[:8];create_fixture(db)
    report={'kind':'exact_typed_bag_comparator_negative_controls','database':db,'official_score':False,'checks':[]}
    with connect(db,autocommit=True) as candidate,connect(db,autocommit=True) as reference:
        execute(candidate,'baseline',['group001'])
        null_dimension=candidate.execute('INSERT INTO public.table3 DEFAULT VALUES RETURNING id_column_1').fetchone()[0]
        first_parent=candidate.execute('SELECT min(id_column_1) FROM public.table1').fetchone()[0]
        candidate.execute('UPDATE public.table1 SET id_column_4=%s WHERE id_column_1=%s',(null_dimension,first_parent))
        before=physical_fingerprints(candidate)
        methods=('except_all','tagged_count')
        for method in methods:
            assert len(exact_via_private_temp(candidate,reference,comparison=method))==6
            report['checks'].append({'case':'intact','method':method,'accepted':True})
        faults=[
            ('same_count_wrong_value','UPDATE public.table2 SET integer_column_2=integer_column_2+1 WHERE id_column_1=(SELECT min(id_column_1) FROM public.table2)','table2'),
            ('duplicate_logical_row','INSERT INTO public.table3(text_column_1,text_column_2,numeric_column_1,text_column_3,text_column_4) SELECT text_column_1,text_column_2,numeric_column_1,text_column_3,text_column_4 FROM public.table3 ORDER BY id_column_1 LIMIT 1','table3'),
            ('missing_row','DELETE FROM public.table2 WHERE id_column_1=(SELECT min(id_column_1) FROM public.table2)','table2'),
            ('null_changes_value','UPDATE public.table2 SET text_column_1=NULL WHERE id_column_1=(SELECT min(id_column_1) FROM public.table2)','table2'),
            ('missing_vs_all_null_dimension','UPDATE public.table1 SET id_column_4=NULL WHERE id_column_1='+str(first_parent),'table1'),
            ('global_numeric_sum_preserved',"WITH selected AS (SELECT min(id_column_1) AS id FROM public.table2 GROUP BY id_column_2 ORDER BY id_column_2 LIMIT 2), ranked AS (SELECT id,row_number() OVER(ORDER BY id) AS n FROM selected) UPDATE public.table2 t SET integer_column_2=t.integer_column_2+CASE WHEN r.n=1 THEN 1 ELSE -1 END FROM ranked r WHERE r.id=t.id_column_1",'table2'),
        ]
        for name,mutation,table in faults:
            with candidate.transaction(force_rollback=True):
                candidate.execute(mutation)
                for method in methods:
                    try:exact_via_private_temp(candidate,reference,comparison=method)
                    except AssertionError as error:assert str(error)=='Exact live comparison differs: '+table
                    else:raise AssertionError((name,method,'accepted incorrect data'))
                    report['checks'].append({'case':name,'method':method,'rejected':True})
            assert physical_fingerprints(candidate)==before
        report['all_passed']=True
        report['all_output_faults_rolled_back']=True
        report['scope']='64-row verifier fixture, not pipeline performance. Every value/multiplicity is compared; deliberately wrong same-count, duplicate, missing, NULL, dimension-presence and zero-global-sum data must fail. Native identity sequence gaps from rolled-back inserts are normal.'
        write_report('reports/exact-tagged-comparator-controls.json',report)
        print('Exact bag comparators:',len(report['checks']),'checks PASS',flush=True)


if __name__=='__main__':main()
