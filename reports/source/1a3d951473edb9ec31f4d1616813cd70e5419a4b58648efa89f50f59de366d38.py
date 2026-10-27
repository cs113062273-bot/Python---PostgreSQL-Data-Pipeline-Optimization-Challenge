"""Audit recorded timing arithmetic/provenance and independently recheck live rows.

Exact comparisons use a session-private TEMP table per destination, native
binary COPY and bidirectional EXCEPT ALL against a freshly executed original.
No stored expected fingerprint substitutes for this exact comparison. Neither
old timing nor this untimed audit is presented as a new measured speedup.
"""
import argparse
from datetime import datetime,timezone
import hashlib
import json
from pathlib import Path
import re
import statistics
import subprocess

from psycopg import sql
from bench.common import ROOT,connect,require_lab,write_report
from pipeline.checkpoints import source_digest,normalize_catalog
from pipeline.recipes import ORDER,compiler_sha256
from tests.equivalence.oracle import canonical,fingerprints,protected_schema_signature,sequence_signature,assert_relationships


def load(path):return json.loads(path.read_text(encoding='utf-8'))


def schema_value(c):
    value=normalize_catalog(protected_schema_signature(c))
    database=c.execute('SELECT current_database()').fetchone()[0]
    # Cross-database comparison normalizes only the database label in catalog
    # fields, never type/schema/default/constraint/index definitions.
    for column in value['columns']:
        for key in column:
            if key.endswith('_catalog') and column[key]==database:column[key]='<database>'
    return value


def exact_via_jsonb_temp(candidate,reference):
    results={}
    for table in ORDER:
        name='audit_'+__import__('uuid').uuid4().hex
        with reference.transaction():
            reference.execute("SET LOCAL work_mem='256MB'")
            reference.execute(sql.SQL('CREATE TEMP TABLE {}(value jsonb) ON COMMIT DROP').format(sql.Identifier(name)))
            checksum=hashlib.sha256();size=0
            with candidate.cursor().copy('COPY ('+canonical(table)+') TO STDOUT (FORMAT BINARY)') as reader:
                with reference.cursor().copy(sql.SQL('COPY {} FROM STDIN (FORMAT BINARY)').format(sql.Identifier('pg_temp',name))) as writer:
                    pending=bytearray()
                    for block in reader:
                        checksum.update(block);size+=len(block);pending.extend(block)
                        if len(pending)>=1024*1024:writer.write(bytes(pending));pending.clear()
                    if pending:writer.write(bytes(pending))
            query=sql.SQL('SELECT EXISTS((SELECT value FROM {} EXCEPT ALL SELECT value FROM ('+canonical(table)+') original) UNION ALL (SELECT value FROM ('+canonical(table)+') original EXCEPT ALL SELECT value FROM {}))').format(sql.Identifier('pg_temp',name),sql.Identifier('pg_temp',name))
            if reference.execute(query).fetchone()[0]:raise AssertionError('Exact live comparison differs: '+table)
            results[table]={'bidirectional_except_all':True,'transferred_binary_bytes':size,'transfer_sha256':checksum.hexdigest()}
        assert reference.execute('SELECT to_regclass(%s)',('pg_temp.'+name,)).fetchone()[0] is None
    return results


def typed_canonical(c,table):
    """Flatten the same logical graph using native column types.

    Native surrogate identities are excluded exactly as in canonical(). The
    joined dimension has a presence bit to distinguish a missing row from a
    row whose non-ID attributes are all NULL. Text uses C collation, matching
    JSONB's binary string equality, rather than locale-dependent equality.
    """
    def attributes(name):
        return c.execute("SELECT attname,format_type(atttypid,atttypmod),atttypid FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum",('public.'+name,)).fetchall()
    expressions=[];definitions=[]
    def field(alias,name,kind,oid,out=None):
        expr=sql.SQL('{}.{}').format(sql.Identifier(alias),sql.Identifier(name))
        definition=sql.Identifier(out or name)+sql.SQL(' '+kind)
        if oid in (25,1043):expr+=sql.SQL(' COLLATE "C"');definition+=sql.SQL(' COLLATE "C"')
        expressions.append(expr+sql.SQL(' AS ')+sql.Identifier(out or name));definitions.append(definition)
    excluded={'id_column_1'} if table=='table3' else set() if table=='table4' else {'id_column_1','id_column_4'} if table=='table1' else {'id_column_1','id_column_2'}
    for name,kind,oid in attributes(table):
        if name not in excluded:field('t',name,kind,oid)
    query=sql.SQL(' FROM {} t').format(sql.Identifier('public',table))
    if table=='table1':
        expressions.append(sql.SQL('d.id_column_1 IS NOT NULL AS dimension_exists'));definitions.append(sql.SQL('dimension_exists boolean'))
        for name,kind,oid in attributes('table3'):
            if name!='id_column_1':field('d',name,kind,oid,'dimension_'+name)
        query+=sql.SQL(' LEFT JOIN public.table3 d ON d.id_column_1=t.id_column_4')
    elif table in ('table2','table5','table6'):
        field('m','id_column_3','bigint',20,'source_raw_id')
        query+=sql.SQL(' LEFT JOIN public.table1 m ON m.id_column_1=t.id_column_2')
    return sql.SQL('SELECT ')+sql.SQL(',').join(expressions)+query,definitions


def exact_via_private_temp(candidate,reference):
    # Native typed rows avoid converting millions of wide rows to JSONB just
    # for verification. EXCEPT ALL still compares every value and multiplicity.
    # Force bounded-memory sort instead of an estimated-size hash set.
    results={}
    for table in ORDER:
        name='audit_'+__import__('uuid').uuid4().hex
        query,definitions=typed_canonical(reference,table)
        with reference.transaction():
            reference.execute("SET LOCAL work_mem='64MB'; SET LOCAL enable_hashagg=off")
            reference.execute(sql.SQL('CREATE TEMP TABLE {}({}) ON COMMIT DROP').format(sql.Identifier(name),sql.SQL(',').join(definitions)))
            checksum=hashlib.sha256();size=0
            with candidate.cursor().copy(sql.SQL('COPY ({}) TO STDOUT (FORMAT BINARY)').format(query)) as reader:
                with reference.cursor().copy(sql.SQL('COPY {} FROM STDIN (FORMAT BINARY)').format(sql.Identifier('pg_temp',name))) as writer:
                    pending=bytearray()
                    for block in reader:
                        checksum.update(block);size+=len(block);pending.extend(block)
                        if len(pending)>=1024*1024:writer.write(bytes(pending));pending.clear()
                    if pending:writer.write(bytes(pending))
            comparison=sql.SQL('SELECT EXISTS((SELECT * FROM {} EXCEPT ALL {}) UNION ALL ({} EXCEPT ALL SELECT * FROM {}))').format(sql.Identifier('pg_temp',name),query,query,sql.Identifier('pg_temp',name))
            if reference.execute(comparison).fetchone()[0]:raise AssertionError('Exact live comparison differs: '+table)
            results[table]={'bidirectional_except_all':True,'representation':'native typed logical rows; C text collation; dimension presence preserved','transferred_binary_bytes':size,'transfer_sha256':checksum.hexdigest()}
        assert reference.execute('SELECT to_regclass(%s)',('pg_temp.'+name,)).fetchone()[0] is None
    return results


def checker_controls():
    # A validator must also reject a deliberately wrong result. Both views
    # use the same committed official fixture; only the candidate transaction
    # sees the fault injection, which is always rolled back.
    from bench.fixture import create_fixture
    from bench.run import execute
    db='lab_audit_control_'+__import__('uuid').uuid4().hex[:8];create_fixture(db)
    with connect(db,autocommit=True) as candidate,connect(db,autocommit=True) as reference:
        execute(candidate,'baseline',['group001'])
        before=source_digest(candidate,['group001'])
        assert len(exact_via_private_temp(candidate,reference))==6
        with candidate.transaction(force_rollback=True):
            candidate.execute('UPDATE public.table2 SET integer_column_2=integer_column_2+1 WHERE id_column_1=(SELECT min(id_column_1) FROM public.table2)')
            try:exact_via_private_temp(candidate,reference)
            except AssertionError as error:assert str(error)=='Exact live comparison differs: table2'
            else:raise AssertionError('Exact comparator accepted an edited output')
        with candidate.transaction(force_rollback=True):
            candidate.execute("UPDATE public.raw_data SET raw=jsonb_set(raw,'{values,text_column_1}',to_jsonb('audit_fault'::text)) WHERE raw_id=1")
            assert source_digest(candidate,['group001'])!=before
        assert source_digest(candidate,['group001'])==before
        assert len(exact_via_private_temp(candidate,reference))==6
    return {'database':db,'fixture_source_rows':64,'unchanged_exact_result_accepted':True,'same_count_edited_output_rejected':True,'source_edit_with_unchanged_stored_raw_hash_detected':True,'all_fault_edits_rolled_back':True,'scope':'Functional validator controls, not a benchmark or managed-counter/version test.'}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--offline',action='store_true')
    p.add_argument('--cohorts',nargs='+',default=['reports/native-fresh-copy-1m-repeated-v2.json','reports/native-official-recheck-after-recovery-1m.json'])
    p.add_argument('--reference-cohort',default='reports/native-official-recheck-after-recovery-1m.json')
    p.add_argument('--output')
    args=p.parse_args()
    output=Path(args.output or ('reports/ranked-cohort-offline-audit.json' if args.offline else 'reports/ranked-cohort-live-audit.json')).resolve()
    if not output.is_relative_to(ROOT/'reports') or output.exists():p.error('Choose a new report path under reports/')
    result={'kind':'independent_recorded_cohort_and_live_database_audit','observed_at_utc':datetime.now(timezone.utc).isoformat(),'passed':False,'offline_only':args.offline,'cohorts':[],'live_checks':[],'limits':['Recorded arithmetic/provenance checks do not retroactively remeasure elapsed time.','Live checks occur after all processing timers; they cannot improve or replace a timing result.','Local Windows evidence is not an official score or Linux/SSH qualification.']}
    result['auditor_sha256_normalized_lf']=hashlib.sha256(Path(__file__).read_text(encoding='utf-8').encode()).hexdigest()
    try:
        frozen=ROOT/'.runtime/bench_native_80'
        commit=subprocess.check_output(['git','-C',str(frozen),'rev-parse','HEAD'],text=True).strip()
        assert commit=='7b0f0776032b9ce222882b063e992d3fda1e9e8d'
        dirty=subprocess.check_output(['git','-C',str(frozen),'status','--porcelain'],text=True).splitlines()
        for line in dirty:
            filename=line[3:]
            assert re.fullmatch(r'reports/sql/[0-9a-f]{64}\.sql',filename),line
            path=frozen/filename
            assert hashlib.sha256(path.read_text(encoding='utf-8').encode()).hexdigest()==path.stem
        manifest=load(ROOT/'upstream.json')
        for name,expected in manifest['files_sha256'].items():
            for tree in (ROOT,frozen):assert hashlib.sha256((tree/'app'/name).read_bytes()).hexdigest()==expected,name
        core=compiler_sha256()
        frozen_core=subprocess.check_output([__import__('sys').executable,'-c','from pipeline.recipes import compiler_sha256; print(compiler_sha256())'],cwd=frozen,text=True).strip()
        assert core==frozen_core=='ce4f60cd7aae88e708301ccbcbe8b97dbafebded5cf8ea4cf34bd0f21028fbfd'
        result['source_checks']={'frozen_commit':commit,'frozen_non_source_status':dirty,'compiler_sha256_current_and_frozen':core,'upstream_files_verified_per_tree':len(manifest['files_sha256'])}
        all_runs=[]
        for name in args.cohorts:
            path=(ROOT/name).resolve();assert path.is_relative_to(ROOT/'reports')
            r=load(path);assert r['kind']=='fresh_local_original_service_complete_processing' and r.get('summary') and r['source_rows']==1000000
            groups={}
            for row in r['runs']:
                child_path=Path(row['report']).resolve();assert child_path.is_relative_to(ROOT/'reports')
                child=load(child_path);mode=row['mode'];actual_mode='baseline' if mode=='official' else mode
                assert child['mode']==actual_mode and child['wall_seconds']==row['wall_seconds']
                assert child['destination_warmup_before_measured_run'] is False
                assert all(child['environment']['settings'][key]=='on' for key in ('fsync','full_page_writes','synchronous_commit'))
                assert all(child[k] is True for k in ('six_table_fingerprints_equivalent','counter_reconciliation','protected_schema_equivalent','sequence_options_and_ownership_equivalent','workspace_namespace_restored'))
                assert len(child['versions'])==row['versions']==6
                total=sum(i['seconds'] for i in child['timing_report']['items'] if i.get('include_in_total',True))
                assert abs(total-row['native_measured_seconds'])<1e-6
                assert total<=row['wall_seconds']+.5
                envelopes=[i for i in child['timing_report']['items'] if 'batch envelope' in i['name']]
                assert len(envelopes)==2 and all(i['include_in_total'] is False for i in envelopes)
                assert {i['name'].split(' - LOCAL')[0] for i in envelopes}=={'Expansion database batch envelope','Version database batch envelope'}
                if actual_mode=='guarded_copy':
                    arch=child['execution_architecture'];assert arch['compiler_sha256']==core and arch['safe_domain'] is True and arch['copy_workspace']['cleanup_success'] is True
                    batch=ROOT/'reports/sql'/(arch['batch_sql_sha256']+'.sql')
                    assert hashlib.sha256(batch.read_text(encoding='utf-8').encode()).hexdigest()==batch.stem
                groups.setdefault(mode,[]).append(row['wall_seconds']);all_runs.append((row,child))
            medians={k:statistics.median(v) for k,v in groups.items()}
            control=r.get('comparison_variant','baseline')
            reductions={k:100*(1-v/medians[control]) for k,v in medians.items()}
            for mode in medians:
                assert abs(medians[mode]-r['summary'][mode]['median_wall_seconds'])<1e-9
                stored=r['summary'][mode].get('reduction_percent',r['summary'][mode].get('time_reduction_percent_vs_comparison_variant'))
                assert abs(reductions[mode]-stored)<1e-8
            result['cohorts'].append({'report':name,'sha256_normalized_lf':hashlib.sha256(path.read_text(encoding='utf-8').encode()).hexdigest(),'runs':len(r['runs']),'comparison_variant':control,'recomputed_medians_seconds':medians,'recomputed_time_reduction_percent':reductions,'timer_envelopes_not_double_counted':True,'all_six_version_and_provenance_gates':True})
        write_report(output,result)
        if not args.offline:
            result['checker_controls']=checker_controls();write_report(output,result)
            ref_cohort=load(ROOT/args.reference_cohort)
            ref_run=next(row for row in ref_cohort['runs'] if row['mode'] in ('official','baseline'))
            reference_db=ref_run['database'];assert reference_db.startswith('lab_')
            with connect(reference_db,autocommit=True) as reference,connect('postgres_data_manager',autocommit=True) as control:
                require_lab(reference)
                assert all(reference.execute('SELECT current_setting(%s)',(key,)).fetchone()[0]=='on' for key in ('fsync','full_page_writes','synchronous_commit'))
                scopes=[r[0] for r in reference.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
                expected_source=source_digest(reference,scopes);expected_schema=schema_value(reference);expected_seq=sequence_signature(reference);expected_fp=fingerprints(reference)
                result['reference']={'database':reference_db,'source_sha256':expected_source,'fingerprints':expected_fp,'method':'Fresh unchanged original service, not an earlier stored expected JSON table.'}
                for row,child in all_runs:
                    db=row['database'];assert db.startswith('lab_')
                    with connect(db,autocommit=True) as candidate, candidate.transaction():
                        candidate.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');require_lab(candidate)
                        candidate.execute(sql.SQL('LOCK TABLE '+','.join('{}' for _ in ('raw_data',)+ORDER)+' IN SHARE MODE').format(*(sql.Identifier('public',t) for t in ('raw_data',)+ORDER)))
                        assert candidate.execute('SELECT count(*) FROM public.raw_data').fetchone()[0]==1000000
                        assert source_digest(candidate,scopes)==expected_source
                        assert schema_value(candidate)==expected_schema and sequence_signature(candidate)==expected_seq
                        actual=fingerprints(candidate);assert actual==expected_fp
                        counts=dict(candidate.execute("SELECT table_name,row_count FROM public.pgdm_table_row_counts WHERE schema_name='public'").fetchall())
                        # The original expansion manages six destination
                        # counters. Source count is independently read above;
                        # it has no pgdm_table_row_counts entry in this path.
                        assert set(counts)==set(ORDER) and all(counts[t]==actual[t][0] for t in ORDER)
                        versions=control.execute("SELECT schema_name||'.'||table_name,version_code,sql_recipe FROM app_control.table_versions WHERE database_name=%s AND version_code<>'0000' ORDER BY table_name",(db,)).fetchall()
                        assert {(v[0],v[1]) for v in versions}=={(v['table_name'],v['version_code']) for v in child['versions']}
                        arch=child.get('execution_architecture')
                        if arch:assert all('compiler_sha256='+core in v[2] and 'batch_sql_sha256='+arch['batch_sql_sha256'] in v[2] for v in versions)
                        assert all(v[0]=='p' for v in candidate.execute('SELECT relpersistence FROM pg_class WHERE oid=ANY(%s::regclass[])',(['public.'+t for t in ORDER],)).fetchall())
                        assert_relationships(candidate)
                        assert candidate.execute("SELECT to_regnamespace('pgdm_pipeline_work')").fetchone()[0] is None
                        exact=exact_via_private_temp(candidate,reference)
                        result['live_checks'].append({'database':db,'mode':row['mode'],'actual_source_sha_and_one_million_rows_match':True,'exact_six_tables':exact,'actual_counters_and_six_control_versions':True,'full_schema_sequence_and_logged_destination_checks':True})
                    write_report(output,result);print('Live exact audit PASS',db,row['mode'],flush=True)
        result['passed']=True;write_report(output,result)
        print('Audit PASS:',len(result['cohorts']),'cohorts;',len(result['live_checks']),'live databases; no new time reduction invented',flush=True)
    except BaseException as error:
        result['error']=type(error).__name__+': '+str(error);write_report(output,result);raise


if __name__=='__main__':main()
