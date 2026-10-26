"""Guarded full normalization: original execution outside a proven safe domain."""
import time
import re
import hashlib
from pipeline.normalization import prepare_sql as normalize_sql,rewrite
from pipeline.recipes import EXTRACT,compiler_sha256,ORDER,CTAS_MODES,BULK_CTAS_MODES,COPY_MODES
from pipeline.safe_domain import gate_sql
from pipeline.bulk_fks import instrument

MODE="guarded_normalize"
PREFIX_MODES=('guarded_prefix','guarded_parallel')+CTAS_MODES+COPY_MODES
MODES=(MODE,)+PREFIX_MODES
EXTRA_DATA_FUNCTIONS=('bool_and','textcat','textregexeq','substr','octet_length','replace','array_remove','unnest')


def validation_destinations(destinations):
    return list(destinations)+[{'statements':[],'function_references':[('pg_catalog',name) for name in EXTRA_DATA_FUNCTIONS]}]


def conditional_sql(sql):
    return "DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN\n"+sql+"\nEND IF; END $guarded_bulk$;"


def prepare_sql(scopes,mode=MODE):
    if mode in ('guarded_ctas',)+COPY_MODES:
        from pipeline.workspace import prepare_sql as workspace_sql,RELATION
        return workspace_sql(scopes)+'\n'+conditional_sql("PERFORM pg_catalog.set_config('work_mem','64MB',true); ANALYZE "+RELATION+"(raw_id,text_column_1,text_column_2,text_column_3);")
    if mode in ('guarded_parallel',)+CTAS_MODES:
        from pipeline.parallel_normalization import prepare_sql as parallel_sql
        relation='challenge_lab.pipeline_normalized_input' if mode in CTAS_MODES else 'pg_temp.pipeline_normalized_input'
        return parallel_sql(scopes,logged=mode in CTAS_MODES)+'\n'+conditional_sql("PERFORM pg_catalog.set_config('work_mem','64MB',true); ANALYZE "+relation+"(raw_id,text_column_1,text_column_2,text_column_3);")
    empty=" AND ".join(f'NOT EXISTS(SELECT 1 FROM public.{table})' for table in ORDER)
    domain="CREATE TEMP TABLE pipeline_normalization_domain ON COMMIT DROP AS SELECT g.safe AND "+empty+" AS safe FROM ("+gate_sql()+") g;"
    index="CREATE INDEX pipeline_normalized_input_raw_id_idx ON pg_temp.pipeline_normalized_input(raw_id);" if mode!= 'guarded_prefix' else ''
    analyzed='raw_id,text_column_1,text_column_2,text_column_3'
    if mode!='guarded_prefix':analyzed+=','+','.join(f'integer_column_{n}' for n in range(2,16))
    return normalize_sql(scopes,all_tables=True,defer_index=True)+"\n"+domain+"\n"+conditional_sql("""
      PERFORM pg_catalog.set_config('work_mem','64MB',true);
      """+index+"\nANALYZE pg_temp.pipeline_normalized_input("+analyzed+""");
    """)


def fast_statement(statement,scopes,mode=MODE):
    if mode in CTAS_MODES+COPY_MODES:
        from pipeline.workspace import RELATION
        relation=RELATION if mode in ('guarded_ctas',)+COPY_MODES else 'challenge_lab.pipeline_normalized_input'
        result=fast_statement(statement,scopes,'guarded_prefix').replace('pg_temp.pipeline_normalized_input',relation)
        if mode in BULK_CTAS_MODES and statement.startswith('INSERT INTO public.table2 ('):
            start='CROSS JOIN LATERAL (SELECT position_number FROM pg_catalog.unnest('
            end=') AS expanded(position_number) OFFSET 0) AS position_keys'
            if result.count(start)!=1 or result.count(end)!=1:raise RuntimeError('Unknown position barrier')
            result=result.replace(start,'CROSS JOIN LATERAL pg_catalog.unnest(',1).replace(end,') AS position_keys(position_number)',1)
        return result
    result=rewrite(statement,scopes,all_tables=True)
    if not statement.startswith('INSERT INTO public.table2 ('):return result
    if mode in PREFIX_MODES:
        parent=re.search(r'JOIN public\.table1 AS main_row\s+ON main_row\.id_column_3 = source_row\.raw_id',result)
        anchor='FROM pg_temp.pipeline_normalized_input AS source_row'
        if not parent or result.count(anchor)!=1:raise RuntimeError('Unknown protected parent join')
        parent_sql=parent[0]
        result=result[:parent.start()]+result[parent.end():]
        result=result.replace(anchor,anchor+'\n'+parent_sql,1)
    matrix=re.search(r'CROSS JOIN LATERAL \(\s*VALUES\s*(.*?)\s*\) AS position_data\([^)]*\)',result,re.S)
    if not matrix:raise RuntimeError('Unknown guarded position matrix')
    fields=EXTRACT.findall(statement)
    # Fields repeat in SELECT/WHERE; the matrix itself has the exact 56 inputs.
    original_matrix=re.search(r'CROSS JOIN LATERAL \(\s*VALUES\s*(.*?)\s*\) AS position_data\([^)]*\)',statement,re.S)
    fields=EXTRACT.findall(original_matrix[1])
    if len(fields)!=56:raise RuntimeError('Unknown guarded position fields')
    matches=[f"CASE WHEN NULLIF(BTRIM(source_row.{fields[4*(n-1)]}), '')::smallint={n} THEN {n}::smallint END" for n in range(1,15)]
    # The barrier keeps numeric/type filters outside this subquery. Memoize can
    # then key solely on declared positions, rather than each unique payload.
    positions="pg_catalog.array_remove(ARRAY["+",".join(matches)+"],NULL::smallint)"
    if mode in PREFIX_MODES:
        # table1.integer_column_1 is the original GREATEST of all 14 declared
        # positions, built earlier in this exact, initially empty six-table
        # transaction. Any matching positive position is <= that maximum.
        # Original declared/type/weight predicates remain below. This bound
        # also makes the parent join precede expansion, avoiding repeated probes.
        positions="(ARRAY["+",".join(f'{n}::smallint' for n in range(1,15))+"])[1:LEAST(COALESCE(main_row.integer_column_1,0),14)]"
    expansion="CROSS JOIN LATERAL (SELECT position_number FROM pg_catalog.unnest("+positions+") AS expanded(position_number) OFFSET 0) AS position_keys"
    result=result[:matrix.start()]+expansion+result[matrix.end():]
    for attribute,column in enumerate(('declared_number_text','type_text','weight_text','gap_text')):
        choices="CASE position_keys.position_number "+" ".join(f"WHEN {n} THEN source_row.{fields[4*(n-1)+attribute]}" for n in range(1,15))+" END"
        result=result.replace('position_data.'+column,'('+choices+')')
    result=result.replace('position_data.position_number','position_keys.position_number')
    if 'position_data' in result:raise RuntimeError('Unrecognized guarded position reference')
    return result


def insert_sql(statement,scopes,mode=MODE):
    return "IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN\n"+fast_statement(statement,scopes,mode)+";\nELSE\n"+statement+";\nEND IF"


class GuardedNormalizationMixin:
    guarded_normalization=False
    capture_batch_sql=False

    def execute_cross_table_expansion(self,*args,**kwargs):
        if self.guarded_normalization and self.bulk_recipe_mode.startswith('lab_'):
            raise RuntimeError('Lab-only CTAS architecture requires bench.run; native counter/version integration is not qualified')
        if self.guarded_normalization and self.bulk_recipe_mode in COPY_MODES and not getattr(self,'supports_private_psql_workspace',False):
            raise RuntimeError('Native COPY requires the private psql workspace transport; local verification is available, SSH transport is not qualified yet')
        result=super().execute_cross_table_expansion(*args,**kwargs)
        if self.guarded_normalization:
            started=time.thread_time()
            mode=self.bulk_recipe_mode
            result['execution_architecture']={'mode':mode,'compiler_sha256':compiler_sha256(),
                'batch_sql_sha256':getattr(self._expansion_timing_local,'guarded_batch_sha256',None)}
            architecture=result['execution_architecture']
            if mode in COPY_MODES:architecture['copy_workspace']=getattr(self._expansion_timing_local,'copy_workspace_result',None)
            decisions=[item.get('rows') for item in (result.get('timing_report') or {}).get('items',[]) if item.get('name')=='Build TEXT input and prove safe cast domain']
            architecture['safe_domain']=bool(decisions[0]) if len(decisions)==1 and decisions[0] in (0,1) else None
            provenance='\n\n-- pipeline '+mode+'_v1 compiler_sha256='+architecture['compiler_sha256']+' batch_sql_sha256='+architecture['batch_sql_sha256']+' safe_domain='+str(architecture['safe_domain']).lower()
            for destination in result['destinations']:
                # The canonical recipe remains replayable by the original app;
                # persist the exact compiler/batch identity in version history.
                destination['sql_recipe']=destination['sql_recipe'].rstrip()+provenance
            self._append_expansion_timing(result.get('timing_report'),'prepare','Attach compiler provenance',time.thread_time()-started,'local_python_cpu')
        return result

    def _validate_cross_table_expansion_functions(self,database_name,destinations,cancel_event=None):
        if self.guarded_normalization:destinations=validation_destinations(destinations)
        return super()._validate_cross_table_expansion_functions(database_name,destinations,cancel_event=cancel_event)

    def run_remote_command(self,command,**kwargs):
        text=kwargs.get('stdin_text')
        if self.guarded_normalization and getattr(self._expansion_timing_local,'bulk_fk_active',False) and isinstance(text,str) and 'BEGIN ISOLATION LEVEL REPEATABLE READ;' in text and 'pgdm_cross_table_hash_manifest' in text:
            started=time.thread_time()
            from pipeline.upstream import PostgresAdminService
            source,scopes,selected=self._expansion_timing_local.bulk_selection_context
            selection=PostgresAdminService._prepare_cross_table_expansion(source,scopes,selected)
            scopes=selection['raw_schemas']
            marker="INSERT INTO pg_temp.pgdm_expansion_timings VALUES\n      (4,'bulk_load'"
            if text.count(marker)!=1:raise RuntimeError('Unknown bulk domain boundary')
            proof_sql=prepare_sql(scopes,self.bulk_recipe_mode)+"\nUPDATE pg_temp.pgdm_expansion_timings SET rows_affected=(SELECT safe::integer FROM pg_temp.pipeline_normalization_domain) WHERE sequence_number=5;"
            if self.bulk_recipe_mode in COPY_MODES:
                from pipeline.native_copy import prepare_receipts_sql
                proof_sql+='\n'+prepare_receipts_sql()
            text=text.replace(marker,instrument(proof_sql,5,'Build TEXT input and prove safe cast domain')+'\n'+marker,1)
            for position,d in enumerate(selection['destinations']):
                statement=d['statements'][0]
                managed=self._build_managed_dml_sql(statement,d['schema_name'],d['pure_table_name'],'add',timing_sequence=10+position,
                    timing_stage='execute',timing_name=f"Insert into {d['table_name']} (statement 1)",timing_destination=d['table_name'],timing_statement_index=1)
                if text.count(managed)!=1 or managed.count(statement)!=1:raise RuntimeError('Unknown guarded normalization DML boundary')
                if self.bulk_recipe_mode in COPY_MODES:
                    from pipeline.native_copy import copy_sql,managed_receipt_sql
                    table=d['pure_table_name']
                    replacement=instrument(copy_sql(fast_statement(statement,scopes,self.bulk_recipe_mode),table),200+int(table[-1]),'Transform and COPY '+table+' with native row receipt')+'\n'+managed_receipt_sql(managed,statement,table)
                elif self.bulk_recipe_mode in CTAS_MODES:
                    from pipeline.parallel_normalization import materialize_statement
                    table=d['pure_table_name']
                    prep,load,cleanup=materialize_statement(fast_statement(statement,scopes,self.bulk_recipe_mode),mode=self.bulk_recipe_mode)
                    replacement=instrument(conditional_sql(prep),200+int(table[-1]),'Parallel transform '+table)+'\n'+managed.replace(statement,"IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN\n"+load+';\nELSE\n'+statement+';\nEND IF',1)+'\n'+instrument(conditional_sql(cleanup),400+int(table[-1]),'Drop ready '+table+' input')
                else:replacement=managed.replace(statement,insert_sql(statement,scopes,self.bulk_recipe_mode),1)
                if self.bulk_recipe_mode in PREFIX_MODES and d['pure_table_name'] in ('table3','table4'):
                    table=d['pure_table_name']
                    replacement+='\n'+instrument(conditional_sql('ANALYZE public.'+table+';'),300+int(table[-1]),'Analyze fresh '+table+' dimension')
                text=text.replace(managed,replacement,1)
            if self.bulk_recipe_mode in ('guarded_ctas',)+COPY_MODES:
                from pipeline.workspace import cleanup_sql
                commit_marker="INSERT INTO pg_temp.pgdm_expansion_timings VALUES (9000, 'execute'"
                if text.count(commit_marker)!=1:raise RuntimeError('Unknown workspace cleanup boundary')
                text=text.replace(commit_marker,instrument(cleanup_sql(),8500,'Remove transaction-owned shared workspace')+'\n'+commit_marker,1)
            kwargs['stdin_text']=text
            self._expansion_timing_local.guarded_batch_sha256=hashlib.sha256(text.encode()).hexdigest()
            if self.capture_batch_sql:
                from pipeline.recipes import ROOT
                directory=ROOT/'reports'/'sql';directory.mkdir(parents=True,exist_ok=True)
                (directory/(self._expansion_timing_local.guarded_batch_sha256+'.sql')).write_text(text,encoding='utf-8')
            context=getattr(self._expansion_timing_local,'context',None)
            if context:
                self._append_expansion_timing(context.get('report'),'prepare','Build guarded TEXT normalization strategy',time.thread_time()-started,'local_python_cpu')
                if isinstance(context.get('report'),dict):context['report']['session_settings']={'work_mem':'64MB only inside proven safe domain; original value on fallback'}
        return super().run_remote_command(command,**kwargs)
