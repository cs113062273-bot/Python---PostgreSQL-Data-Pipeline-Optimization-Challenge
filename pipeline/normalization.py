"""Transaction-local TEXT extraction reused by only table1 and table2.

All JSON extraction is total: no casts, trimming, filtering or input rejection
occurs in the stage. Original expressions still perform every transformation.
The stage is created from the exact protected scope, never from stored answers.
"""
import time
from pipeline.recipes import EXTRACT, destinations
from pipeline.bulk_fks import instrument

MODES=("bulk_normalize","bulk_normalize64")


def source_sql(scopes):
    from utils import sql_literal
    from pipeline.upstream import PostgresAdminService
    scopes=PostgresAdminService._dedupe_non_empty_strings(scopes)
    return '(\n    SELECT *\n    FROM "public"."raw_data"\n    WHERE "raw_schema" IN ('+", ".join(sql_literal(s) for s in scopes)+')\n)'


def prepare_sql(scopes,*,all_tables=False,defer_index=False):
    fields=sorted({key for d in destinations() if all_tables or d['table_name'] in ('public.table1','public.table2') for key in EXTRACT.findall(d['sql'])})
    columns=",\n".join(f"projected.payload ->> '{key}' AS {key}" for key in fields)
    return """CREATE TEMP TABLE pipeline_normalized_input ON COMMIT DROP AS
      SELECT source_row.raw_id,"""+columns+"\nFROM "+source_sql(scopes)+""" AS source_row
      CROSS JOIN LATERAL (SELECT source_row.raw -> 'values' AS payload OFFSET 0) projected;"""+("" if defer_index else """
      ALTER TABLE pg_temp.pipeline_normalized_input ADD PRIMARY KEY(raw_id);
      ANALYZE pg_temp.pipeline_normalized_input(raw_id,text_column_1,text_column_2);""")


def rewrite(statement,scopes,*,all_tables=False):
    if not all_tables and not any(statement.startswith(f"INSERT INTO public.{t} (") for t in ("table1","table2")):return statement
    expected=source_sql(scopes)
    if statement.count(expected)!=1:raise RuntimeError("Unknown protected normalization source")
    result=EXTRACT.sub(lambda m:"source_row."+m[1],statement)
    if "source_row.raw " in result:raise RuntimeError("Unrecognized normalization extraction")
    return result.replace(expected,"pg_temp.pipeline_normalized_input",1)


class NormalizationMixin:
    normalize_input=False

    def run_remote_command(self,command,**kwargs):
        text=kwargs.get("stdin_text")
        if self.normalize_input and getattr(self._expansion_timing_local,"bulk_fk_active",False) and isinstance(text,str) and "BEGIN ISOLATION LEVEL REPEATABLE READ;" in text and "pgdm_cross_table_hash_manifest" in text:
            started=time.thread_time()
            from pipeline.upstream import PostgresAdminService
            source,scopes,selected=self._expansion_timing_local.bulk_selection_context
            selection=PostgresAdminService._prepare_cross_table_expansion(source,scopes,selected)
            prepared=selection["destinations"]
            scopes=selection["raw_schemas"]
            for position,d in enumerate(prepared):
                table=d['pure_table_name']
                if table not in ('table1','table2'):continue
                statement=d['statements'][0]
                managed=self._build_managed_dml_sql(statement,d['schema_name'],table,'add',timing_sequence=10+position,
                    timing_stage='execute',timing_name=f"Insert into {d['table_name']} (statement 1)",
                    timing_destination=d['table_name'],timing_statement_index=1)
                if text.count(managed)!=1 or managed.count(statement)!=1:raise RuntimeError("Unknown normalization DML boundary")
                replacement=managed.replace(statement,rewrite(statement,scopes),1)
                if table=='table1':replacement=instrument(prepare_sql(scopes),5,"Build and analyze transaction-local TEXT input")+"\n"+replacement
                text=text.replace(managed,replacement,1)
            kwargs['stdin_text']=text
            context=getattr(self._expansion_timing_local,'context',None)
            if context:self._append_expansion_timing(context.get('report'),'prepare','Build protected normalization strategy',time.thread_time()-started,'local_python_cpu')
        return super().run_remote_command(command,**kwargs)
