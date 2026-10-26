"""Narrow table1 planner tuning with original counter/timing instrumentation."""


def tune_table1_block(wrapped):
    declaration="pgdm_affected_rows bigint;"
    diagnostics="GET DIAGNOSTICS pgdm_affected_rows = ROW_COUNT;"
    if wrapped.count(declaration)!=1 or wrapped.count(diagnostics)!=1 or wrapped.count("BEGIN\n")!=1:
        raise RuntimeError("Organizer managed-DML structure changed; refusing planner rewrite")
    wrapped=wrapped.replace(declaration,declaration+"\n    pipeline_previous_mergejoin text := current_setting('enable_mergejoin');",1)
    wrapped=wrapped.replace("BEGIN\n","BEGIN\n    PERFORM set_config('enable_mergejoin','off',true);\n",1)
    return wrapped.replace(diagnostics,diagnostics+"\n    PERFORM set_config('enable_mergejoin',pipeline_previous_mergejoin,true);",1)


class Table1PlannerMixin:
    table1_hash_join=True
    expansion_work_mem=None

    def run_remote_command(self,command,**kwargs):
        text=kwargs.get("stdin_text")
        if self.expansion_work_mem and isinstance(text,str) and "BEGIN ISOLATION LEVEL REPEATABLE READ;" in text and "pgdm_expansion_timings" in text:
            marker="SET LOCAL client_min_messages = warning;"
            if text.count(marker)!=1 or self.expansion_work_mem!="64MB":
                raise RuntimeError("Unsupported protected batch/settings structure")
            kwargs["stdin_text"]=text.replace(marker,marker+"\nSET LOCAL work_mem='64MB';",1)
            context=getattr(self._expansion_timing_local,"context",None)
            if context and isinstance(context.get("report"),dict):
                context["report"]["session_settings"]={"work_mem":"64MB","enable_mergejoin":"off for table1 only"}
        return super().run_remote_command(command,**kwargs)

    @classmethod
    def _build_managed_dml_sql(cls,statement,schema_name,table_name,action,*args,**kwargs):
        wrapped=super()._build_managed_dml_sql(statement,schema_name,table_name,action,*args,**kwargs)
        if cls.table1_hash_join and schema_name=="public" and table_name=="table1" and action=="add" and "source_row.raw_id" in statement:
            return tune_table1_block(wrapped)
        return wrapped
