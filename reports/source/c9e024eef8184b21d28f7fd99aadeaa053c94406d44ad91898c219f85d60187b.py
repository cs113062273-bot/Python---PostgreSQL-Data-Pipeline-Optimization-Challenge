"""Narrow table1 planner tuning with original counter/timing instrumentation."""


def tune_setting_block(wrapped,setting,value):
    declaration="pgdm_affected_rows bigint;"
    diagnostics="GET DIAGNOSTICS pgdm_affected_rows = ROW_COUNT;"
    if wrapped.count(declaration)!=1 or wrapped.count(diagnostics)!=1 or wrapped.count("BEGIN\n")!=1:
        raise RuntimeError("Organizer managed-DML structure changed; refusing planner rewrite")
    if (setting,value) not in (("enable_mergejoin","off"),("enable_memoize","off")):
        raise ValueError("Unsupported planner setting")
    wrapped=wrapped.replace(declaration,declaration+f"\n    pipeline_previous_setting text := current_setting('{setting}');",1)
    wrapped=wrapped.replace("BEGIN\n",f"BEGIN\n    PERFORM set_config('{setting}','{value}',true);\n",1)
    return wrapped.replace(diagnostics,diagnostics+f"\n    PERFORM set_config('{setting}',pipeline_previous_setting,true);",1)


class Table1PlannerMixin:
    table1_hash_join=True
    table2_memoize=True
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
                context["report"]["session_settings"]={"work_mem":"64MB","enable_mergejoin":"off for table1 only","enable_memoize":"on" if self.table2_memoize else "off for table2 only"}
        return super().run_remote_command(command,**kwargs)

    @classmethod
    def _build_managed_dml_sql(cls,statement,schema_name,table_name,action,*args,**kwargs):
        wrapped=super()._build_managed_dml_sql(statement,schema_name,table_name,action,*args,**kwargs)
        if cls.table1_hash_join and schema_name=="public" and table_name=="table1" and action=="add" and "source_row.raw_id" in statement:
            return tune_setting_block(wrapped,"enable_mergejoin","off")
        if not cls.table2_memoize and schema_name=="public" and table_name=="table2" and action=="add" and "source_row.raw_id" in statement:
            return tune_setting_block(wrapped,"enable_memoize","off")
        return wrapped
