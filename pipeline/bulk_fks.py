"""Transactional bulk FK validation for the exact empty organizer schema.

All PK/UNIQUE/CHECK constraints remain active. The six known FK relationships
are guaranteed by the recipes' source/parent joins while all parents are locked.
They are rebuilt AND validated before the data transaction may commit.
"""
from pipeline.recipes import ORDER
from pipeline.upstream import APP
import re
import time

FOREIGN_KEYS=(
 ("table1","table1_id_column_3_fkey","FOREIGN KEY (id_column_3) REFERENCES public.raw_data(raw_id) ON UPDATE CASCADE ON DELETE CASCADE"),
 ("table1","table1_id_column_4_fkey","FOREIGN KEY (id_column_4) REFERENCES public.table3(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE"),
 ("table2","table2_id_column_2_fkey","FOREIGN KEY (id_column_2) REFERENCES public.table1(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE"),
 ("table5","table5_id_column_2_fkey","FOREIGN KEY (id_column_2) REFERENCES public.table1(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE"),
 ("table6","table6_id_column_2_fkey","FOREIGN KEY (id_column_2) REFERENCES public.table1(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE"),
 ("table6","table6_id_column_3_fkey","FOREIGN KEY (id_column_3) REFERENCES public.table4(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE"),
)


def prepare_sql():
    from utils import sql_literal
    values=",".join("("+",".join(sql_literal(s) for s in row)+")" for row in FOREIGN_KEYS)
    relations=",".join("'public."+t+"'::regclass" for t in ("raw_data",)+ORDER)
    empty_checks="\n".join(f"IF EXISTS(SELECT 1 FROM public.{t}) THEN RAISE EXCEPTION 'Bulk FK mode requires empty destinations'; END IF;" for t in ORDER)
    source=(APP/"generate_database.py").read_text(encoding="utf-8")
    column_checks=[]
    for table in ("raw_data",)+ORDER:
        match=re.search(r"CREATE TABLE public\."+table+r"\s*\((.*?)\n\)",source,re.S)
        if not match:raise RuntimeError("Unrecognized organizer schema source")
        columns=[m[1] for line in match[1].splitlines() if (m:=re.match(r"\s+([a-z][a-z_0-9]*)\s+",line))]
        expected="ARRAY["+",".join(sql_literal(c) for c in columns)+"]::text[]"
        column_checks.append("IF (SELECT array_agg(attname::text ORDER BY attnum) FROM pg_attribute WHERE attrelid='public."+table+"'::regclass AND attnum>0 AND NOT attisdropped) IS DISTINCT FROM "+expected+" THEN RAISE EXCEPTION 'Unsupported bulk column layout: "+table+"'; END IF;")
    key_types=[("raw_data","raw_id","bigint"),("table1","id_column_1","bigint"),("table1","id_column_3","bigint"),("table1","id_column_4","bigint"),("table1","integer_column_1","smallint"),
               ("table3","id_column_1","bigint"),("table4","id_column_1","smallint"),("table2","id_column_2","bigint"),("table5","id_column_2","bigint"),
               ("table6","id_column_2","bigint"),("table6","id_column_3","smallint")]
    for table,column,kind in key_types:
        column_checks.append(f"IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.{table}'::regclass AND attname='{column}' AND atttypid='{kind}'::regtype) THEN RAISE EXCEPTION 'Unsupported bulk relationship type'; END IF;")
    # Relationship/uniqueness proofs rely on native integer equality, not a
    # user-defined B-tree operator class attached to an otherwise familiar PK.
    for table in ("raw_data",)+ORDER:
        column="raw_id" if table=="raw_data" else "id_column_1"
        kind="int2_ops" if table=="table4" else "int8_ops"
        column_checks.append(f"""IF NOT EXISTS(SELECT 1 FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid
          JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attname='{column}'
          WHERE k.conrelid='public.{table}'::regclass AND k.contype='p' AND k.convalidated AND NOT k.condeferrable
          AND k.conkey=ARRAY[a.attnum]::smallint[] AND i.indisvalid AND i.indisunique
          AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace
             AND opcname='{kind}' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree')))
          THEN RAISE EXCEPTION 'Unsupported bulk primary-key equality: {table}'; END IF;""")
    for table in ("table1","table2","table3","table5","table6"):
        column_checks.append(f"IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.{table}'::regclass AND attname='id_column_1' AND attidentity IN ('a','d')) THEN RAISE EXCEPTION 'Unsupported bulk identity default'; END IF;")
    checks=(
        "CHECK (((text_column_1 IS NULL) OR ((text_column_1)::text = ANY ((ARRAY['X'::character varying, 'Y'::character varying, 'Z'::character varying])::text[]))))",
        "CHECK (((text_column_1 IS NULL) OR ((text_column_1)::text = ANY (ARRAY[('X'::character varying)::text, ('Y'::character varying)::text, ('Z'::character varying)::text]))))",
    )
    allowed_checks=",".join(sql_literal(c) for c in checks)
    return """DO $bulk_prepare$
DECLARE item record; actual text; previous_path text:=pg_catalog.current_setting('search_path');
BEGIN
 PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
 LOCK TABLE public.raw_data IN SHARE MODE;
 IF EXISTS(SELECT 1 FROM pg_event_trigger WHERE evtenabled<>'D') THEN
   RAISE EXCEPTION 'Bulk FK mode does not support database event triggers';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_class WHERE oid IN ("""+relations+""") AND (relkind<>'r' OR relispartition OR relrowsecurity))
 OR EXISTS(SELECT 1 FROM pg_inherits WHERE inhparent IN ("""+relations+""") OR inhrelid IN ("""+relations+""")) THEN
   RAISE EXCEPTION 'Bulk FK mode requires ordinary non-inherited non-RLS tables';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_constraint k JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attname='raw_id'
   WHERE k.conrelid='public.raw_data'::regclass AND k.contype='p' AND k.convalidated AND NOT k.condeferrable
   AND k.conkey=ARRAY[a.attnum]::smallint[] AND a.atttypid='bigint'::regtype AND a.attnotnull) THEN
   RAISE EXCEPTION 'Bulk FK mode requires the organizer raw_id primary key';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid IN ("""+relations+""") AND NOT tgisinternal) THEN
   RAISE EXCEPTION 'Bulk FK mode does not support additional user triggers';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_constraint k WHERE k.conrelid IN ("""+relations+""") AND k.contype='c'
   AND NOT (k.conrelid='public.table2'::regclass AND k.conname='table2_text_column_1_check' AND k.convalidated
            AND pg_get_constraintdef(k.oid) IN ("""+allowed_checks+"""))) THEN
   RAISE EXCEPTION 'Bulk FK mode requires the builtin organizer CHECK constraints';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid IN ("""+relations+""") AND attnum>0 AND NOT attisdropped
   AND (atttypid NOT IN ('bigint'::regtype,'smallint'::regtype,'integer'::regtype,'text'::regtype,
        'varchar'::regtype,'numeric'::regtype,'timestamp'::regtype,'timestamptz'::regtype,'jsonb'::regtype)
        OR attgenerated<>'')) THEN
   RAISE EXCEPTION 'Bulk FK mode requires builtin organizer column types';
 END IF;
 """+"\n".join(column_checks)+"\n"+empty_checks+"""
 CREATE TEMP TABLE pipeline_bulk_foreign_keys(table_name text,constraint_name text,definition text) ON COMMIT DROP;
 PERFORM set_config('search_path','pg_catalog',true);
 FOR item IN SELECT * FROM (VALUES """+values+""") v(table_name,constraint_name,definition) LOOP
   SELECT pg_get_constraintdef(k.oid) INTO actual FROM pg_constraint k
   WHERE k.conrelid=('public.'||item.table_name)::regclass AND k.conname=item.constraint_name
     AND k.contype='f' AND k.convalidated AND NOT k.condeferrable AND NOT k.condeferred;
   IF actual IS DISTINCT FROM item.definition THEN RAISE EXCEPTION 'Unsupported bulk FK definition: %',item.constraint_name; END IF;
   INSERT INTO pg_temp.pipeline_bulk_foreign_keys VALUES(item.table_name,item.constraint_name,actual);
 END LOOP;
 FOR item IN SELECT * FROM pg_temp.pipeline_bulk_foreign_keys LOOP
   EXECUTE format('ALTER TABLE public.%I DROP CONSTRAINT %I',item.table_name,item.constraint_name);
 END LOOP;
 PERFORM set_config('search_path',previous_path,true);
END $bulk_prepare$;"""


def restore_sql():
    return """DO $bulk_restore$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
 PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
 IF (SELECT count(*) FROM pg_temp.pipeline_bulk_foreign_keys)<>6 THEN RAISE EXCEPTION 'Incomplete bulk FK restoration plan'; END IF;
 FOR item IN SELECT * FROM pg_temp.pipeline_bulk_foreign_keys ORDER BY table_name,constraint_name LOOP
   EXECUTE format('ALTER TABLE public.%I ADD CONSTRAINT %I %s',item.table_name,item.constraint_name,item.definition);
 END LOOP;
 IF EXISTS(SELECT 1 FROM pg_constraint WHERE connamespace='public'::regnamespace AND NOT convalidated) THEN
   RAISE EXCEPTION 'Bulk FK validation did not complete';
 END IF;
 PERFORM pg_catalog.set_config('search_path',previous_path,true);
END $bulk_restore$;"""


def instrument(sql,sequence,name):
    return f"""INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      ({sequence},'bulk_load','{name}',clock_timestamp(),NULL,NULL,NULL,NULL);
      {sql}
      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number={sequence};"""


class BulkFKMixin:
    bulk_foreign_keys=False
    bulk_recipe_mode="baseline"
    guarded_normalization=False

    def _guard_bulk_sql(self,sql):
        if self.guarded_normalization:
            from pipeline.guarded_normalization import conditional_sql
            return conditional_sql(sql)
        return sql

    def execute_cross_table_expansion(self,database_name,source_full_table_name,raw_schemas,destinations,**kwargs):
        if self.bulk_foreign_keys:
            from pipeline.recipes import destinations as recipe_destinations
            originals=recipe_destinations(self.bulk_recipe_mode)
            if source_full_table_name!="public.raw_data" or len(destinations)!=len(originals) or any(
                d.get("table_name")!=o["table_name"] or d.get("sql","").strip()!=o["sql"].strip() for d,o in zip(destinations,originals)):
                raise RuntimeError("Bulk FK mode requires the exact organizer source and six original recipes")
        previous=getattr(self._expansion_timing_local,"bulk_fk_active",False)
        previous_context=getattr(self._expansion_timing_local,"bulk_selection_context",None)
        self._expansion_timing_local.bulk_fk_active=self.bulk_foreign_keys
        self._expansion_timing_local.bulk_selection_context=(source_full_table_name,raw_schemas,destinations)
        try:
            return super().execute_cross_table_expansion(database_name,source_full_table_name,raw_schemas,destinations,**kwargs)
        finally:
            self._expansion_timing_local.bulk_fk_active=previous
            self._expansion_timing_local.bulk_selection_context=previous_context

    def run_remote_command(self,command,**kwargs):
        text=kwargs.get("stdin_text")
        if self.bulk_foreign_keys and getattr(self._expansion_timing_local,"bulk_fk_active",False) and isinstance(text,str) and "BEGIN ISOLATION LEVEL REPEATABLE READ;" in text and "pgdm_cross_table_hash_manifest" in text:
            started=time.thread_time()
            begin="BEGIN ISOLATION LEVEL REPEATABLE READ;"
            boundary="clock_timestamp() WHERE sequence_number = 3;"
            if text.count(begin)!=1 or text.count(boundary)!=1 or text.count("COMMIT;")!=1:
                raise RuntimeError("Organizer expansion batch structure changed")
            # Lock BEFORE the first RR data snapshot. Source-lock wait is timed.
            text=text.replace(begin,"INSERT INTO pg_temp.pgdm_expansion_timings VALUES (0,'bulk_load','Acquire stable source lock',clock_timestamp(),NULL,NULL,NULL,NULL);\n"+begin+
                "\nLOCK TABLE public.raw_data IN SHARE MODE;\nUPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=0;",1)
            text=text.replace(boundary,boundary+"\n"+instrument(self._guard_bulk_sql(prepare_sql()),4,"Prepare transactional bulk FK load"),1)
            commit_marker="INSERT INTO pg_temp.pgdm_expansion_timings VALUES (9000, 'execute'"
            if text.count(commit_marker)!=1:raise RuntimeError("Unknown commit timing boundary")
            # Restoration has its own stage; keep it outside the commit timer.
            text=text.replace(commit_marker,instrument(self._guard_bulk_sql(restore_sql()),8000,"Restore and validate all bulk foreign keys")+"\n"+commit_marker,1)
            kwargs["stdin_text"]=text
            context=getattr(self._expansion_timing_local,"context",None)
            if context:
                self._append_expansion_timing(context.get("report"),"prepare","Build and validate bulk FK strategy",time.thread_time()-started,"local_python_cpu")
        return super().run_remote_command(command,**kwargs)
