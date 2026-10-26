"""Build mathematically unique recipe keys in bulk; keep all primary keys.

The exact recipes cannot duplicate table2(main_id, position): source raw_id is
unique, table1(raw_id) stays unique until its own rebuild, and positions 1..14
occur once. Table1(raw_id) is unique because the DISTINCT dimension key matches
all its join columns under the same default collations. Unsupported metadata
retains the original index rather than taking this fast path.
"""
from pipeline.bulk_fks import instrument
import time


def prepare_primary_sql(table):
    if table not in ("table1","table2","table5","table6"):raise ValueError(table)
    # The exact recipes omit this identity column. A noncycling native sequence
    # produces distinct keys or raises its original exhaustion error. NOT NULL
    # remains active while only the backing primary-key index is rebuilt.
    return f"""DO $primary_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF NOT EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_sequence s
         ON s.seqrelid=pg_get_serial_sequence('public.{table}','id_column_1')::regclass
         WHERE a.attrelid='public.{table}'::regclass AND a.attname='id_column_1' AND a.attidentity IN ('a','d')
           AND a.attnotnull AND a.atttypid='bigint'::regtype AND NOT s.seqcycle AND s.seqtypid='bigint'::regtype)
      THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      SELECT k.conname,k.conkey,pg_get_constraintdef(k.oid) AS definition INTO item
        FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid JOIN pg_class x ON x.oid=i.indexrelid
        WHERE k.conrelid='public.{table}'::regclass AND k.conname='{table}_pkey' AND k.contype='p'
          AND k.convalidated AND NOT k.condeferrable AND i.indisvalid AND i.indisunique
          AND NOT i.indisclustered AND NOT i.indisreplident AND i.indnkeyatts=1 AND i.indnatts=1
          AND i.indexprs IS NULL AND i.indpred IS NULL AND x.reltablespace=0 AND x.reloptions IS NULL
          AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace
             AND opcname='int8_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree'));
      IF item.conname IS NULL OR item.definition<>'PRIMARY KEY (id_column_1)' OR EXISTS(
         SELECT 1 FROM pg_constraint k WHERE k.contype='f' AND k.confrelid='public.{table}'::regclass AND k.confkey=item.conkey)
      THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_unique_keys(table_name text,constraint_name text,definition text) ON COMMIT DROP;
      INSERT INTO pg_temp.pipeline_bulk_unique_keys VALUES('{table}',item.conname,item.definition);
      EXECUTE format('ALTER TABLE public.%I DROP CONSTRAINT %I','{table}',item.conname);
      IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.{table}'::regclass AND attname='id_column_1' AND attnotnull)
      THEN RAISE EXCEPTION 'Identity NOT NULL protection unexpectedly changed'; END IF;
      PERFORM set_config('search_path',previous_path,true);
    END $primary_prepare$;"""


def prepare_mode_sql(mode,table):
    from pipeline.recipes import bulk_unique_tables
    unique=prepare_sql(table) if table in bulk_unique_tables(mode) and table in ("table1","table2") else ""
    primary=prepare_primary_sql(table) if mode in ("bulk_full","bulk_full64","guarded_normalize","guarded_prefix") else ""
    return unique+"\n"+primary


def prepare_sql(table):
    if table=="table2":
        name="table2_id_column_2_integer_column_1_key";columns="id_column_2, integer_column_1";kinds=("int8_ops","int2_ops")
        column_test="a.attname='integer_column_1' AND a.atttypid='smallint'::regtype"
        dimension_test=""
    elif table=="table1":
        name="table1_id_column_3_key";columns="id_column_3";kinds=("int8_ops",)
        column_test="a.attname='id_column_3' AND a.atttypid='bigint'::regtype"
        dimension_test="""IF (SELECT count(*) FROM pg_attribute WHERE attrelid='public.table3'::regclass
          AND attname IN ('text_column_1','text_column_2','text_column_3','text_column_4')
          AND atttypid='varchar'::regtype AND atttypmod=-1 AND attcollation='default'::regcollation)<>4 THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;"""
    else:raise ValueError(table)
    opclasses=" AND ".join(f"i.indclass[{n}]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace AND opcname='{kind}' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree'))" for n,kind in enumerate(kinds))
    return f"""DO $index_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      {dimension_test}
      IF NOT EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid='public.{table}'::regclass AND {column_test}) THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      SELECT k.conname,k.conkey,k.conindid,pg_get_constraintdef(k.oid) AS definition INTO item
        FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid JOIN pg_class x ON x.oid=i.indexrelid
        WHERE k.conrelid='public.{table}'::regclass AND k.conname='{name}' AND k.contype='u'
          AND k.convalidated AND NOT k.condeferrable AND i.indisvalid AND i.indisunique
          AND NOT i.indisclustered AND NOT i.indisreplident
          AND i.indnkeyatts={len(kinds)} AND i.indnatts={len(kinds)} AND i.indexprs IS NULL AND i.indpred IS NULL
          AND x.reltablespace=0 AND x.reloptions IS NULL AND {opclasses};
      IF item.conname IS NULL OR item.definition<>'UNIQUE ({columns})' OR EXISTS(
         SELECT 1 FROM pg_constraint k WHERE k.contype='f' AND k.confrelid='public.{table}'::regclass AND k.confkey=item.conkey)
      THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_unique_keys(table_name text,constraint_name text,definition text) ON COMMIT DROP;
      INSERT INTO pg_temp.pipeline_bulk_unique_keys VALUES('{table}',item.conname,item.definition);
      EXECUTE format('ALTER TABLE public.%I DROP CONSTRAINT %I','{table}',item.conname);
      PERFORM set_config('search_path',previous_path,true);
    END $index_prepare$;"""


def restore_sql(table):
    if table not in ("table1","table2","table5","table6"):raise ValueError(table)
    return f"""DO $index_restore$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); previous_memory text:=pg_catalog.current_setting('maintenance_work_mem'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF to_regclass('pg_temp.pipeline_bulk_unique_keys') IS NULL THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      PERFORM set_config('maintenance_work_mem','128MB',true);
      FOR item IN SELECT * FROM pg_temp.pipeline_bulk_unique_keys WHERE table_name='{table}' LOOP
        EXECUTE format('ALTER TABLE public.%I ADD CONSTRAINT %I %s',item.table_name,item.constraint_name,item.definition);
        DELETE FROM pg_temp.pipeline_bulk_unique_keys WHERE table_name=item.table_name AND constraint_name=item.constraint_name;
      END LOOP;
      PERFORM set_config('maintenance_work_mem',previous_memory,true);
      PERFORM set_config('search_path',previous_path,true);
    END $index_restore$;"""


class BulkIndexMixin:
    bulk_unique_tables=()

    def run_remote_command(self,command,**kwargs):
        text=kwargs.get("stdin_text")
        if self.bulk_unique_tables and getattr(self._expansion_timing_local,"bulk_fk_active",False) and isinstance(text,str) and "BEGIN ISOLATION LEVEL REPEATABLE READ;" in text and "pgdm_cross_table_hash_manifest" in text:
            started=time.thread_time()
            from pipeline.upstream import PostgresAdminService
            context=getattr(self._expansion_timing_local,"bulk_selection_context",None)
            if not context:raise RuntimeError("Missing protected bulk index context")
            source,scopes,selected=context
            prepared=PostgresAdminService._prepare_cross_table_expansion(source,scopes,selected)["destinations"]
            for d in prepared:
                table=d["pure_table_name"]
                if table not in self.bulk_unique_tables:continue
                statement=d["statements"][0]
                managed=self._build_managed_dml_sql(statement,d["schema_name"],table,"add",timing_sequence=10+list(x['pure_table_name'] for x in prepared).index(table),
                    timing_stage="execute",timing_name=f"Insert into {d['table_name']} (statement 1)",timing_destination=d["table_name"],timing_statement_index=1)
                if text.count(managed)!=1:raise RuntimeError("Unknown managed index boundary")
                # Rebuild immediately after that table, before dependent recipes.
                text=text.replace(managed,instrument(self._guard_bulk_sql(prepare_mode_sql(self.bulk_recipe_mode,table)),100+10*int(table[-1]),f"Prepare {table} key bulk build")+"\n"+managed+"\n"+instrument(self._guard_bulk_sql(restore_sql(table)),101+10*int(table[-1]),f"Rebuild and validate {table} keys"),1)
            kwargs["stdin_text"]=text
            timing_context=getattr(self._expansion_timing_local,"context",None)
            if timing_context:
                self._append_expansion_timing(timing_context.get("report"),"prepare","Build protected bulk index strategy",time.thread_time()-started,"local_python_cpu")
        return super().run_remote_command(command,**kwargs)
