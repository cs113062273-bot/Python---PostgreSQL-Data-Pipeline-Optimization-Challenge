"""Transactional native identity-cache tuning, never manual ID assignment."""
TABLES=('table1','table2','table5','table6')


def prepare_sql(table):
    if table not in TABLES:raise ValueError(table)
    return f"""DO $sequence_cache_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      SELECT s.seqrelid,s.seqcache INTO item FROM pg_sequence s JOIN pg_attribute a
        ON a.attrelid='public.{table}'::regclass AND a.attname='id_column_1'
      WHERE s.seqrelid=pg_get_serial_sequence('public.{table}','id_column_1')::regclass
        AND a.attidentity IN ('a','d') AND a.atttypid='bigint'::regtype
        AND s.seqtypid='bigint'::regtype AND NOT s.seqcycle AND s.seqincrement=1 AND s.seqcache=1;
      IF item.seqrelid IS NOT NULL THEN
        CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_sequence_settings(table_name text,sequence_oid oid,cache_size bigint) ON COMMIT DROP;
        INSERT INTO pg_temp.pipeline_bulk_sequence_settings VALUES('{table}',item.seqrelid,item.seqcache);
        EXECUTE format('ALTER SEQUENCE %s CACHE 1000',item.seqrelid::regclass);
      END IF;
      PERFORM pg_catalog.set_config('search_path',previous_path,true);
    END $sequence_cache_prepare$;"""


def restore_sql(table):
    if table not in TABLES:raise ValueError(table)
    return f"""DO $sequence_cache_restore$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF to_regclass('pg_temp.pipeline_bulk_sequence_settings') IS NOT NULL THEN
        FOR item IN SELECT * FROM pg_temp.pipeline_bulk_sequence_settings WHERE table_name='{table}' LOOP
          EXECUTE format('ALTER SEQUENCE %s CACHE %s',item.sequence_oid::regclass,item.cache_size);
          DELETE FROM pg_temp.pipeline_bulk_sequence_settings WHERE table_name=item.table_name;
        END LOOP;
      END IF;
      PERFORM pg_catalog.set_config('search_path',previous_path,true);
    END $sequence_cache_restore$;"""
