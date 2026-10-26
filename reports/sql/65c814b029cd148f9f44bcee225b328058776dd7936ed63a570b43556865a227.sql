BEGIN ISOLATION LEVEL REPEATABLE READ;

LOCK TABLE public.raw_data IN SHARE MODE;

LOCK TABLE
    "public"."table1",
    "public"."table2",
    "public"."table3",
    "public"."table4",
    "public"."table5",
    "public"."table6"
IN SHARE ROW EXCLUSIVE MODE;

CREATE SCHEMA IF NOT EXISTS challenge_lab;
CREATE TEMP TABLE pipeline_parallel_settings ON COMMIT DROP AS SELECT name,setting FROM pg_settings WHERE name IN ('max_parallel_workers_per_gather','min_parallel_table_scan_size','parallel_setup_cost','parallel_tuple_cost');
SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;
CREATE TABLE challenge_lab.pipeline_normalized_input AS
      SELECT s.raw_id,n.*,(pg_catalog.octet_length(c.integers)-pg_catalog.octet_length(pg_catalog.replace(c.integers,'|',''))=14 AND (c.integers COLLATE "C") ~ E'\\A(?:[+-]?[0-9]{1,4})?(?:[|](?:[+-]?[0-9]{1,4})?)*\\Z') AND (pg_catalog.octet_length(c.numerics)-pg_catalog.octet_length(pg_catalog.replace(c.numerics,'|',''))=29 AND (c.numerics COLLATE "C") ~ E'\\A(?:[+-]?[0-9]{1,6}(?:[.][0-9]{1,3})?)?(?:[|](?:[+-]?[0-9]{1,6}(?:[.][0-9]{1,3})?)?)*\\Z') AND (c.identifier COLLATE "C") ~ E'\\A(?:[+-]?(?:[0-9]{1,9}|[01][0-9]{9}))?\\Z' AND (CASE WHEN c.stamp='' THEN true WHEN (c.stamp COLLATE "C") ~ E'\\A[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])[ T](?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:[.][0-9]{1,6})?(?:Z|[+-](?:0[0-9]|1[0-2])(?::?[0-5][0-9])?)?\\Z' THEN
      pg_catalog.substr(c.stamp,1,4)::integer>0 AND pg_catalog.substr(c.stamp,9,2)::integer<=CASE WHEN pg_catalog.substr(c.stamp,6,2)='02' THEN 28+CASE WHEN pg_catalog.substr(c.stamp,1,4)::integer%4=0 AND (pg_catalog.substr(c.stamp,1,4)::integer%100<>0 OR pg_catalog.substr(c.stamp,1,4)::integer%400=0) THEN 1 ELSE 0 END
      WHEN pg_catalog.substr(c.stamp,6,2) IN ('04','06','09','11') THEN 30 ELSE 31 END ELSE false END) AS pipeline_domain_safe
      FROM (
    SELECT *
    FROM "public"."raw_data"
    WHERE "raw_schema" IN ('group001')
) s
      CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[s.raw->'values']) p(payload)
      CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW(p.payload->>'datetime_column_1',p.payload->>'id_column_1',p.payload->>'integer_column_1',p.payload->>'integer_column_10',p.payload->>'integer_column_11',p.payload->>'integer_column_12',p.payload->>'integer_column_13',p.payload->>'integer_column_14',p.payload->>'integer_column_15',p.payload->>'integer_column_2',p.payload->>'integer_column_3',p.payload->>'integer_column_4',p.payload->>'integer_column_5',p.payload->>'integer_column_6',p.payload->>'integer_column_7',p.payload->>'integer_column_8',p.payload->>'integer_column_9',p.payload->>'numeric_column_1',p.payload->>'numeric_column_10',p.payload->>'numeric_column_11',p.payload->>'numeric_column_12',p.payload->>'numeric_column_13',p.payload->>'numeric_column_14',p.payload->>'numeric_column_15',p.payload->>'numeric_column_16',p.payload->>'numeric_column_17',p.payload->>'numeric_column_18',p.payload->>'numeric_column_19',p.payload->>'numeric_column_2',p.payload->>'numeric_column_20',p.payload->>'numeric_column_21',p.payload->>'numeric_column_22',p.payload->>'numeric_column_23',p.payload->>'numeric_column_24',p.payload->>'numeric_column_25',p.payload->>'numeric_column_26',p.payload->>'numeric_column_27',p.payload->>'numeric_column_28',p.payload->>'numeric_column_29',p.payload->>'numeric_column_3',p.payload->>'numeric_column_30',p.payload->>'numeric_column_4',p.payload->>'numeric_column_5',p.payload->>'numeric_column_6',p.payload->>'numeric_column_7',p.payload->>'numeric_column_8',p.payload->>'numeric_column_9',p.payload->>'text_column_1',p.payload->>'text_column_10',p.payload->>'text_column_11',p.payload->>'text_column_12',p.payload->>'text_column_13',p.payload->>'text_column_14',p.payload->>'text_column_15',p.payload->>'text_column_16',p.payload->>'text_column_17',p.payload->>'text_column_2',p.payload->>'text_column_3',p.payload->>'text_column_4',p.payload->>'text_column_5',p.payload->>'text_column_6',p.payload->>'text_column_7',p.payload->>'text_column_8',p.payload->>'text_column_9')]) n(datetime_column_1 text,id_column_1 text,integer_column_1 text,integer_column_10 text,integer_column_11 text,integer_column_12 text,integer_column_13 text,integer_column_14 text,integer_column_15 text,integer_column_2 text,integer_column_3 text,integer_column_4 text,integer_column_5 text,integer_column_6 text,integer_column_7 text,integer_column_8 text,integer_column_9 text,numeric_column_1 text,numeric_column_10 text,numeric_column_11 text,numeric_column_12 text,numeric_column_13 text,numeric_column_14 text,numeric_column_15 text,numeric_column_16 text,numeric_column_17 text,numeric_column_18 text,numeric_column_19 text,numeric_column_2 text,numeric_column_20 text,numeric_column_21 text,numeric_column_22 text,numeric_column_23 text,numeric_column_24 text,numeric_column_25 text,numeric_column_26 text,numeric_column_27 text,numeric_column_28 text,numeric_column_29 text,numeric_column_3 text,numeric_column_30 text,numeric_column_4 text,numeric_column_5 text,numeric_column_6 text,numeric_column_7 text,numeric_column_8 text,numeric_column_9 text,text_column_1 text,text_column_10 text,text_column_11 text,text_column_12 text,text_column_13 text,text_column_14 text,text_column_15 text,text_column_16 text,text_column_17 text,text_column_2 text,text_column_3 text,text_column_4 text,text_column_5 text,text_column_6 text,text_column_7 text,text_column_8 text,text_column_9 text)
CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW((((coalesce(n.integer_column_1,'')||'|'||(coalesce(n.integer_column_2,'')||'|'||coalesce(n.integer_column_3,'')))||'|'||((coalesce(n.integer_column_4,'')||'|'||coalesce(n.integer_column_5,''))||'|'||(coalesce(n.integer_column_6,'')||'|'||coalesce(n.integer_column_7,''))))||'|'||(((coalesce(n.integer_column_8,'')||'|'||coalesce(n.integer_column_9,''))||'|'||(coalesce(n.integer_column_10,'')||'|'||coalesce(n.integer_column_11,'')))||'|'||((coalesce(n.integer_column_12,'')||'|'||coalesce(n.integer_column_13,''))||'|'||(coalesce(n.integer_column_14,'')||'|'||coalesce(n.integer_column_15,''))))),((((coalesce(n.numeric_column_1,'')||'|'||(coalesce(n.numeric_column_2,'')||'|'||coalesce(n.numeric_column_3,'')))||'|'||((coalesce(n.numeric_column_4,'')||'|'||coalesce(n.numeric_column_5,''))||'|'||(coalesce(n.numeric_column_6,'')||'|'||coalesce(n.numeric_column_7,''))))||'|'||(((coalesce(n.numeric_column_8,'')||'|'||coalesce(n.numeric_column_9,''))||'|'||(coalesce(n.numeric_column_10,'')||'|'||coalesce(n.numeric_column_11,'')))||'|'||((coalesce(n.numeric_column_12,'')||'|'||coalesce(n.numeric_column_13,''))||'|'||(coalesce(n.numeric_column_14,'')||'|'||coalesce(n.numeric_column_15,'')))))||'|'||(((coalesce(n.numeric_column_16,'')||'|'||(coalesce(n.numeric_column_17,'')||'|'||coalesce(n.numeric_column_18,'')))||'|'||((coalesce(n.numeric_column_19,'')||'|'||coalesce(n.numeric_column_20,''))||'|'||(coalesce(n.numeric_column_21,'')||'|'||coalesce(n.numeric_column_22,''))))||'|'||(((coalesce(n.numeric_column_23,'')||'|'||coalesce(n.numeric_column_24,''))||'|'||(coalesce(n.numeric_column_25,'')||'|'||coalesce(n.numeric_column_26,'')))||'|'||((coalesce(n.numeric_column_27,'')||'|'||coalesce(n.numeric_column_28,''))||'|'||(coalesce(n.numeric_column_29,'')||'|'||coalesce(n.numeric_column_30,'')))))),coalesce(n.id_column_1,''),coalesce(n.datetime_column_1,''))]) c(integers text,numerics text,identifier text,stamp text);
DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;
CREATE TEMP TABLE pipeline_normalization_domain ON COMMIT DROP AS SELECT coalesce(bool_and(pipeline_domain_safe),true) AND NOT EXISTS(SELECT 1 FROM public.table3) AND NOT EXISTS(SELECT 1 FROM public.table4) AND NOT EXISTS(SELECT 1 FROM public.table1) AND NOT EXISTS(SELECT 1 FROM public.table5) AND NOT EXISTS(SELECT 1 FROM public.table6) AND NOT EXISTS(SELECT 1 FROM public.table2) AS safe FROM challenge_lab.pipeline_normalized_input;
DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
PERFORM pg_catalog.set_config('work_mem','64MB',true); ANALYZE challenge_lab.pipeline_normalized_input(raw_id,text_column_1,text_column_2,text_column_3);
END IF; END $guarded_bulk$;

DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
DO $bulk_prepare$
DECLARE item record; actual text; previous_path text:=pg_catalog.current_setting('search_path');
BEGIN
 PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
 LOCK TABLE public.raw_data IN SHARE MODE;
 IF EXISTS(SELECT 1 FROM pg_event_trigger WHERE evtenabled<>'D') THEN
   RAISE EXCEPTION 'Bulk FK mode does not support database event triggers';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_class WHERE oid IN ('public.raw_data'::regclass,'public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass) AND (relkind<>'r' OR relispartition OR relrowsecurity))
 OR EXISTS(SELECT 1 FROM pg_inherits WHERE inhparent IN ('public.raw_data'::regclass,'public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass) OR inhrelid IN ('public.raw_data'::regclass,'public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass)) THEN
   RAISE EXCEPTION 'Bulk FK mode requires ordinary non-inherited non-RLS tables';
 END IF;
 IF NOT EXISTS(SELECT 1 FROM pg_constraint k JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attname='raw_id'
   WHERE k.conrelid='public.raw_data'::regclass AND k.contype='p' AND k.convalidated AND NOT k.condeferrable
   AND k.conkey=ARRAY[a.attnum]::smallint[] AND a.atttypid='bigint'::regtype AND a.attnotnull) THEN
   RAISE EXCEPTION 'Bulk FK mode requires the organizer raw_id primary key';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid IN ('public.raw_data'::regclass,'public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass) AND NOT tgisinternal) THEN
   RAISE EXCEPTION 'Bulk FK mode does not support additional user triggers';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_constraint k WHERE k.conrelid IN ('public.raw_data'::regclass,'public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass) AND k.contype='c'
   AND NOT (k.conrelid='public.table2'::regclass AND k.conname='table2_text_column_1_check' AND k.convalidated
            AND pg_get_constraintdef(k.oid) IN ('CHECK (((text_column_1 IS NULL) OR ((text_column_1)::text = ANY ((ARRAY[''X''::character varying, ''Y''::character varying, ''Z''::character varying])::text[]))))','CHECK (((text_column_1 IS NULL) OR ((text_column_1)::text = ANY (ARRAY[(''X''::character varying)::text, (''Y''::character varying)::text, (''Z''::character varying)::text]))))'))) THEN
   RAISE EXCEPTION 'Bulk FK mode requires the builtin organizer CHECK constraints';
 END IF;
 IF EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid IN ('public.raw_data'::regclass,'public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass) AND attnum>0 AND NOT attisdropped
   AND (atttypid NOT IN ('bigint'::regtype,'smallint'::regtype,'integer'::regtype,'text'::regtype,
        'varchar'::regtype,'numeric'::regtype,'timestamp'::regtype,'timestamptz'::regtype,'jsonb'::regtype)
        OR attgenerated<>'')) THEN
   RAISE EXCEPTION 'Bulk FK mode requires builtin organizer column types';
 END IF;
 IF (SELECT array_agg(attname::text ORDER BY attnum) FROM pg_attribute WHERE attrelid='public.raw_data'::regclass AND attnum>0 AND NOT attisdropped) IS DISTINCT FROM ARRAY['raw_id','raw_hash','raw_ingested_at','raw_schema','raw_tab','raw']::text[] THEN RAISE EXCEPTION 'Unsupported bulk column layout: raw_data'; END IF;
IF (SELECT array_agg(attname::text ORDER BY attnum) FROM pg_attribute WHERE attrelid='public.table3'::regclass AND attnum>0 AND NOT attisdropped) IS DISTINCT FROM ARRAY['id_column_1','text_column_1','text_column_2','numeric_column_1','text_column_3','text_column_4']::text[] THEN RAISE EXCEPTION 'Unsupported bulk column layout: table3'; END IF;
IF (SELECT array_agg(attname::text ORDER BY attnum) FROM pg_attribute WHERE attrelid='public.table4'::regclass AND attnum>0 AND NOT attisdropped) IS DISTINCT FROM ARRAY['id_column_1','text_column_1','text_column_2']::text[] THEN RAISE EXCEPTION 'Unsupported bulk column layout: table4'; END IF;
IF (SELECT array_agg(attname::text ORDER BY attnum) FROM pg_attribute WHERE attrelid='public.table1'::regclass AND attnum>0 AND NOT attisdropped) IS DISTINCT FROM ARRAY['id_column_1','id_column_2','text_column_1','datetime_column_1','text_column_2','integer_column_1','integer_column_2','integer_column_3','text_column_3','id_column_3','id_column_4','id_column_5','integer_column_4','integer_column_5','id_column_6','integer_column_6','integer_column_7']::text[] THEN RAISE EXCEPTION 'Unsupported bulk column layout: table1'; END IF;
IF (SELECT array_agg(attname::text ORDER BY attnum) FROM pg_attribute WHERE attrelid='public.table5'::regclass AND attnum>0 AND NOT attisdropped) IS DISTINCT FROM ARRAY['id_column_1','numeric_column_1','numeric_column_2','integer_column_1','integer_column_2','id_column_2','id_column_3']::text[] THEN RAISE EXCEPTION 'Unsupported bulk column layout: table5'; END IF;
IF (SELECT array_agg(attname::text ORDER BY attnum) FROM pg_attribute WHERE attrelid='public.table6'::regclass AND attnum>0 AND NOT attisdropped) IS DISTINCT FROM ARRAY['id_column_1','id_column_2','id_column_3']::text[] THEN RAISE EXCEPTION 'Unsupported bulk column layout: table6'; END IF;
IF (SELECT array_agg(attname::text ORDER BY attnum) FROM pg_attribute WHERE attrelid='public.table2'::regclass AND attnum>0 AND NOT attisdropped) IS DISTINCT FROM ARRAY['id_column_1','id_column_2','integer_column_1','integer_column_2','text_column_1','integer_column_3','integer_column_4','integer_column_5','integer_column_6']::text[] THEN RAISE EXCEPTION 'Unsupported bulk column layout: table2'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.raw_data'::regclass AND attname='raw_id' AND atttypid='bigint'::regtype) THEN RAISE EXCEPTION 'Unsupported bulk relationship type'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table1'::regclass AND attname='id_column_1' AND atttypid='bigint'::regtype) THEN RAISE EXCEPTION 'Unsupported bulk relationship type'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table1'::regclass AND attname='id_column_3' AND atttypid='bigint'::regtype) THEN RAISE EXCEPTION 'Unsupported bulk relationship type'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table1'::regclass AND attname='id_column_4' AND atttypid='bigint'::regtype) THEN RAISE EXCEPTION 'Unsupported bulk relationship type'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table1'::regclass AND attname='integer_column_1' AND atttypid='smallint'::regtype) THEN RAISE EXCEPTION 'Unsupported bulk relationship type'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table3'::regclass AND attname='id_column_1' AND atttypid='bigint'::regtype) THEN RAISE EXCEPTION 'Unsupported bulk relationship type'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table4'::regclass AND attname='id_column_1' AND atttypid='smallint'::regtype) THEN RAISE EXCEPTION 'Unsupported bulk relationship type'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table2'::regclass AND attname='id_column_2' AND atttypid='bigint'::regtype) THEN RAISE EXCEPTION 'Unsupported bulk relationship type'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table5'::regclass AND attname='id_column_2' AND atttypid='bigint'::regtype) THEN RAISE EXCEPTION 'Unsupported bulk relationship type'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table6'::regclass AND attname='id_column_2' AND atttypid='bigint'::regtype) THEN RAISE EXCEPTION 'Unsupported bulk relationship type'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table6'::regclass AND attname='id_column_3' AND atttypid='smallint'::regtype) THEN RAISE EXCEPTION 'Unsupported bulk relationship type'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid
          JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attname='raw_id'
          WHERE k.conrelid='public.raw_data'::regclass AND k.contype='p' AND k.convalidated AND NOT k.condeferrable
          AND k.conkey=ARRAY[a.attnum]::smallint[] AND i.indisvalid AND i.indisunique
          AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace
             AND opcname='int8_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree')))
          THEN RAISE EXCEPTION 'Unsupported bulk primary-key equality: raw_data'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid
          JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attname='id_column_1'
          WHERE k.conrelid='public.table3'::regclass AND k.contype='p' AND k.convalidated AND NOT k.condeferrable
          AND k.conkey=ARRAY[a.attnum]::smallint[] AND i.indisvalid AND i.indisunique
          AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace
             AND opcname='int8_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree')))
          THEN RAISE EXCEPTION 'Unsupported bulk primary-key equality: table3'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid
          JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attname='id_column_1'
          WHERE k.conrelid='public.table4'::regclass AND k.contype='p' AND k.convalidated AND NOT k.condeferrable
          AND k.conkey=ARRAY[a.attnum]::smallint[] AND i.indisvalid AND i.indisunique
          AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace
             AND opcname='int2_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree')))
          THEN RAISE EXCEPTION 'Unsupported bulk primary-key equality: table4'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid
          JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attname='id_column_1'
          WHERE k.conrelid='public.table1'::regclass AND k.contype='p' AND k.convalidated AND NOT k.condeferrable
          AND k.conkey=ARRAY[a.attnum]::smallint[] AND i.indisvalid AND i.indisunique
          AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace
             AND opcname='int8_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree')))
          THEN RAISE EXCEPTION 'Unsupported bulk primary-key equality: table1'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid
          JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attname='id_column_1'
          WHERE k.conrelid='public.table5'::regclass AND k.contype='p' AND k.convalidated AND NOT k.condeferrable
          AND k.conkey=ARRAY[a.attnum]::smallint[] AND i.indisvalid AND i.indisunique
          AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace
             AND opcname='int8_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree')))
          THEN RAISE EXCEPTION 'Unsupported bulk primary-key equality: table5'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid
          JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attname='id_column_1'
          WHERE k.conrelid='public.table6'::regclass AND k.contype='p' AND k.convalidated AND NOT k.condeferrable
          AND k.conkey=ARRAY[a.attnum]::smallint[] AND i.indisvalid AND i.indisunique
          AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace
             AND opcname='int8_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree')))
          THEN RAISE EXCEPTION 'Unsupported bulk primary-key equality: table6'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid
          JOIN pg_attribute a ON a.attrelid=k.conrelid AND a.attname='id_column_1'
          WHERE k.conrelid='public.table2'::regclass AND k.contype='p' AND k.convalidated AND NOT k.condeferrable
          AND k.conkey=ARRAY[a.attnum]::smallint[] AND i.indisvalid AND i.indisunique
          AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace
             AND opcname='int8_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree')))
          THEN RAISE EXCEPTION 'Unsupported bulk primary-key equality: table2'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table1'::regclass AND attname='id_column_1' AND attidentity IN ('a','d')) THEN RAISE EXCEPTION 'Unsupported bulk identity default'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table2'::regclass AND attname='id_column_1' AND attidentity IN ('a','d')) THEN RAISE EXCEPTION 'Unsupported bulk identity default'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table3'::regclass AND attname='id_column_1' AND attidentity IN ('a','d')) THEN RAISE EXCEPTION 'Unsupported bulk identity default'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table5'::regclass AND attname='id_column_1' AND attidentity IN ('a','d')) THEN RAISE EXCEPTION 'Unsupported bulk identity default'; END IF;
IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table6'::regclass AND attname='id_column_1' AND attidentity IN ('a','d')) THEN RAISE EXCEPTION 'Unsupported bulk identity default'; END IF;
IF EXISTS(SELECT 1 FROM public.table3) THEN RAISE EXCEPTION 'Bulk FK mode requires empty destinations'; END IF;
IF EXISTS(SELECT 1 FROM public.table4) THEN RAISE EXCEPTION 'Bulk FK mode requires empty destinations'; END IF;
IF EXISTS(SELECT 1 FROM public.table1) THEN RAISE EXCEPTION 'Bulk FK mode requires empty destinations'; END IF;
IF EXISTS(SELECT 1 FROM public.table5) THEN RAISE EXCEPTION 'Bulk FK mode requires empty destinations'; END IF;
IF EXISTS(SELECT 1 FROM public.table6) THEN RAISE EXCEPTION 'Bulk FK mode requires empty destinations'; END IF;
IF EXISTS(SELECT 1 FROM public.table2) THEN RAISE EXCEPTION 'Bulk FK mode requires empty destinations'; END IF;
 CREATE TEMP TABLE pipeline_bulk_foreign_keys(table_name text,constraint_name text,definition text) ON COMMIT DROP;
 PERFORM set_config('search_path','pg_catalog',true);
 FOR item IN SELECT * FROM (VALUES ('table1','table1_id_column_3_fkey','FOREIGN KEY (id_column_3) REFERENCES public.raw_data(raw_id) ON UPDATE CASCADE ON DELETE CASCADE'),('table1','table1_id_column_4_fkey','FOREIGN KEY (id_column_4) REFERENCES public.table3(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE'),('table2','table2_id_column_2_fkey','FOREIGN KEY (id_column_2) REFERENCES public.table1(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE'),('table5','table5_id_column_2_fkey','FOREIGN KEY (id_column_2) REFERENCES public.table1(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE'),('table6','table6_id_column_2_fkey','FOREIGN KEY (id_column_2) REFERENCES public.table1(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE'),('table6','table6_id_column_3_fkey','FOREIGN KEY (id_column_3) REFERENCES public.table4(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE')) v(table_name,constraint_name,definition) LOOP
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
END $bulk_prepare$;
END IF; END $guarded_bulk$;

CREATE TEMP TABLE "pgdm_cross_table_hash_manifest" ON COMMIT DROP AS
SELECT
    "raw_schema"::text AS raw_schema,
    "raw_hash"::text AS raw_hash,
    COUNT(*)::bigint AS row_count
FROM "public"."raw_data"
WHERE "raw_schema" IN ('group001')
GROUP BY "raw_schema", "raw_hash";

DO $pgdm_expansion_scope$
DECLARE
    missing_schemas text;
BEGIN
    SELECT string_agg(expected.raw_schema, ', ' ORDER BY expected.raw_schema)
    INTO missing_schemas
    FROM unnest(ARRAY['group001']::text[]) AS expected(raw_schema)
    WHERE NOT EXISTS (
        SELECT 1
        FROM "pgdm_cross_table_hash_manifest" AS actual
        WHERE actual.raw_schema = expected.raw_schema
    );

    IF missing_schemas IS NOT NULL THEN
        RAISE EXCEPTION
            'raw_schema has no source rows: %.',
            missing_schemas;
    END IF;
END
$pgdm_expansion_scope$;



DO $lab_ctas$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;
CREATE TEMP TABLE pipeline_ready_table3 (text_column_1,text_column_2,numeric_column_1,text_column_3,text_column_4) ON COMMIT DROP AS SELECT DISTINCT
    BTRIM(source_row.text_column_1),
    'SYNTHETIC/REFERENCE',
    NULL::numeric,
    LEFT(UPPER(BTRIM(source_row.text_column_2)), 1),
    RIGHT(UPPER(BTRIM(source_row.text_column_2)), 1)
FROM challenge_lab.pipeline_normalized_input AS source_row
WHERE BTRIM(source_row.text_column_1) <> ''
  AND UPPER(BTRIM(source_row.text_column_2)) ~ '^[A-Z]{2}$';
DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;
INSERT INTO public.table3 (
    text_column_1,
    text_column_2,
    numeric_column_1,
    text_column_3,
    text_column_4
) SELECT text_column_1,text_column_2,numeric_column_1,text_column_3,text_column_4 FROM pg_temp.pipeline_ready_table3;
DROP TABLE pg_temp.pipeline_ready_table3;
ELSE
INSERT INTO public.table3 (
    text_column_1,
    text_column_2,
    numeric_column_1,
    text_column_3,
    text_column_4
)
SELECT DISTINCT
    BTRIM(source_row.raw -> 'values' ->> 'text_column_1'),
    'SYNTHETIC/REFERENCE',
    NULL::numeric,
    LEFT(UPPER(BTRIM(source_row.raw -> 'values' ->> 'text_column_2')), 1),
    RIGHT(UPPER(BTRIM(source_row.raw -> 'values' ->> 'text_column_2')), 1)
FROM (
    SELECT *
    FROM "public"."raw_data"
    WHERE "raw_schema" IN ('group001')
) AS source_row
WHERE BTRIM(source_row.raw -> 'values' ->> 'text_column_1') <> ''
  AND UPPER(BTRIM(source_row.raw -> 'values' ->> 'text_column_2')) ~ '^[A-Z]{2}$';
END IF; END $lab_ctas$;

DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
ANALYZE public.table3;
END IF; END $guarded_bulk$;

DO $lab_ctas$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;
CREATE TEMP TABLE pipeline_ready_table4 (id_column_1,text_column_1,text_column_2) ON COMMIT DROP AS SELECT DISTINCT
    (
        (ASCII(LEFT(UPPER(BTRIM(source_row.text_column_3)), 1))
            - ASCII('A')) * 26
        + ASCII(RIGHT(UPPER(BTRIM(source_row.text_column_3)), 1))
        - ASCII('A')
        + 1
    )::smallint,
    'SYNTHETIC/GENERIC',
    UPPER(BTRIM(source_row.text_column_3))
FROM challenge_lab.pipeline_normalized_input AS source_row
WHERE UPPER(BTRIM(source_row.text_column_3)) ~ '^[A-Z]{2}$';
DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;
INSERT INTO public.table4 (
    id_column_1,
    text_column_1,
    text_column_2
) SELECT id_column_1,text_column_1,text_column_2 FROM pg_temp.pipeline_ready_table4;
DROP TABLE pg_temp.pipeline_ready_table4;
ELSE
INSERT INTO public.table4 (
    id_column_1,
    text_column_1,
    text_column_2
)
SELECT DISTINCT
    (
        (ASCII(LEFT(UPPER(BTRIM(source_row.raw -> 'values' ->> 'text_column_3')), 1))
            - ASCII('A')) * 26
        + ASCII(RIGHT(UPPER(BTRIM(source_row.raw -> 'values' ->> 'text_column_3')), 1))
        - ASCII('A')
        + 1
    )::smallint,
    'SYNTHETIC/GENERIC',
    UPPER(BTRIM(source_row.raw -> 'values' ->> 'text_column_3'))
FROM (
    SELECT *
    FROM "public"."raw_data"
    WHERE "raw_schema" IN ('group001')
) AS source_row
WHERE UPPER(BTRIM(source_row.raw -> 'values' ->> 'text_column_3')) ~ '^[A-Z]{2}$';
END IF; END $lab_ctas$;

DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
ANALYZE public.table4;
END IF; END $guarded_bulk$;

DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
DO $index_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF (SELECT count(*) FROM pg_attribute WHERE attrelid='public.table3'::regclass
          AND attname IN ('text_column_1','text_column_2','text_column_3','text_column_4')
          AND atttypid='varchar'::regtype AND atttypmod=-1 AND attcollation='default'::regcollation)<>4 THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      IF NOT EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid='public.table1'::regclass AND a.attname='id_column_3' AND a.atttypid='bigint'::regtype) THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      SELECT k.conname,k.conkey,k.conindid,pg_get_constraintdef(k.oid) AS definition INTO item
        FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid JOIN pg_class x ON x.oid=i.indexrelid
        WHERE k.conrelid='public.table1'::regclass AND k.conname='table1_id_column_3_key' AND k.contype='u'
          AND k.convalidated AND NOT k.condeferrable AND i.indisvalid AND i.indisunique
          AND NOT i.indisclustered AND NOT i.indisreplident
          AND i.indnkeyatts=1 AND i.indnatts=1 AND i.indexprs IS NULL AND i.indpred IS NULL
          AND x.reltablespace=0 AND x.reloptions IS NULL AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace AND opcname='int8_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree'));
      IF item.conname IS NULL OR item.definition<>'UNIQUE (id_column_3)' OR EXISTS(
         SELECT 1 FROM pg_constraint k WHERE k.contype='f' AND k.confrelid='public.table1'::regclass AND k.confkey=item.conkey)
      THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_unique_keys(table_name text,constraint_name text,definition text) ON COMMIT DROP;
      INSERT INTO pg_temp.pipeline_bulk_unique_keys VALUES('table1',item.conname,item.definition);
      EXECUTE format('ALTER TABLE public.%I DROP CONSTRAINT %I','table1',item.conname);
      PERFORM set_config('search_path',previous_path,true);
    END $index_prepare$;
DO $primary_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF NOT EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_sequence s
         ON s.seqrelid=pg_get_serial_sequence('public.table1','id_column_1')::regclass
         WHERE a.attrelid='public.table1'::regclass AND a.attname='id_column_1' AND a.attidentity IN ('a','d')
           AND a.attnotnull AND a.atttypid='bigint'::regtype AND NOT s.seqcycle AND s.seqtypid='bigint'::regtype)
      THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      SELECT k.conname,k.conkey,pg_get_constraintdef(k.oid) AS definition INTO item
        FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid JOIN pg_class x ON x.oid=i.indexrelid
        WHERE k.conrelid='public.table1'::regclass AND k.conname='table1_pkey' AND k.contype='p'
          AND k.convalidated AND NOT k.condeferrable AND i.indisvalid AND i.indisunique
          AND NOT i.indisclustered AND NOT i.indisreplident AND i.indnkeyatts=1 AND i.indnatts=1
          AND i.indexprs IS NULL AND i.indpred IS NULL AND x.reltablespace=0 AND x.reloptions IS NULL
          AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace
             AND opcname='int8_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree'));
      IF item.conname IS NULL OR item.definition<>'PRIMARY KEY (id_column_1)' OR EXISTS(
         SELECT 1 FROM pg_constraint k WHERE k.contype='f' AND k.confrelid='public.table1'::regclass AND k.confkey=item.conkey)
      THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_unique_keys(table_name text,constraint_name text,definition text) ON COMMIT DROP;
      INSERT INTO pg_temp.pipeline_bulk_unique_keys VALUES('table1',item.conname,item.definition);
      EXECUTE format('ALTER TABLE public.%I DROP CONSTRAINT %I','table1',item.conname);
      IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table1'::regclass AND attname='id_column_1' AND attnotnull)
      THEN RAISE EXCEPTION 'Identity NOT NULL protection unexpectedly changed'; END IF;
      PERFORM set_config('search_path',previous_path,true);
    END $primary_prepare$;
END IF; END $guarded_bulk$;

DO $lab_ctas$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;
CREATE TEMP TABLE pipeline_ready_table1 (id_column_2,text_column_1,datetime_column_1,text_column_2,integer_column_1,integer_column_2,integer_column_3,text_column_3,id_column_3,id_column_4,id_column_5,integer_column_4,integer_column_5,id_column_6,integer_column_6,integer_column_7) ON COMMIT DROP AS SELECT
    NULLIF(BTRIM(source_row.id_column_1), '')::integer,
    NULL::character varying,
    NULLIF(BTRIM(source_row.datetime_column_1), '')::timestamptz
        AT TIME ZONE 'America/Sao_Paulo',
    NULL::character varying,
    GREATEST(
        NULLIF(BTRIM(source_row.integer_column_2), '')::smallint,
        NULLIF(BTRIM(source_row.integer_column_3), '')::smallint,
        NULLIF(BTRIM(source_row.integer_column_4), '')::smallint,
        NULLIF(BTRIM(source_row.integer_column_5), '')::smallint,
        NULLIF(BTRIM(source_row.integer_column_6), '')::smallint,
        NULLIF(BTRIM(source_row.integer_column_7), '')::smallint,
        NULLIF(BTRIM(source_row.integer_column_8), '')::smallint,
        NULLIF(BTRIM(source_row.integer_column_9), '')::smallint,
        NULLIF(BTRIM(source_row.integer_column_10), '')::smallint,
        NULLIF(BTRIM(source_row.integer_column_11), '')::smallint,
        NULLIF(BTRIM(source_row.integer_column_12), '')::smallint,
        NULLIF(BTRIM(source_row.integer_column_13), '')::smallint,
        NULLIF(BTRIM(source_row.integer_column_14), '')::smallint,
        NULLIF(BTRIM(source_row.integer_column_15), '')::smallint
    ),
    NULL::smallint,
    CASE
        WHEN NULLIF(BTRIM(source_row.numeric_column_2), '')::numeric > 0
        THEN ROUND(
            NULLIF(BTRIM(source_row.numeric_column_2), '')::numeric * 1000
        )::integer
        ELSE NULL::integer
    END,
    NULL::character varying,
    source_row.raw_id,
    dimension_row.id_column_1,
    NULL::bigint,
    NULL::integer,
    NULL::integer,
    NULL::integer,
    NULL::integer,
    CASE
        WHEN NULLIF(BTRIM(source_row.numeric_column_1), '')::numeric > 0
        THEN ROUND(
            NULLIF(BTRIM(source_row.numeric_column_1), '')::numeric * 1000
        )::integer
        ELSE NULL::integer
    END
FROM challenge_lab.pipeline_normalized_input AS source_row
JOIN public.table3 AS dimension_row
  ON dimension_row.text_column_1 = BTRIM(source_row.text_column_1)
 AND dimension_row.text_column_2 = 'SYNTHETIC/REFERENCE'
 AND dimension_row.numeric_column_1 IS NULL
 AND dimension_row.text_column_3 = LEFT(
        UPPER(BTRIM(source_row.text_column_2)),
        1
    )
 AND dimension_row.text_column_4 = RIGHT(
        UPPER(BTRIM(source_row.text_column_2)),
        1
    );
DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;
INSERT INTO public.table1 (
    id_column_2,
    text_column_1,
    datetime_column_1,
    text_column_2,
    integer_column_1,
    integer_column_2,
    integer_column_3,
    text_column_3,
    id_column_3,
    id_column_4,
    id_column_5,
    integer_column_4,
    integer_column_5,
    id_column_6,
    integer_column_6,
    integer_column_7
) SELECT id_column_2,text_column_1,datetime_column_1,text_column_2,integer_column_1,integer_column_2,integer_column_3,text_column_3,id_column_3,id_column_4,id_column_5,integer_column_4,integer_column_5,id_column_6,integer_column_6,integer_column_7 FROM pg_temp.pipeline_ready_table1;
DROP TABLE pg_temp.pipeline_ready_table1;
ELSE
INSERT INTO public.table1 (
    id_column_2,
    text_column_1,
    datetime_column_1,
    text_column_2,
    integer_column_1,
    integer_column_2,
    integer_column_3,
    text_column_3,
    id_column_3,
    id_column_4,
    id_column_5,
    integer_column_4,
    integer_column_5,
    id_column_6,
    integer_column_6,
    integer_column_7
)
SELECT
    NULLIF(BTRIM(source_row.raw -> 'values' ->> 'id_column_1'), '')::integer,
    NULL::character varying,
    NULLIF(BTRIM(source_row.raw -> 'values' ->> 'datetime_column_1'), '')::timestamptz
        AT TIME ZONE 'America/Sao_Paulo',
    NULL::character varying,
    GREATEST(
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_2'), '')::smallint,
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_3'), '')::smallint,
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_4'), '')::smallint,
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_5'), '')::smallint,
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_6'), '')::smallint,
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_7'), '')::smallint,
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_8'), '')::smallint,
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_9'), '')::smallint,
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_10'), '')::smallint,
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_11'), '')::smallint,
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_12'), '')::smallint,
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_13'), '')::smallint,
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_14'), '')::smallint,
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_15'), '')::smallint
    ),
    NULL::smallint,
    CASE
        WHEN NULLIF(BTRIM(source_row.raw -> 'values' ->> 'numeric_column_2'), '')::numeric > 0
        THEN ROUND(
            NULLIF(BTRIM(source_row.raw -> 'values' ->> 'numeric_column_2'), '')::numeric * 1000
        )::integer
        ELSE NULL::integer
    END,
    NULL::character varying,
    source_row.raw_id,
    dimension_row.id_column_1,
    NULL::bigint,
    NULL::integer,
    NULL::integer,
    NULL::integer,
    NULL::integer,
    CASE
        WHEN NULLIF(BTRIM(source_row.raw -> 'values' ->> 'numeric_column_1'), '')::numeric > 0
        THEN ROUND(
            NULLIF(BTRIM(source_row.raw -> 'values' ->> 'numeric_column_1'), '')::numeric * 1000
        )::integer
        ELSE NULL::integer
    END
FROM (
    SELECT *
    FROM "public"."raw_data"
    WHERE "raw_schema" IN ('group001')
) AS source_row
JOIN public.table3 AS dimension_row
  ON dimension_row.text_column_1 = BTRIM(source_row.raw -> 'values' ->> 'text_column_1')
 AND dimension_row.text_column_2 = 'SYNTHETIC/REFERENCE'
 AND dimension_row.numeric_column_1 IS NULL
 AND dimension_row.text_column_3 = LEFT(
        UPPER(BTRIM(source_row.raw -> 'values' ->> 'text_column_2')),
        1
    )
 AND dimension_row.text_column_4 = RIGHT(
        UPPER(BTRIM(source_row.raw -> 'values' ->> 'text_column_2')),
        1
    );
END IF; END $lab_ctas$;

DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
DO $index_restore$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); previous_memory text:=pg_catalog.current_setting('maintenance_work_mem'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF to_regclass('pg_temp.pipeline_bulk_unique_keys') IS NULL THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      PERFORM set_config('maintenance_work_mem','128MB',true);
      FOR item IN SELECT * FROM pg_temp.pipeline_bulk_unique_keys WHERE table_name='table1' LOOP
        EXECUTE format('ALTER TABLE public.%I ADD CONSTRAINT %I %s',item.table_name,item.constraint_name,item.definition);
        DELETE FROM pg_temp.pipeline_bulk_unique_keys WHERE table_name=item.table_name AND constraint_name=item.constraint_name;
      END LOOP;
      PERFORM set_config('maintenance_work_mem',previous_memory,true);
      PERFORM set_config('search_path',previous_path,true);
    END $index_restore$;
END IF; END $guarded_bulk$;

DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN

DO $primary_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF NOT EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_sequence s
         ON s.seqrelid=pg_get_serial_sequence('public.table5','id_column_1')::regclass
         WHERE a.attrelid='public.table5'::regclass AND a.attname='id_column_1' AND a.attidentity IN ('a','d')
           AND a.attnotnull AND a.atttypid='bigint'::regtype AND NOT s.seqcycle AND s.seqtypid='bigint'::regtype)
      THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      SELECT k.conname,k.conkey,pg_get_constraintdef(k.oid) AS definition INTO item
        FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid JOIN pg_class x ON x.oid=i.indexrelid
        WHERE k.conrelid='public.table5'::regclass AND k.conname='table5_pkey' AND k.contype='p'
          AND k.convalidated AND NOT k.condeferrable AND i.indisvalid AND i.indisunique
          AND NOT i.indisclustered AND NOT i.indisreplident AND i.indnkeyatts=1 AND i.indnatts=1
          AND i.indexprs IS NULL AND i.indpred IS NULL AND x.reltablespace=0 AND x.reloptions IS NULL
          AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace
             AND opcname='int8_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree'));
      IF item.conname IS NULL OR item.definition<>'PRIMARY KEY (id_column_1)' OR EXISTS(
         SELECT 1 FROM pg_constraint k WHERE k.contype='f' AND k.confrelid='public.table5'::regclass AND k.confkey=item.conkey)
      THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_unique_keys(table_name text,constraint_name text,definition text) ON COMMIT DROP;
      INSERT INTO pg_temp.pipeline_bulk_unique_keys VALUES('table5',item.conname,item.definition);
      EXECUTE format('ALTER TABLE public.%I DROP CONSTRAINT %I','table5',item.conname);
      IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table5'::regclass AND attname='id_column_1' AND attnotnull)
      THEN RAISE EXCEPTION 'Identity NOT NULL protection unexpectedly changed'; END IF;
      PERFORM set_config('search_path',previous_path,true);
    END $primary_prepare$;
END IF; END $guarded_bulk$;

DO $lab_ctas$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;
CREATE TEMP TABLE pipeline_ready_table5 (numeric_column_1,numeric_column_2,integer_column_1,integer_column_2,id_column_2,id_column_3) ON COMMIT DROP AS SELECT
    ROUND(
        NULLIF(BTRIM(source_row.integer_column_1), '')::numeric,
        1
    ),
    NULL::numeric,
    NULL::integer,
    NULL::integer,
    main_row.id_column_1,
    NULL::bigint
FROM challenge_lab.pipeline_normalized_input AS source_row
JOIN public.table1 AS main_row
  ON main_row.id_column_3 = source_row.raw_id
WHERE NULLIF(BTRIM(source_row.integer_column_1), '')::numeric > 0;
DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;
INSERT INTO public.table5 (
    numeric_column_1,
    numeric_column_2,
    integer_column_1,
    integer_column_2,
    id_column_2,
    id_column_3
) SELECT numeric_column_1,numeric_column_2,integer_column_1,integer_column_2,id_column_2,id_column_3 FROM pg_temp.pipeline_ready_table5;
DROP TABLE pg_temp.pipeline_ready_table5;
ELSE
INSERT INTO public.table5 (
    numeric_column_1,
    numeric_column_2,
    integer_column_1,
    integer_column_2,
    id_column_2,
    id_column_3
)
SELECT
    ROUND(
        NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_1'), '')::numeric,
        1
    ),
    NULL::numeric,
    NULL::integer,
    NULL::integer,
    main_row.id_column_1,
    NULL::bigint
FROM (
    SELECT *
    FROM "public"."raw_data"
    WHERE "raw_schema" IN ('group001')
) AS source_row
JOIN public.table1 AS main_row
  ON main_row.id_column_3 = source_row.raw_id
WHERE NULLIF(BTRIM(source_row.raw -> 'values' ->> 'integer_column_1'), '')::numeric > 0;
END IF; END $lab_ctas$;

DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
DO $index_restore$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); previous_memory text:=pg_catalog.current_setting('maintenance_work_mem'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF to_regclass('pg_temp.pipeline_bulk_unique_keys') IS NULL THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      PERFORM set_config('maintenance_work_mem','128MB',true);
      FOR item IN SELECT * FROM pg_temp.pipeline_bulk_unique_keys WHERE table_name='table5' LOOP
        EXECUTE format('ALTER TABLE public.%I ADD CONSTRAINT %I %s',item.table_name,item.constraint_name,item.definition);
        DELETE FROM pg_temp.pipeline_bulk_unique_keys WHERE table_name=item.table_name AND constraint_name=item.constraint_name;
      END LOOP;
      PERFORM set_config('maintenance_work_mem',previous_memory,true);
      PERFORM set_config('search_path',previous_path,true);
    END $index_restore$;
END IF; END $guarded_bulk$;

DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN

DO $primary_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF NOT EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_sequence s
         ON s.seqrelid=pg_get_serial_sequence('public.table6','id_column_1')::regclass
         WHERE a.attrelid='public.table6'::regclass AND a.attname='id_column_1' AND a.attidentity IN ('a','d')
           AND a.attnotnull AND a.atttypid='bigint'::regtype AND NOT s.seqcycle AND s.seqtypid='bigint'::regtype)
      THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      SELECT k.conname,k.conkey,pg_get_constraintdef(k.oid) AS definition INTO item
        FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid JOIN pg_class x ON x.oid=i.indexrelid
        WHERE k.conrelid='public.table6'::regclass AND k.conname='table6_pkey' AND k.contype='p'
          AND k.convalidated AND NOT k.condeferrable AND i.indisvalid AND i.indisunique
          AND NOT i.indisclustered AND NOT i.indisreplident AND i.indnkeyatts=1 AND i.indnatts=1
          AND i.indexprs IS NULL AND i.indpred IS NULL AND x.reltablespace=0 AND x.reloptions IS NULL
          AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace
             AND opcname='int8_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree'));
      IF item.conname IS NULL OR item.definition<>'PRIMARY KEY (id_column_1)' OR EXISTS(
         SELECT 1 FROM pg_constraint k WHERE k.contype='f' AND k.confrelid='public.table6'::regclass AND k.confkey=item.conkey)
      THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_unique_keys(table_name text,constraint_name text,definition text) ON COMMIT DROP;
      INSERT INTO pg_temp.pipeline_bulk_unique_keys VALUES('table6',item.conname,item.definition);
      EXECUTE format('ALTER TABLE public.%I DROP CONSTRAINT %I','table6',item.conname);
      IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table6'::regclass AND attname='id_column_1' AND attnotnull)
      THEN RAISE EXCEPTION 'Identity NOT NULL protection unexpectedly changed'; END IF;
      PERFORM set_config('search_path',previous_path,true);
    END $primary_prepare$;
END IF; END $guarded_bulk$;

DO $lab_ctas$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;
CREATE TEMP TABLE pipeline_ready_table6 (id_column_2,id_column_3) ON COMMIT DROP AS SELECT
    main_row.id_column_1,
    reference_row.id_column_1
FROM challenge_lab.pipeline_normalized_input AS source_row
JOIN public.table1 AS main_row
  ON main_row.id_column_3 = source_row.raw_id
JOIN public.table4 AS reference_row
  ON reference_row.text_column_1 = 'SYNTHETIC/GENERIC'
 AND reference_row.text_column_2 = UPPER(
        BTRIM(source_row.text_column_3)
    );
DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;
INSERT INTO public.table6 (
    id_column_2,
    id_column_3
) SELECT id_column_2,id_column_3 FROM pg_temp.pipeline_ready_table6;
DROP TABLE pg_temp.pipeline_ready_table6;
ELSE
INSERT INTO public.table6 (
    id_column_2,
    id_column_3
)
SELECT
    main_row.id_column_1,
    reference_row.id_column_1
FROM (
    SELECT *
    FROM "public"."raw_data"
    WHERE "raw_schema" IN ('group001')
) AS source_row
JOIN public.table1 AS main_row
  ON main_row.id_column_3 = source_row.raw_id
JOIN public.table4 AS reference_row
  ON reference_row.text_column_1 = 'SYNTHETIC/GENERIC'
 AND reference_row.text_column_2 = UPPER(
        BTRIM(source_row.raw -> 'values' ->> 'text_column_3')
    );
END IF; END $lab_ctas$;

DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
DO $index_restore$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); previous_memory text:=pg_catalog.current_setting('maintenance_work_mem'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF to_regclass('pg_temp.pipeline_bulk_unique_keys') IS NULL THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      PERFORM set_config('maintenance_work_mem','128MB',true);
      FOR item IN SELECT * FROM pg_temp.pipeline_bulk_unique_keys WHERE table_name='table6' LOOP
        EXECUTE format('ALTER TABLE public.%I ADD CONSTRAINT %I %s',item.table_name,item.constraint_name,item.definition);
        DELETE FROM pg_temp.pipeline_bulk_unique_keys WHERE table_name=item.table_name AND constraint_name=item.constraint_name;
      END LOOP;
      PERFORM set_config('maintenance_work_mem',previous_memory,true);
      PERFORM set_config('search_path',previous_path,true);
    END $index_restore$;
END IF; END $guarded_bulk$;

DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
DO $index_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      
      IF NOT EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid='public.table2'::regclass AND a.attname='integer_column_1' AND a.atttypid='smallint'::regtype) THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      SELECT k.conname,k.conkey,k.conindid,pg_get_constraintdef(k.oid) AS definition INTO item
        FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid JOIN pg_class x ON x.oid=i.indexrelid
        WHERE k.conrelid='public.table2'::regclass AND k.conname='table2_id_column_2_integer_column_1_key' AND k.contype='u'
          AND k.convalidated AND NOT k.condeferrable AND i.indisvalid AND i.indisunique
          AND NOT i.indisclustered AND NOT i.indisreplident
          AND i.indnkeyatts=2 AND i.indnatts=2 AND i.indexprs IS NULL AND i.indpred IS NULL
          AND x.reltablespace=0 AND x.reloptions IS NULL AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace AND opcname='int8_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree')) AND i.indclass[1]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace AND opcname='int2_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree'));
      IF item.conname IS NULL OR item.definition<>'UNIQUE (id_column_2, integer_column_1)' OR EXISTS(
         SELECT 1 FROM pg_constraint k WHERE k.contype='f' AND k.confrelid='public.table2'::regclass AND k.confkey=item.conkey)
      THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_unique_keys(table_name text,constraint_name text,definition text) ON COMMIT DROP;
      INSERT INTO pg_temp.pipeline_bulk_unique_keys VALUES('table2',item.conname,item.definition);
      EXECUTE format('ALTER TABLE public.%I DROP CONSTRAINT %I','table2',item.conname);
      PERFORM set_config('search_path',previous_path,true);
    END $index_prepare$;
DO $primary_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF NOT EXISTS(SELECT 1 FROM pg_attribute a JOIN pg_sequence s
         ON s.seqrelid=pg_get_serial_sequence('public.table2','id_column_1')::regclass
         WHERE a.attrelid='public.table2'::regclass AND a.attname='id_column_1' AND a.attidentity IN ('a','d')
           AND a.attnotnull AND a.atttypid='bigint'::regtype AND NOT s.seqcycle AND s.seqtypid='bigint'::regtype)
      THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      SELECT k.conname,k.conkey,pg_get_constraintdef(k.oid) AS definition INTO item
        FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid JOIN pg_class x ON x.oid=i.indexrelid
        WHERE k.conrelid='public.table2'::regclass AND k.conname='table2_pkey' AND k.contype='p'
          AND k.convalidated AND NOT k.condeferrable AND i.indisvalid AND i.indisunique
          AND NOT i.indisclustered AND NOT i.indisreplident AND i.indnkeyatts=1 AND i.indnatts=1
          AND i.indexprs IS NULL AND i.indpred IS NULL AND x.reltablespace=0 AND x.reloptions IS NULL
          AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace
             AND opcname='int8_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree'));
      IF item.conname IS NULL OR item.definition<>'PRIMARY KEY (id_column_1)' OR EXISTS(
         SELECT 1 FROM pg_constraint k WHERE k.contype='f' AND k.confrelid='public.table2'::regclass AND k.confkey=item.conkey)
      THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_unique_keys(table_name text,constraint_name text,definition text) ON COMMIT DROP;
      INSERT INTO pg_temp.pipeline_bulk_unique_keys VALUES('table2',item.conname,item.definition);
      EXECUTE format('ALTER TABLE public.%I DROP CONSTRAINT %I','table2',item.conname);
      IF NOT EXISTS(SELECT 1 FROM pg_attribute WHERE attrelid='public.table2'::regclass AND attname='id_column_1' AND attnotnull)
      THEN RAISE EXCEPTION 'Identity NOT NULL protection unexpectedly changed'; END IF;
      PERFORM set_config('search_path',previous_path,true);
    END $primary_prepare$;
END IF; END $guarded_bulk$;

DO $lab_ctas$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;
CREATE TEMP TABLE pipeline_ready_table2 (id_column_2,integer_column_1,integer_column_2,text_column_1,integer_column_3,integer_column_4,integer_column_5,integer_column_6) ON COMMIT DROP AS SELECT
    main_row.id_column_1,
    position_keys.position_number,
    ROUND(
        NULLIF(BTRIM((CASE position_keys.position_number WHEN 1 THEN source_row.numeric_column_3 WHEN 2 THEN source_row.numeric_column_5 WHEN 3 THEN source_row.numeric_column_7 WHEN 4 THEN source_row.numeric_column_9 WHEN 5 THEN source_row.numeric_column_11 WHEN 6 THEN source_row.numeric_column_13 WHEN 7 THEN source_row.numeric_column_15 WHEN 8 THEN source_row.numeric_column_17 WHEN 9 THEN source_row.numeric_column_19 WHEN 10 THEN source_row.numeric_column_21 WHEN 11 THEN source_row.numeric_column_23 WHEN 12 THEN source_row.numeric_column_25 WHEN 13 THEN source_row.numeric_column_27 WHEN 14 THEN source_row.numeric_column_29 END)), '')::numeric * 1000
    )::integer,
    UPPER(BTRIM((CASE position_keys.position_number WHEN 1 THEN source_row.text_column_4 WHEN 2 THEN source_row.text_column_5 WHEN 3 THEN source_row.text_column_6 WHEN 4 THEN source_row.text_column_7 WHEN 5 THEN source_row.text_column_8 WHEN 6 THEN source_row.text_column_9 WHEN 7 THEN source_row.text_column_10 WHEN 8 THEN source_row.text_column_11 WHEN 9 THEN source_row.text_column_12 WHEN 10 THEN source_row.text_column_13 WHEN 11 THEN source_row.text_column_14 WHEN 12 THEN source_row.text_column_15 WHEN 13 THEN source_row.text_column_16 WHEN 14 THEN source_row.text_column_17 END))),
    NULL::integer,
    NULL::integer,
    NULL::integer,
    CASE
        WHEN NULLIF(BTRIM((CASE position_keys.position_number WHEN 1 THEN source_row.numeric_column_4 WHEN 2 THEN source_row.numeric_column_6 WHEN 3 THEN source_row.numeric_column_8 WHEN 4 THEN source_row.numeric_column_10 WHEN 5 THEN source_row.numeric_column_12 WHEN 6 THEN source_row.numeric_column_14 WHEN 7 THEN source_row.numeric_column_16 WHEN 8 THEN source_row.numeric_column_18 WHEN 9 THEN source_row.numeric_column_20 WHEN 10 THEN source_row.numeric_column_22 WHEN 11 THEN source_row.numeric_column_24 WHEN 12 THEN source_row.numeric_column_26 WHEN 13 THEN source_row.numeric_column_28 WHEN 14 THEN source_row.numeric_column_30 END)), '')::numeric > 0
        THEN ROUND(
            NULLIF(BTRIM((CASE position_keys.position_number WHEN 1 THEN source_row.numeric_column_4 WHEN 2 THEN source_row.numeric_column_6 WHEN 3 THEN source_row.numeric_column_8 WHEN 4 THEN source_row.numeric_column_10 WHEN 5 THEN source_row.numeric_column_12 WHEN 6 THEN source_row.numeric_column_14 WHEN 7 THEN source_row.numeric_column_16 WHEN 8 THEN source_row.numeric_column_18 WHEN 9 THEN source_row.numeric_column_20 WHEN 10 THEN source_row.numeric_column_22 WHEN 11 THEN source_row.numeric_column_24 WHEN 12 THEN source_row.numeric_column_26 WHEN 13 THEN source_row.numeric_column_28 WHEN 14 THEN source_row.numeric_column_30 END)), '')::numeric * 1000
        )::integer
        ELSE NULL::integer
    END
FROM challenge_lab.pipeline_normalized_input AS source_row
JOIN public.table1 AS main_row
  ON main_row.id_column_3 = source_row.raw_id
CROSS JOIN LATERAL (SELECT position_number FROM pg_catalog.unnest((ARRAY[1::smallint,2::smallint,3::smallint,4::smallint,5::smallint,6::smallint,7::smallint,8::smallint,9::smallint,10::smallint,11::smallint,12::smallint,13::smallint,14::smallint])[1:LEAST(COALESCE(main_row.integer_column_1,0),14)]) AS expanded(position_number) OFFSET 0) AS position_keys

WHERE NULLIF(BTRIM((CASE position_keys.position_number WHEN 1 THEN source_row.integer_column_2 WHEN 2 THEN source_row.integer_column_3 WHEN 3 THEN source_row.integer_column_4 WHEN 4 THEN source_row.integer_column_5 WHEN 5 THEN source_row.integer_column_6 WHEN 6 THEN source_row.integer_column_7 WHEN 7 THEN source_row.integer_column_8 WHEN 8 THEN source_row.integer_column_9 WHEN 9 THEN source_row.integer_column_10 WHEN 10 THEN source_row.integer_column_11 WHEN 11 THEN source_row.integer_column_12 WHEN 12 THEN source_row.integer_column_13 WHEN 13 THEN source_row.integer_column_14 WHEN 14 THEN source_row.integer_column_15 END)), '')::smallint
        = position_keys.position_number
  AND NULLIF(BTRIM((CASE position_keys.position_number WHEN 1 THEN source_row.numeric_column_3 WHEN 2 THEN source_row.numeric_column_5 WHEN 3 THEN source_row.numeric_column_7 WHEN 4 THEN source_row.numeric_column_9 WHEN 5 THEN source_row.numeric_column_11 WHEN 6 THEN source_row.numeric_column_13 WHEN 7 THEN source_row.numeric_column_15 WHEN 8 THEN source_row.numeric_column_17 WHEN 9 THEN source_row.numeric_column_19 WHEN 10 THEN source_row.numeric_column_21 WHEN 11 THEN source_row.numeric_column_23 WHEN 12 THEN source_row.numeric_column_25 WHEN 13 THEN source_row.numeric_column_27 WHEN 14 THEN source_row.numeric_column_29 END)), '')::numeric >= 0
  AND UPPER(BTRIM((CASE position_keys.position_number WHEN 1 THEN source_row.text_column_4 WHEN 2 THEN source_row.text_column_5 WHEN 3 THEN source_row.text_column_6 WHEN 4 THEN source_row.text_column_7 WHEN 5 THEN source_row.text_column_8 WHEN 6 THEN source_row.text_column_9 WHEN 7 THEN source_row.text_column_10 WHEN 8 THEN source_row.text_column_11 WHEN 9 THEN source_row.text_column_12 WHEN 10 THEN source_row.text_column_13 WHEN 11 THEN source_row.text_column_14 WHEN 12 THEN source_row.text_column_15 WHEN 13 THEN source_row.text_column_16 WHEN 14 THEN source_row.text_column_17 END))) IN ('X', 'Y', 'Z');
DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;
INSERT INTO public.table2 (
    id_column_2,
    integer_column_1,
    integer_column_2,
    text_column_1,
    integer_column_3,
    integer_column_4,
    integer_column_5,
    integer_column_6
) SELECT id_column_2,integer_column_1,integer_column_2,text_column_1,integer_column_3,integer_column_4,integer_column_5,integer_column_6 FROM pg_temp.pipeline_ready_table2;
DROP TABLE pg_temp.pipeline_ready_table2;
ELSE
INSERT INTO public.table2 (
    id_column_2,
    integer_column_1,
    integer_column_2,
    text_column_1,
    integer_column_3,
    integer_column_4,
    integer_column_5,
    integer_column_6
)
SELECT
    main_row.id_column_1,
    position_data.position_number,
    ROUND(
        NULLIF(BTRIM(position_data.weight_text), '')::numeric * 1000
    )::integer,
    UPPER(BTRIM(position_data.type_text)),
    NULL::integer,
    NULL::integer,
    NULL::integer,
    CASE
        WHEN NULLIF(BTRIM(position_data.gap_text), '')::numeric > 0
        THEN ROUND(
            NULLIF(BTRIM(position_data.gap_text), '')::numeric * 1000
        )::integer
        ELSE NULL::integer
    END
FROM (
    SELECT *
    FROM "public"."raw_data"
    WHERE "raw_schema" IN ('group001')
) AS source_row
CROSS JOIN LATERAL (
    VALUES
        (1::smallint, source_row.raw -> 'values' ->> 'integer_column_2', source_row.raw -> 'values' ->> 'text_column_4', source_row.raw -> 'values' ->> 'numeric_column_3', source_row.raw -> 'values' ->> 'numeric_column_4'),
        (2::smallint, source_row.raw -> 'values' ->> 'integer_column_3', source_row.raw -> 'values' ->> 'text_column_5', source_row.raw -> 'values' ->> 'numeric_column_5', source_row.raw -> 'values' ->> 'numeric_column_6'),
        (3::smallint, source_row.raw -> 'values' ->> 'integer_column_4', source_row.raw -> 'values' ->> 'text_column_6', source_row.raw -> 'values' ->> 'numeric_column_7', source_row.raw -> 'values' ->> 'numeric_column_8'),
        (4::smallint, source_row.raw -> 'values' ->> 'integer_column_5', source_row.raw -> 'values' ->> 'text_column_7', source_row.raw -> 'values' ->> 'numeric_column_9', source_row.raw -> 'values' ->> 'numeric_column_10'),
        (5::smallint, source_row.raw -> 'values' ->> 'integer_column_6', source_row.raw -> 'values' ->> 'text_column_8', source_row.raw -> 'values' ->> 'numeric_column_11', source_row.raw -> 'values' ->> 'numeric_column_12'),
        (6::smallint, source_row.raw -> 'values' ->> 'integer_column_7', source_row.raw -> 'values' ->> 'text_column_9', source_row.raw -> 'values' ->> 'numeric_column_13', source_row.raw -> 'values' ->> 'numeric_column_14'),
        (7::smallint, source_row.raw -> 'values' ->> 'integer_column_8', source_row.raw -> 'values' ->> 'text_column_10', source_row.raw -> 'values' ->> 'numeric_column_15', source_row.raw -> 'values' ->> 'numeric_column_16'),
        (8::smallint, source_row.raw -> 'values' ->> 'integer_column_9', source_row.raw -> 'values' ->> 'text_column_11', source_row.raw -> 'values' ->> 'numeric_column_17', source_row.raw -> 'values' ->> 'numeric_column_18'),
        (9::smallint, source_row.raw -> 'values' ->> 'integer_column_10', source_row.raw -> 'values' ->> 'text_column_12', source_row.raw -> 'values' ->> 'numeric_column_19', source_row.raw -> 'values' ->> 'numeric_column_20'),
        (10::smallint, source_row.raw -> 'values' ->> 'integer_column_11', source_row.raw -> 'values' ->> 'text_column_13', source_row.raw -> 'values' ->> 'numeric_column_21', source_row.raw -> 'values' ->> 'numeric_column_22'),
        (11::smallint, source_row.raw -> 'values' ->> 'integer_column_12', source_row.raw -> 'values' ->> 'text_column_14', source_row.raw -> 'values' ->> 'numeric_column_23', source_row.raw -> 'values' ->> 'numeric_column_24'),
        (12::smallint, source_row.raw -> 'values' ->> 'integer_column_13', source_row.raw -> 'values' ->> 'text_column_15', source_row.raw -> 'values' ->> 'numeric_column_25', source_row.raw -> 'values' ->> 'numeric_column_26'),
        (13::smallint, source_row.raw -> 'values' ->> 'integer_column_14', source_row.raw -> 'values' ->> 'text_column_16', source_row.raw -> 'values' ->> 'numeric_column_27', source_row.raw -> 'values' ->> 'numeric_column_28'),
        (14::smallint, source_row.raw -> 'values' ->> 'integer_column_15', source_row.raw -> 'values' ->> 'text_column_17', source_row.raw -> 'values' ->> 'numeric_column_29', source_row.raw -> 'values' ->> 'numeric_column_30')
) AS position_data(position_number, declared_number_text, type_text, weight_text, gap_text)
JOIN public.table1 AS main_row
  ON main_row.id_column_3 = source_row.raw_id
WHERE NULLIF(BTRIM(position_data.declared_number_text), '')::smallint
        = position_data.position_number
  AND NULLIF(BTRIM(position_data.weight_text), '')::numeric >= 0
  AND UPPER(BTRIM(position_data.type_text)) IN ('X', 'Y', 'Z');
END IF; END $lab_ctas$;

DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
DO $index_restore$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); previous_memory text:=pg_catalog.current_setting('maintenance_work_mem'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF to_regclass('pg_temp.pipeline_bulk_unique_keys') IS NULL THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      PERFORM set_config('maintenance_work_mem','128MB',true);
      FOR item IN SELECT * FROM pg_temp.pipeline_bulk_unique_keys WHERE table_name='table2' LOOP
        EXECUTE format('ALTER TABLE public.%I ADD CONSTRAINT %I %s',item.table_name,item.constraint_name,item.definition);
        DELETE FROM pg_temp.pipeline_bulk_unique_keys WHERE table_name=item.table_name AND constraint_name=item.constraint_name;
      END LOOP;
      PERFORM set_config('maintenance_work_mem',previous_memory,true);
      PERFORM set_config('search_path',previous_path,true);
    END $index_restore$;
END IF; END $guarded_bulk$;

DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
DO $bulk_restore$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
 PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
 IF (SELECT count(*) FROM pg_temp.pipeline_bulk_foreign_keys)<>6 THEN RAISE EXCEPTION 'Incomplete bulk FK restoration plan'; END IF;
 FOR item IN SELECT * FROM pg_temp.pipeline_bulk_foreign_keys ORDER BY table_name,constraint_name LOOP
   EXECUTE format('ALTER TABLE public.%I ADD CONSTRAINT %I %s',item.table_name,item.constraint_name,item.definition);
 END LOOP;
 IF EXISTS(SELECT 1 FROM pg_constraint WHERE connamespace='public'::regnamespace AND NOT convalidated) THEN
   RAISE EXCEPTION 'Bulk FK validation did not complete';
 END IF;
 PERFORM pg_catalog.set_config('search_path',previous_path,true);
END $bulk_restore$;
END IF; END $guarded_bulk$;

DROP TABLE challenge_lab.pipeline_normalized_input;

COMMIT;