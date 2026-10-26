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
    NULLIF(BTRIM(qf_payload.payload ->> 'id_column_1'), '')::integer,
    NULL::character varying,
    NULLIF(BTRIM(qf_payload.payload ->> 'datetime_column_1'), '')::timestamptz
        AT TIME ZONE 'America/Sao_Paulo',
    NULL::character varying,
    GREATEST(
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_2'), '')::smallint,
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_3'), '')::smallint,
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_4'), '')::smallint,
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_5'), '')::smallint,
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_6'), '')::smallint,
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_7'), '')::smallint,
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_8'), '')::smallint,
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_9'), '')::smallint,
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_10'), '')::smallint,
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_11'), '')::smallint,
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_12'), '')::smallint,
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_13'), '')::smallint,
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_14'), '')::smallint,
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_15'), '')::smallint
    ),
    NULL::smallint,
    CASE
        WHEN NULLIF(BTRIM(qf_payload.payload ->> 'numeric_column_2'), '')::numeric > 0
        THEN ROUND(
            NULLIF(BTRIM(qf_payload.payload ->> 'numeric_column_2'), '')::numeric * 1000
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
        WHEN NULLIF(BTRIM(qf_payload.payload ->> 'numeric_column_1'), '')::numeric > 0
        THEN ROUND(
            NULLIF(BTRIM(qf_payload.payload ->> 'numeric_column_1'), '')::numeric * 1000
        )::integer
        ELSE NULL::integer
    END
FROM (
    SELECT *
    FROM "public"."raw_data"
    WHERE "raw_schema" IN ('group001')
) AS source_row
CROSS JOIN LATERAL (
    SELECT source_row.raw -> 'values' AS payload OFFSET 0
) AS qf_payload
JOIN public.table3 AS dimension_row
  ON dimension_row.text_column_1 = BTRIM(qf_payload.payload ->> 'text_column_1')
 AND dimension_row.text_column_2 = 'SYNTHETIC/REFERENCE'
 AND dimension_row.numeric_column_1 IS NULL
 AND dimension_row.text_column_3 = LEFT(
        UPPER(BTRIM(qf_payload.payload ->> 'text_column_2')),
        1
    )
 AND dimension_row.text_column_4 = RIGHT(
        UPPER(BTRIM(qf_payload.payload ->> 'text_column_2')),
        1
    );

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
CROSS JOIN LATERAL (SELECT source_row.raw -> 'values' AS payload OFFSET 0) AS projected
CROSS JOIN LATERAL unnest(ARRAY[1::smallint,2::smallint,3::smallint,4::smallint,5::smallint,6::smallint,7::smallint,8::smallint,9::smallint,10::smallint,11::smallint,12::smallint,13::smallint,14::smallint],
ARRAY[projected.payload ->> 'integer_column_2',projected.payload ->> 'integer_column_3',projected.payload ->> 'integer_column_4',projected.payload ->> 'integer_column_5',projected.payload ->> 'integer_column_6',projected.payload ->> 'integer_column_7',projected.payload ->> 'integer_column_8',projected.payload ->> 'integer_column_9',projected.payload ->> 'integer_column_10',projected.payload ->> 'integer_column_11',projected.payload ->> 'integer_column_12',projected.payload ->> 'integer_column_13',projected.payload ->> 'integer_column_14',projected.payload ->> 'integer_column_15'],
ARRAY[projected.payload ->> 'text_column_4',projected.payload ->> 'text_column_5',projected.payload ->> 'text_column_6',projected.payload ->> 'text_column_7',projected.payload ->> 'text_column_8',projected.payload ->> 'text_column_9',projected.payload ->> 'text_column_10',projected.payload ->> 'text_column_11',projected.payload ->> 'text_column_12',projected.payload ->> 'text_column_13',projected.payload ->> 'text_column_14',projected.payload ->> 'text_column_15',projected.payload ->> 'text_column_16',projected.payload ->> 'text_column_17'],
ARRAY[projected.payload ->> 'numeric_column_3',projected.payload ->> 'numeric_column_5',projected.payload ->> 'numeric_column_7',projected.payload ->> 'numeric_column_9',projected.payload ->> 'numeric_column_11',projected.payload ->> 'numeric_column_13',projected.payload ->> 'numeric_column_15',projected.payload ->> 'numeric_column_17',projected.payload ->> 'numeric_column_19',projected.payload ->> 'numeric_column_21',projected.payload ->> 'numeric_column_23',projected.payload ->> 'numeric_column_25',projected.payload ->> 'numeric_column_27',projected.payload ->> 'numeric_column_29'],
ARRAY[projected.payload ->> 'numeric_column_4',projected.payload ->> 'numeric_column_6',projected.payload ->> 'numeric_column_8',projected.payload ->> 'numeric_column_10',projected.payload ->> 'numeric_column_12',projected.payload ->> 'numeric_column_14',projected.payload ->> 'numeric_column_16',projected.payload ->> 'numeric_column_18',projected.payload ->> 'numeric_column_20',projected.payload ->> 'numeric_column_22',projected.payload ->> 'numeric_column_24',projected.payload ->> 'numeric_column_26',projected.payload ->> 'numeric_column_28',projected.payload ->> 'numeric_column_30']) AS position_data(position_number, declared_number_text, type_text, weight_text, gap_text)
JOIN public.table1 AS main_row
  ON main_row.id_column_3 = source_row.raw_id
WHERE NULLIF(BTRIM(position_data.declared_number_text), '')::smallint
        = position_data.position_number
  AND NULLIF(BTRIM(position_data.weight_text), '')::numeric >= 0
  AND UPPER(BTRIM(position_data.type_text)) IN ('X', 'Y', 'Z');

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

COMMIT;