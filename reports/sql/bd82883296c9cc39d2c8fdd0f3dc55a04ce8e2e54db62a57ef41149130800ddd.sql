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