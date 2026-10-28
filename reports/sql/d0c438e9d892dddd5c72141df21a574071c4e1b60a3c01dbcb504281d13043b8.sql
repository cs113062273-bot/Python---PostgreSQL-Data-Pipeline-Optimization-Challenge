-- lab metadata session reuse source_sha256=d483cf4df54bfe971c4db96c850fee4ddf697c8087bbf09819d8a2d96101af9a
-- lab native metadata stdin reuse source_sha256=2c903b3592ebbd3434840e382c5f73733d92088a25f45e2963f425731bb9adc6
CREATE TEMP TABLE pgdm_expansion_timings (
    sequence_number integer PRIMARY KEY,
    stage_key text NOT NULL,
    item_name text NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    rows_affected bigint,
    destination_name text,
    statement_index integer
) ON COMMIT PRESERVE ROWS;

INSERT INTO pg_temp.pgdm_expansion_timings VALUES (0,'bulk_load','Acquire stable source and target locks',clock_timestamp(),NULL,NULL,NULL,NULL);
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
UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=0;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES (1, 'execute', 'Initialize database transaction', clock_timestamp(), NULL, NULL, NULL, NULL);
SET LOCAL client_min_messages = warning;
UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() WHERE sequence_number = 1;

INSERT INTO pg_temp.pgdm_expansion_timings VALUES (2, 'execute', 'Acquire destination table locks', clock_timestamp(), NULL, NULL, NULL, NULL);
LOCK TABLE
    "public"."table1",
    "public"."table2",
    "public"."table3",
    "public"."table4",
    "public"."table5",
    "public"."table6"
IN SHARE ROW EXCLUSIVE MODE;
UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() WHERE sequence_number = 2;

INSERT INTO pg_temp.pgdm_expansion_timings VALUES (3, 'execute', 'Scan source and build dependency manifest', clock_timestamp(), NULL, NULL, NULL, NULL);
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

SELECT json_build_object(
    'raw_schemas', to_json(ARRAY['group001']::text[]),
    'expected_row_count', COALESCE((
        SELECT SUM(row_count)
        FROM "pgdm_cross_table_hash_manifest"
    ), 0),
    'expected_distinct_raw_hash_count', (
        SELECT COUNT(*)
        FROM (
            SELECT DISTINCT raw_hash
            FROM "pgdm_cross_table_hash_manifest"
        ) AS distinct_hashes
    ),
    'raw_hash_manifest', COALESCE((
        SELECT json_agg(
            json_build_object(
                'raw_schema', raw_schema,
                'raw_hash', raw_hash,
                'row_count', row_count
            )
            ORDER BY raw_schema, raw_hash NULLS FIRST
        )
        FROM "pgdm_cross_table_hash_manifest"
    ), '[]'::json),
    'source_columns', COALESCE((
        SELECT json_agg(
            json_build_object(
                'name', attribute.attname,
                'type', pg_catalog.format_type(attribute.atttypid, attribute.atttypmod)
            )
            ORDER BY attribute.attnum
        )
        FROM pg_attribute AS attribute
        JOIN pg_class AS relation
          ON relation.oid = attribute.attrelid
        JOIN pg_namespace AS namespace
          ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = 'public'
          AND relation.relname = 'raw_data'
          AND relation.relkind IN ('r', 'p')
          AND attribute.attnum > 0
          AND NOT attribute.attisdropped
    ), '[]'::json)
)::text;
UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() WHERE sequence_number = 3;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (5,'bulk_load','Build TEXT input and prove safe cast domain',clock_timestamp(),NULL,NULL,NULL,NULL);
      CREATE TEMP TABLE pipeline_work_ownership(owned boolean NOT NULL) ON COMMIT DROP;
DO $work_namespace$ BEGIN
  IF pg_catalog.to_regnamespace('pgdm_pipeline_work') IS NULL
     AND pg_catalog.has_database_privilege(current_user,current_database(),'CREATE') THEN
    CREATE SCHEMA pgdm_pipeline_work;
    INSERT INTO pg_temp.pipeline_work_ownership VALUES(true);
    CREATE TEMP TABLE pipeline_parallel_settings ON COMMIT DROP AS SELECT name,setting FROM pg_settings WHERE name IN ('max_parallel_workers_per_gather','min_parallel_table_scan_size','parallel_setup_cost','parallel_tuple_cost','enable_parallel_hash');
SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;
-- lab per-value numeric Memoize variant=function source_sha256=f99f64baec8438d3495b15da941ed7d73d4757428aadcf7dbd9371ba5a67c079
-- lab refined COPY source_sha256=acc36c542f61f9e528b09624882c363938ab1e6c6ff638424bfc7faec5e36f43
-- lab normalized text source_sha256=e336edd535f832b64fc3305da635f9cb9ad599ab72b28074e42238be9e8c6c1f
CREATE UNLOGGED TABLE pgdm_pipeline_work.pipeline_normalized_input AS
      SELECT s.raw_id,n.*,(pg_catalog.octet_length(c.integers)-pg_catalog.octet_length(pg_catalog.replace(c.integers,'|',''))=14 AND (c.integers COLLATE "C") !~ E'[^0-9|+-]|[0-9]{5}|[0-9][+-]|[+-][|+-]|[+-]$') AND (pg_catalog.octet_length(c.numerics)-pg_catalog.octet_length(pg_catalog.replace(c.numerics,'|',''))=29 AND (c.numerics COLLATE "C") !~ E'[^0-9|+.-]|[0-9]{7}|[.][0-9]{4}|[.][0-9]*[.]|[.]([|]|$)|(^|[|])[+-]?[.]|[0-9.][+-]|[+-][|+.-]|[+-]$') AND (c.identifier COLLATE "C") ~ E'\\A(?:[+-]?(?:[0-9]{1,9}|[01][0-9]{9}))?\\Z' AND (CASE WHEN c.stamp='' THEN true WHEN (c.stamp COLLATE "C") ~ E'\\A[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])[ T](?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:[.][0-9]{1,6})?(?:Z|[+-](?:0[0-9]|1[0-2])(?::?[0-5][0-9])?)?\\Z' THEN
      pg_catalog.substr(c.stamp,1,4)::integer>0 AND pg_catalog.substr(c.stamp,9,2)::integer<=CASE WHEN pg_catalog.substr(c.stamp,6,2)='02' THEN 28+CASE WHEN pg_catalog.substr(c.stamp,1,4)::integer%4=0 AND (pg_catalog.substr(c.stamp,1,4)::integer%100<>0 OR pg_catalog.substr(c.stamp,1,4)::integer%400=0) THEN 1 ELSE 0 END
      WHEN pg_catalog.substr(c.stamp,6,2) IN ('04','06','09','11') THEN 30 ELSE 31 END ELSE false END) AS pipeline_domain_safe
      FROM (
    SELECT *
    FROM "public"."raw_data"
    WHERE "raw_schema" IN ('group001')
) s
      CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[s.raw->'values']) p(payload)
      CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW(p.payload->>'datetime_column_1',p.payload->>'id_column_1',p.payload->>'integer_column_1',p.payload->>'integer_column_10',p.payload->>'integer_column_11',p.payload->>'integer_column_12',p.payload->>'integer_column_13',p.payload->>'integer_column_14',p.payload->>'integer_column_15',p.payload->>'integer_column_2',p.payload->>'integer_column_3',p.payload->>'integer_column_4',p.payload->>'integer_column_5',p.payload->>'integer_column_6',p.payload->>'integer_column_7',p.payload->>'integer_column_8',p.payload->>'integer_column_9',p.payload->>'numeric_column_1',p.payload->>'numeric_column_10',p.payload->>'numeric_column_11',p.payload->>'numeric_column_12',p.payload->>'numeric_column_13',p.payload->>'numeric_column_14',p.payload->>'numeric_column_15',p.payload->>'numeric_column_16',p.payload->>'numeric_column_17',p.payload->>'numeric_column_18',p.payload->>'numeric_column_19',p.payload->>'numeric_column_2',p.payload->>'numeric_column_20',p.payload->>'numeric_column_21',p.payload->>'numeric_column_22',p.payload->>'numeric_column_23',p.payload->>'numeric_column_24',p.payload->>'numeric_column_25',p.payload->>'numeric_column_26',p.payload->>'numeric_column_27',p.payload->>'numeric_column_28',p.payload->>'numeric_column_29',p.payload->>'numeric_column_3',p.payload->>'numeric_column_30',p.payload->>'numeric_column_4',p.payload->>'numeric_column_5',p.payload->>'numeric_column_6',p.payload->>'numeric_column_7',p.payload->>'numeric_column_8',p.payload->>'numeric_column_9',pg_catalog.btrim(p.payload->>'text_column_1'),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_10')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_11')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_12')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_13')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_14')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_15')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_16')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_17')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_2')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_3')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_4')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_5')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_6')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_7')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_8')),pg_catalog.upper(pg_catalog.btrim(p.payload->>'text_column_9')))]) n(datetime_column_1 text,id_column_1 text,integer_column_1 text,integer_column_10 text,integer_column_11 text,integer_column_12 text,integer_column_13 text,integer_column_14 text,integer_column_15 text,integer_column_2 text,integer_column_3 text,integer_column_4 text,integer_column_5 text,integer_column_6 text,integer_column_7 text,integer_column_8 text,integer_column_9 text,numeric_column_1 text,numeric_column_10 text,numeric_column_11 text,numeric_column_12 text,numeric_column_13 text,numeric_column_14 text,numeric_column_15 text,numeric_column_16 text,numeric_column_17 text,numeric_column_18 text,numeric_column_19 text,numeric_column_2 text,numeric_column_20 text,numeric_column_21 text,numeric_column_22 text,numeric_column_23 text,numeric_column_24 text,numeric_column_25 text,numeric_column_26 text,numeric_column_27 text,numeric_column_28 text,numeric_column_29 text,numeric_column_3 text,numeric_column_30 text,numeric_column_4 text,numeric_column_5 text,numeric_column_6 text,numeric_column_7 text,numeric_column_8 text,numeric_column_9 text,text_column_1 text,text_column_10 text,text_column_11 text,text_column_12 text,text_column_13 text,text_column_14 text,text_column_15 text,text_column_16 text,text_column_17 text,text_column_2 text,text_column_3 text,text_column_4 text,text_column_5 text,text_column_6 text,text_column_7 text,text_column_8 text,text_column_9 text)
CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW((((coalesce(n.integer_column_1,'')||'|'||(coalesce(n.integer_column_2,'')||'|'||coalesce(n.integer_column_3,'')))||'|'||((coalesce(n.integer_column_4,'')||'|'||coalesce(n.integer_column_5,''))||'|'||(coalesce(n.integer_column_6,'')||'|'||coalesce(n.integer_column_7,''))))||'|'||(((coalesce(n.integer_column_8,'')||'|'||coalesce(n.integer_column_9,''))||'|'||(coalesce(n.integer_column_10,'')||'|'||coalesce(n.integer_column_11,'')))||'|'||((coalesce(n.integer_column_12,'')||'|'||coalesce(n.integer_column_13,''))||'|'||(coalesce(n.integer_column_14,'')||'|'||coalesce(n.integer_column_15,''))))),((((coalesce(n.numeric_column_1,'')||'|'||(coalesce(n.numeric_column_2,'')||'|'||coalesce(n.numeric_column_3,'')))||'|'||((coalesce(n.numeric_column_4,'')||'|'||coalesce(n.numeric_column_5,''))||'|'||(coalesce(n.numeric_column_6,'')||'|'||coalesce(n.numeric_column_7,''))))||'|'||(((coalesce(n.numeric_column_8,'')||'|'||coalesce(n.numeric_column_9,''))||'|'||(coalesce(n.numeric_column_10,'')||'|'||coalesce(n.numeric_column_11,'')))||'|'||((coalesce(n.numeric_column_12,'')||'|'||coalesce(n.numeric_column_13,''))||'|'||(coalesce(n.numeric_column_14,'')||'|'||coalesce(n.numeric_column_15,'')))))||'|'||(((coalesce(n.numeric_column_16,'')||'|'||(coalesce(n.numeric_column_17,'')||'|'||coalesce(n.numeric_column_18,'')))||'|'||((coalesce(n.numeric_column_19,'')||'|'||coalesce(n.numeric_column_20,''))||'|'||(coalesce(n.numeric_column_21,'')||'|'||coalesce(n.numeric_column_22,''))))||'|'||(((coalesce(n.numeric_column_23,'')||'|'||coalesce(n.numeric_column_24,''))||'|'||(coalesce(n.numeric_column_25,'')||'|'||coalesce(n.numeric_column_26,'')))||'|'||((coalesce(n.numeric_column_27,'')||'|'||coalesce(n.numeric_column_28,''))||'|'||(coalesce(n.numeric_column_29,'')||'|'||coalesce(n.numeric_column_30,'')))))),coalesce(n.id_column_1,''),coalesce(n.datetime_column_1,''))]) c(integers text,numerics text,identifier text,stamp text);
DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;
CREATE TEMP TABLE pipeline_normalization_domain ON COMMIT DROP AS SELECT coalesce(bool_and(pipeline_domain_safe),true) AND NOT EXISTS(SELECT 1 FROM public.table3) AND NOT EXISTS(SELECT 1 FROM public.table4) AND NOT EXISTS(SELECT 1 FROM public.table1) AND NOT EXISTS(SELECT 1 FROM public.table5) AND NOT EXISTS(SELECT 1 FROM public.table6) AND NOT EXISTS(SELECT 1 FROM public.table2) AS safe FROM pgdm_pipeline_work.pipeline_normalized_input;
  ELSE
    INSERT INTO pg_temp.pipeline_work_ownership VALUES(false);
    CREATE TEMP TABLE pipeline_normalization_domain ON COMMIT DROP AS SELECT false AS safe;
  END IF;
END $work_namespace$;
DO $copy_layout$ DECLARE previous_path text:=pg_catalog.current_setting('search_path'); BEGIN PERFORM pg_catalog.set_config('search_path','pg_catalog',true); UPDATE pg_temp.pipeline_normalization_domain SET safe=safe AND ((SELECT count(*) FROM pg_attribute WHERE attrelid IN ('public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass) AND attnum>0 AND NOT attisdropped)=45
      AND NOT EXISTS(SELECT 1 FROM (VALUES ('table3',1,'id_column_1','bigint','d',true),('table3',2,'text_column_1','character varying','',false),('table3',3,'text_column_2','character varying','',false),('table3',4,'numeric_column_1','numeric','',false),('table3',5,'text_column_3','character varying','',false),('table3',6,'text_column_4','character varying','',false),('table4',1,'id_column_1','smallint','',true),('table4',2,'text_column_1','character varying','',false),('table4',3,'text_column_2','character varying','',false),('table1',1,'id_column_1','bigint','d',true),('table1',2,'id_column_2','integer','',false),('table1',3,'text_column_1','character varying','',false),('table1',4,'datetime_column_1','timestamp without time zone','',false),('table1',5,'text_column_2','character varying','',false),('table1',6,'integer_column_1','smallint','',false),('table1',7,'integer_column_2','smallint','',false),('table1',8,'integer_column_3','integer','',false),('table1',9,'text_column_3','character varying','',false),('table1',10,'id_column_3','bigint','',true),('table1',11,'id_column_4','bigint','',false),('table1',12,'id_column_5','bigint','',false),('table1',13,'integer_column_4','integer','',false),('table1',14,'integer_column_5','integer','',false),('table1',15,'id_column_6','integer','',false),('table1',16,'integer_column_6','integer','',false),('table1',17,'integer_column_7','integer','',false),('table5',1,'id_column_1','bigint','d',true),('table5',2,'numeric_column_1','numeric','',false),('table5',3,'numeric_column_2','numeric','',false),('table5',4,'integer_column_1','integer','',false),('table5',5,'integer_column_2','integer','',false),('table5',6,'id_column_2','bigint','',true),('table5',7,'id_column_3','bigint','',false),('table6',1,'id_column_1','bigint','d',true),('table6',2,'id_column_2','bigint','',true),('table6',3,'id_column_3','smallint','',false),('table2',1,'id_column_1','bigint','d',true),('table2',2,'id_column_2','bigint','',true),('table2',3,'integer_column_1','smallint','',true),('table2',4,'integer_column_2','integer','',false),('table2',5,'text_column_1','character varying','',false),('table2',6,'integer_column_3','integer','',false),('table2',7,'integer_column_4','integer','',false),('table2',8,'integer_column_5','integer','',false),('table2',9,'integer_column_6','integer','',false)) e(table_name,ordinal,name,kind,identity_kind,required)
        LEFT JOIN pg_attribute a ON a.attrelid=('public.'||e.table_name)::regclass AND a.attnum=e.ordinal
        WHERE a.attname IS DISTINCT FROM e.name OR format_type(a.atttypid,a.atttypmod) IS DISTINCT FROM e.kind
          OR a.attidentity::text IS DISTINCT FROM e.identity_kind OR a.attnotnull IS DISTINCT FROM e.required
          OR a.attgenerated<>'' OR a.attisdropped
          OR a.attcollation<>CASE WHEN e.kind IN ('text','character varying') THEN 'default'::regcollation ELSE 0::oid END)
      AND (SELECT count(*) FROM pg_index WHERE indrelid IN ('public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass))=11
      AND NOT EXISTS(SELECT 1 FROM (VALUES ('table3','table3_pkey','CREATE UNIQUE INDEX table3_pkey ON public.table3 USING btree (id_column_1)'),('table4','table4_pkey','CREATE UNIQUE INDEX table4_pkey ON public.table4 USING btree (id_column_1)'),('table1','table1_pkey','CREATE UNIQUE INDEX table1_pkey ON public.table1 USING btree (id_column_1)'),('table5','table5_pkey','CREATE UNIQUE INDEX table5_pkey ON public.table5 USING btree (id_column_1)'),('table6','table6_pkey','CREATE UNIQUE INDEX table6_pkey ON public.table6 USING btree (id_column_1)'),('table2','table2_pkey','CREATE UNIQUE INDEX table2_pkey ON public.table2 USING btree (id_column_1)'),('table1','table1_id_column_3_key','CREATE UNIQUE INDEX table1_id_column_3_key ON public.table1 USING btree (id_column_3)'),('table2','table2_id_column_2_integer_column_1_key','CREATE UNIQUE INDEX table2_id_column_2_integer_column_1_key ON public.table2 USING btree (id_column_2, integer_column_1)'),('table6','table6_id_column_2_id_column_3_key','CREATE UNIQUE INDEX table6_id_column_2_id_column_3_key ON public.table6 USING btree (id_column_2, id_column_3)'),('table5','table5_id_column_3_not_null_key','CREATE UNIQUE INDEX table5_id_column_3_not_null_key ON public.table5 USING btree (id_column_3) WHERE (id_column_3 IS NOT NULL)'),('table5','table5_id_column_2_when_id_column_3_null_key','CREATE UNIQUE INDEX table5_id_column_2_when_id_column_3_null_key ON public.table5 USING btree (id_column_2) WHERE (id_column_3 IS NULL)')) e(table_name,index_name,definition)
        LEFT JOIN pg_class x ON x.relnamespace='public'::regnamespace AND x.relname=e.index_name
        LEFT JOIN pg_index i ON i.indexrelid=x.oid AND i.indrelid=('public.'||e.table_name)::regclass
        WHERE i.indexrelid IS NULL OR NOT i.indisvalid OR NOT i.indisready OR pg_get_indexdef(i.indexrelid) IS DISTINCT FROM e.definition)
      AND (SELECT count(*) FROM pg_constraint WHERE conrelid IN ('public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass))=16
      AND NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid IN ('public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass) AND (condeferrable OR NOT convalidated))
      AND NOT EXISTS(SELECT 1 FROM (VALUES ('table1','table1_id_column_3_fkey','FOREIGN KEY (id_column_3) REFERENCES public.raw_data(raw_id) ON UPDATE CASCADE ON DELETE CASCADE'),('table1','table1_id_column_4_fkey','FOREIGN KEY (id_column_4) REFERENCES public.table3(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE'),('table2','table2_id_column_2_fkey','FOREIGN KEY (id_column_2) REFERENCES public.table1(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE'),('table5','table5_id_column_2_fkey','FOREIGN KEY (id_column_2) REFERENCES public.table1(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE'),('table6','table6_id_column_2_fkey','FOREIGN KEY (id_column_2) REFERENCES public.table1(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE'),('table6','table6_id_column_3_fkey','FOREIGN KEY (id_column_3) REFERENCES public.table4(id_column_1) ON UPDATE CASCADE ON DELETE CASCADE')) e(table_name,constraint_name,definition)
        LEFT JOIN pg_constraint k ON k.conrelid=('public.'||e.table_name)::regclass AND k.conname=e.constraint_name AND k.contype='f'
        WHERE k.oid IS NULL OR NOT k.convalidated OR k.condeferrable OR pg_get_constraintdef(k.oid) IS DISTINCT FROM e.definition)
      AND EXISTS(SELECT 1 FROM pg_constraint WHERE conrelid='public.table2'::regclass AND conname='table2_text_column_1_check'
        AND convalidated AND pg_get_constraintdef(oid) IN ('CHECK (((text_column_1 IS NULL) OR ((text_column_1)::text = ANY ((ARRAY[''X''::character varying, ''Y''::character varying, ''Z''::character varying])::text[]))))','CHECK (((text_column_1 IS NULL) OR ((text_column_1)::text = ANY (ARRAY[(''X''::character varying)::text, (''Y''::character varying)::text, (''Z''::character varying)::text]))))'))
      AND NOT EXISTS(SELECT 1 FROM pg_class WHERE oid IN ('public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass) AND (relkind<>'r' OR relispartition OR relrowsecurity))
      AND NOT EXISTS(SELECT 1 FROM pg_class WHERE oid IN ('public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass) AND NOT pg_has_role(current_user,relowner,'USAGE'))
      AND NOT EXISTS(SELECT 1 FROM pg_inherits WHERE inhrelid IN ('public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass) OR inhparent IN ('public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass))
      AND NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid IN ('public.table3'::regclass,'public.table4'::regclass,'public.table1'::regclass,'public.table5'::regclass,'public.table6'::regclass,'public.table2'::regclass) AND NOT tgisinternal)); PERFORM pg_catalog.set_config('search_path',previous_path,true); END $copy_layout$;
DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
PERFORM pg_catalog.set_config('work_mem','64MB',true); ANALYZE pgdm_pipeline_work.pipeline_normalized_input(raw_id,text_column_1,text_column_2,text_column_3);
END IF; END $guarded_bulk$;
UPDATE pg_temp.pgdm_expansion_timings SET rows_affected=(SELECT safe::integer FROM pg_temp.pipeline_normalization_domain) WHERE sequence_number=5;
CREATE TEMP TABLE pipeline_copy_receipts(table_name text PRIMARY KEY,affected_rows bigint NOT NULL CHECK(affected_rows>=0)) ON COMMIT DROP;
SELECT safe AS pipeline_copy_safe FROM pg_temp.pipeline_normalization_domain
\gset

      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=5;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (4,'bulk_load','Prepare transactional bulk FK load',clock_timestamp(),NULL,NULL,NULL,NULL);
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
      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=4;

INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (203,'bulk_load','Transform and COPY table3 with native row receipt',clock_timestamp(),NULL,NULL,NULL,NULL);
-- lab server-file COPY source_sha256=3ba660db347d4eee967a9e290bcf13b9eaf90fb56814bf075cc800ee3d154294
SELECT pg_catalog.pg_has_role(current_user,'pg_read_server_files','USAGE') AND pg_catalog.pg_has_role(current_user,'pg_write_server_files','USAGE') AS pipeline_server_file_copy
\gset
\echo PGDM_SERVER_FILE_COPY=:pipeline_server_file_copy
      -- pipeline native psql copy v1
\if :pipeline_copy_safe
SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;
\if :pipeline_server_file_copy
COPY (SELECT DISTINCT (source_row.text_column_1), 'SYNTHETIC/REFERENCE', NULL::numeric, LEFT(((source_row.text_column_2)), 1), RIGHT(((source_row.text_column_2)), 1) FROM pgdm_pipeline_work.pipeline_normalized_input AS source_row WHERE (source_row.text_column_1) <> '' AND ((source_row.text_column_2)) ~ '^[A-Z]{2}$') TO 'C:/Users/c9408/Desktop/Python & PostgreSQL Data Pipeline Optimization Challenge/.runtime/native_copy/copy_batch_dg652onj/table3.bin' (FORMAT BINARY);
\else
\copy (SELECT DISTINCT (source_row.text_column_1), 'SYNTHETIC/REFERENCE', NULL::numeric, LEFT(((source_row.text_column_2)), 1), RIGHT(((source_row.text_column_2)), 1) FROM pgdm_pipeline_work.pipeline_normalized_input AS source_row WHERE (source_row.text_column_1) <> '' AND ((source_row.text_column_2)) ~ '^[A-Z]{2}$') TO 'table3.bin' (FORMAT BINARY)
\endif
\if :pipeline_server_file_copy
COPY public.table3 (text_column_1,text_column_2,numeric_column_1,text_column_3,text_column_4) FROM 'C:/Users/c9408/Desktop/Python & PostgreSQL Data Pipeline Optimization Challenge/.runtime/native_copy/copy_batch_dg652onj/table3.bin' (FORMAT BINARY);
\else
\copy public.table3 (text_column_1,text_column_2,numeric_column_1,text_column_3,text_column_4) FROM 'table3.bin' (FORMAT BINARY)
\endif
INSERT INTO pg_temp.pipeline_copy_receipts VALUES('table3',:'ROW_COUNT'::bigint);
DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;
\endif

      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=203;
DO $pgdm_managed_sql$
DECLARE
    pgdm_affected_rows bigint;
    pipeline_copy_fast boolean := (SELECT safe FROM pg_temp.pipeline_normalization_domain);
    pgdm_timing_started_at timestamptz := clock_timestamp();
BEGIN
    IF pipeline_copy_fast THEN
      SELECT affected_rows INTO STRICT pgdm_affected_rows FROM pg_temp.pipeline_copy_receipts WHERE table_name='table3';
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
      GET DIAGNOSTICS pgdm_affected_rows = ROW_COUNT;
    END IF
    ;
    

    UPDATE "public"."pgdm_table_row_counts"
    SET row_count = row_count + pgdm_affected_rows,
        updated_at = clock_timestamp()
    WHERE schema_name = 'public'
      AND table_name = 'table3';

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'Contador ausente para %.% durante insercao.',
            'public',
            'table3';
    END IF;


    INSERT INTO pg_temp.pgdm_expansion_timings (
        sequence_number,
        stage_key,
        item_name,
        started_at,
        finished_at,
        rows_affected,
        destination_name,
        statement_index
    ) VALUES (
        10,
        'execute',
        'Insert into public.table3 (statement 1)',
        pgdm_timing_started_at,
        clock_timestamp(),
        pgdm_affected_rows,
        'public.table3',
        1
    );
END
$pgdm_managed_sql$;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (303,'bulk_load','Analyze fresh table3 dimension',clock_timestamp(),NULL,NULL,NULL,NULL);
      DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
ANALYZE public.table3;
END IF; END $guarded_bulk$;
      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=303;

INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (204,'bulk_load','Transform and COPY table4 with native row receipt',clock_timestamp(),NULL,NULL,NULL,NULL);
      -- pipeline native psql copy v1
\if :pipeline_copy_safe
SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;
\if :pipeline_server_file_copy
COPY (SELECT DISTINCT ( (ASCII(LEFT(((source_row.text_column_3)), 1)) - ASCII('A')) * 26 + ASCII(RIGHT(((source_row.text_column_3)), 1)) - ASCII('A') + 1 )::smallint, 'SYNTHETIC/GENERIC', ((source_row.text_column_3)) FROM pgdm_pipeline_work.pipeline_normalized_input AS source_row WHERE ((source_row.text_column_3)) ~ '^[A-Z]{2}$') TO 'C:/Users/c9408/Desktop/Python & PostgreSQL Data Pipeline Optimization Challenge/.runtime/native_copy/copy_batch_dg652onj/table4.bin' (FORMAT BINARY);
\else
\copy (SELECT DISTINCT ( (ASCII(LEFT(((source_row.text_column_3)), 1)) - ASCII('A')) * 26 + ASCII(RIGHT(((source_row.text_column_3)), 1)) - ASCII('A') + 1 )::smallint, 'SYNTHETIC/GENERIC', ((source_row.text_column_3)) FROM pgdm_pipeline_work.pipeline_normalized_input AS source_row WHERE ((source_row.text_column_3)) ~ '^[A-Z]{2}$') TO 'table4.bin' (FORMAT BINARY)
\endif
\if :pipeline_server_file_copy
COPY public.table4 (id_column_1,text_column_1,text_column_2) FROM 'C:/Users/c9408/Desktop/Python & PostgreSQL Data Pipeline Optimization Challenge/.runtime/native_copy/copy_batch_dg652onj/table4.bin' (FORMAT BINARY);
\else
\copy public.table4 (id_column_1,text_column_1,text_column_2) FROM 'table4.bin' (FORMAT BINARY)
\endif
INSERT INTO pg_temp.pipeline_copy_receipts VALUES('table4',:'ROW_COUNT'::bigint);
DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;
\endif

      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=204;
DO $pgdm_managed_sql$
DECLARE
    pgdm_affected_rows bigint;
    pipeline_copy_fast boolean := (SELECT safe FROM pg_temp.pipeline_normalization_domain);
    pgdm_timing_started_at timestamptz := clock_timestamp();
BEGIN
    IF pipeline_copy_fast THEN
      SELECT affected_rows INTO STRICT pgdm_affected_rows FROM pg_temp.pipeline_copy_receipts WHERE table_name='table4';
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
      GET DIAGNOSTICS pgdm_affected_rows = ROW_COUNT;
    END IF
    ;
    

    UPDATE "public"."pgdm_table_row_counts"
    SET row_count = row_count + pgdm_affected_rows,
        updated_at = clock_timestamp()
    WHERE schema_name = 'public'
      AND table_name = 'table4';

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'Contador ausente para %.% durante insercao.',
            'public',
            'table4';
    END IF;


    INSERT INTO pg_temp.pgdm_expansion_timings (
        sequence_number,
        stage_key,
        item_name,
        started_at,
        finished_at,
        rows_affected,
        destination_name,
        statement_index
    ) VALUES (
        11,
        'execute',
        'Insert into public.table4 (statement 1)',
        pgdm_timing_started_at,
        clock_timestamp(),
        pgdm_affected_rows,
        'public.table4',
        1
    );
END
$pgdm_managed_sql$;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (304,'bulk_load','Analyze fresh table4 dimension',clock_timestamp(),NULL,NULL,NULL,NULL);
      DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN
ANALYZE public.table4;
END IF; END $guarded_bulk$;
      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=304;

INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (110,'bulk_load','Prepare table1 key bulk build',clock_timestamp(),NULL,NULL,NULL,NULL);
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
DO $sequence_cache_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      SELECT s.seqrelid,s.seqcache INTO item FROM pg_sequence s JOIN pg_attribute a
        ON a.attrelid='public.table1'::regclass AND a.attname='id_column_1'
      WHERE s.seqrelid=pg_get_serial_sequence('public.table1','id_column_1')::regclass
        AND a.attidentity IN ('a','d') AND a.atttypid='bigint'::regtype
        AND s.seqtypid='bigint'::regtype AND NOT s.seqcycle AND s.seqincrement=1 AND s.seqcache=1;
      IF item.seqrelid IS NOT NULL THEN
        CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_sequence_settings(table_name text,sequence_oid oid,cache_size bigint) ON COMMIT DROP;
        INSERT INTO pg_temp.pipeline_bulk_sequence_settings VALUES('table1',item.seqrelid,item.seqcache);
        EXECUTE format('ALTER SEQUENCE %s CACHE 1000',item.seqrelid::regclass);
      END IF;
      PERFORM pg_catalog.set_config('search_path',previous_path,true);
    END $sequence_cache_prepare$;
END IF; END $guarded_bulk$;
      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=110;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (201,'bulk_load','Transform and COPY table1 with native row receipt',clock_timestamp(),NULL,NULL,NULL,NULL);
      -- pipeline native psql copy v1
\if :pipeline_copy_safe
SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;
SET LOCAL enable_parallel_hash=off;
\if :pipeline_server_file_copy
COPY (SELECT NULLIF(BTRIM(source_row.id_column_1), '')::integer, NULL::character varying, NULLIF(BTRIM(source_row.datetime_column_1), '')::timestamptz AT TIME ZONE 'America/Sao_Paulo', NULL::character varying, GREATEST( NULLIF(BTRIM(source_row.integer_column_2), '')::smallint, NULLIF(BTRIM(source_row.integer_column_3), '')::smallint, NULLIF(BTRIM(source_row.integer_column_4), '')::smallint, NULLIF(BTRIM(source_row.integer_column_5), '')::smallint, NULLIF(BTRIM(source_row.integer_column_6), '')::smallint, NULLIF(BTRIM(source_row.integer_column_7), '')::smallint, NULLIF(BTRIM(source_row.integer_column_8), '')::smallint, NULLIF(BTRIM(source_row.integer_column_9), '')::smallint, NULLIF(BTRIM(source_row.integer_column_10), '')::smallint, NULLIF(BTRIM(source_row.integer_column_11), '')::smallint, NULLIF(BTRIM(source_row.integer_column_12), '')::smallint, NULLIF(BTRIM(source_row.integer_column_13), '')::smallint, NULLIF(BTRIM(source_row.integer_column_14), '')::smallint, NULLIF(BTRIM(source_row.integer_column_15), '')::smallint ), NULL::smallint, CASE WHEN NULLIF(BTRIM(source_row.numeric_column_2), '')::numeric > 0 THEN ROUND( NULLIF(BTRIM(source_row.numeric_column_2), '')::numeric * 1000 )::integer ELSE NULL::integer END, NULL::character varying, source_row.raw_id, dimension_row.id_column_1, NULL::bigint, NULL::integer, NULL::integer, NULL::integer, NULL::integer, CASE WHEN NULLIF(BTRIM(source_row.numeric_column_1), '')::numeric > 0 THEN ROUND( NULLIF(BTRIM(source_row.numeric_column_1), '')::numeric * 1000 )::integer ELSE NULL::integer END FROM pgdm_pipeline_work.pipeline_normalized_input AS source_row JOIN public.table3 AS dimension_row ON dimension_row.text_column_1 = (source_row.text_column_1) AND dimension_row.text_column_2 = 'SYNTHETIC/REFERENCE' AND dimension_row.numeric_column_1 IS NULL AND dimension_row.text_column_3 = LEFT( ((source_row.text_column_2)), 1 ) AND dimension_row.text_column_4 = RIGHT( ((source_row.text_column_2)), 1 )) TO 'C:/Users/c9408/Desktop/Python & PostgreSQL Data Pipeline Optimization Challenge/.runtime/native_copy/copy_batch_dg652onj/table1.bin' (FORMAT BINARY);
\else
\copy (SELECT NULLIF(BTRIM(source_row.id_column_1), '')::integer, NULL::character varying, NULLIF(BTRIM(source_row.datetime_column_1), '')::timestamptz AT TIME ZONE 'America/Sao_Paulo', NULL::character varying, GREATEST( NULLIF(BTRIM(source_row.integer_column_2), '')::smallint, NULLIF(BTRIM(source_row.integer_column_3), '')::smallint, NULLIF(BTRIM(source_row.integer_column_4), '')::smallint, NULLIF(BTRIM(source_row.integer_column_5), '')::smallint, NULLIF(BTRIM(source_row.integer_column_6), '')::smallint, NULLIF(BTRIM(source_row.integer_column_7), '')::smallint, NULLIF(BTRIM(source_row.integer_column_8), '')::smallint, NULLIF(BTRIM(source_row.integer_column_9), '')::smallint, NULLIF(BTRIM(source_row.integer_column_10), '')::smallint, NULLIF(BTRIM(source_row.integer_column_11), '')::smallint, NULLIF(BTRIM(source_row.integer_column_12), '')::smallint, NULLIF(BTRIM(source_row.integer_column_13), '')::smallint, NULLIF(BTRIM(source_row.integer_column_14), '')::smallint, NULLIF(BTRIM(source_row.integer_column_15), '')::smallint ), NULL::smallint, CASE WHEN NULLIF(BTRIM(source_row.numeric_column_2), '')::numeric > 0 THEN ROUND( NULLIF(BTRIM(source_row.numeric_column_2), '')::numeric * 1000 )::integer ELSE NULL::integer END, NULL::character varying, source_row.raw_id, dimension_row.id_column_1, NULL::bigint, NULL::integer, NULL::integer, NULL::integer, NULL::integer, CASE WHEN NULLIF(BTRIM(source_row.numeric_column_1), '')::numeric > 0 THEN ROUND( NULLIF(BTRIM(source_row.numeric_column_1), '')::numeric * 1000 )::integer ELSE NULL::integer END FROM pgdm_pipeline_work.pipeline_normalized_input AS source_row JOIN public.table3 AS dimension_row ON dimension_row.text_column_1 = (source_row.text_column_1) AND dimension_row.text_column_2 = 'SYNTHETIC/REFERENCE' AND dimension_row.numeric_column_1 IS NULL AND dimension_row.text_column_3 = LEFT( ((source_row.text_column_2)), 1 ) AND dimension_row.text_column_4 = RIGHT( ((source_row.text_column_2)), 1 )) TO 'table1.bin' (FORMAT BINARY)
\endif
\if :pipeline_server_file_copy
COPY public.table1 (id_column_2,text_column_1,datetime_column_1,text_column_2,integer_column_1,integer_column_2,integer_column_3,text_column_3,id_column_3,id_column_4,id_column_5,integer_column_4,integer_column_5,id_column_6,integer_column_6,integer_column_7) FROM 'C:/Users/c9408/Desktop/Python & PostgreSQL Data Pipeline Optimization Challenge/.runtime/native_copy/copy_batch_dg652onj/table1.bin' (FORMAT BINARY);
\else
\copy public.table1 (id_column_2,text_column_1,datetime_column_1,text_column_2,integer_column_1,integer_column_2,integer_column_3,text_column_3,id_column_3,id_column_4,id_column_5,integer_column_4,integer_column_5,id_column_6,integer_column_6,integer_column_7) FROM 'table1.bin' (FORMAT BINARY)
\endif
INSERT INTO pg_temp.pipeline_copy_receipts VALUES('table1',:'ROW_COUNT'::bigint);
DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;
\endif

      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=201;
DO $pgdm_managed_sql$
DECLARE
    pgdm_affected_rows bigint;
    pipeline_copy_fast boolean := (SELECT safe FROM pg_temp.pipeline_normalization_domain);
    pgdm_timing_started_at timestamptz := clock_timestamp();
BEGIN
    IF pipeline_copy_fast THEN
      SELECT affected_rows INTO STRICT pgdm_affected_rows FROM pg_temp.pipeline_copy_receipts WHERE table_name='table1';
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
      GET DIAGNOSTICS pgdm_affected_rows = ROW_COUNT;
    END IF
    ;
    

    UPDATE "public"."pgdm_table_row_counts"
    SET row_count = row_count + pgdm_affected_rows,
        updated_at = clock_timestamp()
    WHERE schema_name = 'public'
      AND table_name = 'table1';

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'Contador ausente para %.% durante insercao.',
            'public',
            'table1';
    END IF;


    INSERT INTO pg_temp.pgdm_expansion_timings (
        sequence_number,
        stage_key,
        item_name,
        started_at,
        finished_at,
        rows_affected,
        destination_name,
        statement_index
    ) VALUES (
        12,
        'execute',
        'Insert into public.table1 (statement 1)',
        pgdm_timing_started_at,
        clock_timestamp(),
        pgdm_affected_rows,
        'public.table1',
        1
    );
END
$pgdm_managed_sql$;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (111,'bulk_load','Rebuild and validate table1 keys',clock_timestamp(),NULL,NULL,NULL,NULL);
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
DO $sequence_cache_restore$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF to_regclass('pg_temp.pipeline_bulk_sequence_settings') IS NOT NULL THEN
        FOR item IN SELECT * FROM pg_temp.pipeline_bulk_sequence_settings WHERE table_name='table1' LOOP
          EXECUTE format('ALTER SEQUENCE %s CACHE %s',item.sequence_oid::regclass,item.cache_size);
          DELETE FROM pg_temp.pipeline_bulk_sequence_settings WHERE table_name=item.table_name;
        END LOOP;
      END IF;
      PERFORM pg_catalog.set_config('search_path',previous_path,true);
    END $sequence_cache_restore$;
END IF; END $guarded_bulk$;
      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=111;

INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (150,'bulk_load','Prepare table5 key bulk build',clock_timestamp(),NULL,NULL,NULL,NULL);
      DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN

DO $partial_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      SELECT x.relname,pg_get_indexdef(x.oid) AS definition INTO item
      FROM pg_index i JOIN pg_class x ON x.oid=i.indexrelid JOIN pg_am am ON am.oid=x.relam
      WHERE i.indrelid='public.table5'::regclass AND x.relnamespace='public'::regnamespace
        AND x.relname='table5_id_column_2_when_id_column_3_null_key' AND x.relkind='i' AND am.amname='btree'
        AND i.indisunique AND i.indisvalid AND i.indisready AND NOT i.indisprimary
        AND NOT i.indisclustered AND NOT i.indisreplident AND NOT i.indnullsnotdistinct
        AND i.indnkeyatts=1 AND i.indnatts=1 AND i.indexprs IS NULL
        AND i.indkey[0]=(SELECT attnum FROM pg_attribute WHERE attrelid=i.indrelid AND attname='id_column_2' AND atttypid='bigint'::regtype)
        AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace AND opcname='int8_ops' AND opcmethod=x.relam)
        AND x.reloptions IS NULL AND x.reltablespace=0
        AND pg_get_expr(i.indpred,i.indrelid)='(id_column_3 IS NULL)'
        AND NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conindid=x.oid);
      IF item.relname IS NULL OR item.definition<>'CREATE UNIQUE INDEX table5_id_column_2_when_id_column_3_null_key ON public.table5 USING btree (id_column_2) WHERE (id_column_3 IS NULL)' THEN PERFORM pg_catalog.set_config('search_path',previous_path,true); RETURN; END IF;
      CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_partial_indexes(table_name text,index_name text,definition text) ON COMMIT DROP;
      INSERT INTO pg_temp.pipeline_bulk_partial_indexes VALUES('table5',item.relname,item.definition);
      DROP INDEX public.table5_id_column_2_when_id_column_3_null_key;
      PERFORM pg_catalog.set_config('search_path',previous_path,true);
    END $partial_prepare$;
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
DO $sequence_cache_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      SELECT s.seqrelid,s.seqcache INTO item FROM pg_sequence s JOIN pg_attribute a
        ON a.attrelid='public.table5'::regclass AND a.attname='id_column_1'
      WHERE s.seqrelid=pg_get_serial_sequence('public.table5','id_column_1')::regclass
        AND a.attidentity IN ('a','d') AND a.atttypid='bigint'::regtype
        AND s.seqtypid='bigint'::regtype AND NOT s.seqcycle AND s.seqincrement=1 AND s.seqcache=1;
      IF item.seqrelid IS NOT NULL THEN
        CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_sequence_settings(table_name text,sequence_oid oid,cache_size bigint) ON COMMIT DROP;
        INSERT INTO pg_temp.pipeline_bulk_sequence_settings VALUES('table5',item.seqrelid,item.seqcache);
        EXECUTE format('ALTER SEQUENCE %s CACHE 1000',item.seqrelid::regclass);
      END IF;
      PERFORM pg_catalog.set_config('search_path',previous_path,true);
    END $sequence_cache_prepare$;
END IF; END $guarded_bulk$;
      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=150;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (205,'bulk_load','Transform and COPY table5 with native row receipt',clock_timestamp(),NULL,NULL,NULL,NULL);
      -- pipeline native psql copy v1
\if :pipeline_copy_safe
SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;
\if :pipeline_server_file_copy
COPY (SELECT ROUND( NULLIF(BTRIM(source_row.integer_column_1), '')::numeric, 1 ), NULL::numeric, NULL::integer, NULL::integer, main_row.id_column_1, NULL::bigint FROM pgdm_pipeline_work.pipeline_normalized_input AS source_row JOIN public.table1 AS main_row ON main_row.id_column_3 = source_row.raw_id WHERE NULLIF(BTRIM(source_row.integer_column_1), '')::numeric > 0) TO 'C:/Users/c9408/Desktop/Python & PostgreSQL Data Pipeline Optimization Challenge/.runtime/native_copy/copy_batch_dg652onj/table5.bin' (FORMAT BINARY);
\else
\copy (SELECT ROUND( NULLIF(BTRIM(source_row.integer_column_1), '')::numeric, 1 ), NULL::numeric, NULL::integer, NULL::integer, main_row.id_column_1, NULL::bigint FROM pgdm_pipeline_work.pipeline_normalized_input AS source_row JOIN public.table1 AS main_row ON main_row.id_column_3 = source_row.raw_id WHERE NULLIF(BTRIM(source_row.integer_column_1), '')::numeric > 0) TO 'table5.bin' (FORMAT BINARY)
\endif
\if :pipeline_server_file_copy
COPY public.table5 (numeric_column_1,numeric_column_2,integer_column_1,integer_column_2,id_column_2,id_column_3) FROM 'C:/Users/c9408/Desktop/Python & PostgreSQL Data Pipeline Optimization Challenge/.runtime/native_copy/copy_batch_dg652onj/table5.bin' (FORMAT BINARY);
\else
\copy public.table5 (numeric_column_1,numeric_column_2,integer_column_1,integer_column_2,id_column_2,id_column_3) FROM 'table5.bin' (FORMAT BINARY)
\endif
INSERT INTO pg_temp.pipeline_copy_receipts VALUES('table5',:'ROW_COUNT'::bigint);
DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;
\endif

      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=205;
DO $pgdm_managed_sql$
DECLARE
    pgdm_affected_rows bigint;
    pipeline_copy_fast boolean := (SELECT safe FROM pg_temp.pipeline_normalization_domain);
    pgdm_timing_started_at timestamptz := clock_timestamp();
BEGIN
    IF pipeline_copy_fast THEN
      SELECT affected_rows INTO STRICT pgdm_affected_rows FROM pg_temp.pipeline_copy_receipts WHERE table_name='table5';
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
      GET DIAGNOSTICS pgdm_affected_rows = ROW_COUNT;
    END IF
    ;
    

    UPDATE "public"."pgdm_table_row_counts"
    SET row_count = row_count + pgdm_affected_rows,
        updated_at = clock_timestamp()
    WHERE schema_name = 'public'
      AND table_name = 'table5';

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'Contador ausente para %.% durante insercao.',
            'public',
            'table5';
    END IF;


    INSERT INTO pg_temp.pgdm_expansion_timings (
        sequence_number,
        stage_key,
        item_name,
        started_at,
        finished_at,
        rows_affected,
        destination_name,
        statement_index
    ) VALUES (
        13,
        'execute',
        'Insert into public.table5 (statement 1)',
        pgdm_timing_started_at,
        clock_timestamp(),
        pgdm_affected_rows,
        'public.table5',
        1
    );
END
$pgdm_managed_sql$;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (151,'bulk_load','Rebuild and validate table5 keys',clock_timestamp(),NULL,NULL,NULL,NULL);
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
DO $partial_restore$ DECLARE item record; previous_memory text:=pg_catalog.current_setting('maintenance_work_mem'); BEGIN
          IF to_regclass('pg_temp.pipeline_bulk_partial_indexes') IS NULL THEN RETURN; END IF;
          PERFORM pg_catalog.set_config('maintenance_work_mem','128MB',true);
          FOR item IN SELECT * FROM pg_temp.pipeline_bulk_partial_indexes WHERE table_name='table5' LOOP
            EXECUTE item.definition;
            DELETE FROM pg_temp.pipeline_bulk_partial_indexes WHERE index_name=item.index_name;
          END LOOP;
          PERFORM pg_catalog.set_config('maintenance_work_mem',previous_memory,true);
        END $partial_restore$;
DO $sequence_cache_restore$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF to_regclass('pg_temp.pipeline_bulk_sequence_settings') IS NOT NULL THEN
        FOR item IN SELECT * FROM pg_temp.pipeline_bulk_sequence_settings WHERE table_name='table5' LOOP
          EXECUTE format('ALTER SEQUENCE %s CACHE %s',item.sequence_oid::regclass,item.cache_size);
          DELETE FROM pg_temp.pipeline_bulk_sequence_settings WHERE table_name=item.table_name;
        END LOOP;
      END IF;
      PERFORM pg_catalog.set_config('search_path',previous_path,true);
    END $sequence_cache_restore$;
END IF; END $guarded_bulk$;
      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=151;

INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (160,'bulk_load','Prepare table6 key bulk build',clock_timestamp(),NULL,NULL,NULL,NULL);
      DO $guarded_bulk$ BEGIN IF (SELECT safe FROM pg_temp.pipeline_normalization_domain) THEN

DO $index_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      
      IF NOT EXISTS(SELECT 1 FROM pg_attribute a WHERE a.attrelid='public.table6'::regclass AND a.attname='id_column_3' AND a.atttypid='smallint'::regtype) THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      SELECT k.conname,k.conkey,k.conindid,pg_get_constraintdef(k.oid) AS definition INTO item
        FROM pg_constraint k JOIN pg_index i ON i.indexrelid=k.conindid JOIN pg_class x ON x.oid=i.indexrelid
        WHERE k.conrelid='public.table6'::regclass AND k.conname='table6_id_column_2_id_column_3_key' AND k.contype='u'
          AND k.convalidated AND NOT k.condeferrable AND i.indisvalid AND i.indisunique
          AND NOT i.indisclustered AND NOT i.indisreplident
          AND i.indnkeyatts=2 AND i.indnatts=2 AND i.indexprs IS NULL AND i.indpred IS NULL
          AND x.reltablespace=0 AND x.reloptions IS NULL AND i.indclass[0]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace AND opcname='int8_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree')) AND i.indclass[1]=(SELECT oid FROM pg_opclass WHERE opcnamespace='pg_catalog'::regnamespace AND opcname='int2_ops' AND opcmethod=(SELECT oid FROM pg_am WHERE amname='btree'));
      IF item.conname IS NULL OR item.definition<>'UNIQUE (id_column_2, id_column_3)' OR EXISTS(
         SELECT 1 FROM pg_constraint k WHERE k.contype='f' AND k.confrelid='public.table6'::regclass AND k.confkey=item.conkey)
      THEN PERFORM set_config('search_path',previous_path,true); RETURN; END IF;
      CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_unique_keys(table_name text,constraint_name text,definition text) ON COMMIT DROP;
      INSERT INTO pg_temp.pipeline_bulk_unique_keys VALUES('table6',item.conname,item.definition);
      EXECUTE format('ALTER TABLE public.%I DROP CONSTRAINT %I','table6',item.conname);
      PERFORM set_config('search_path',previous_path,true);
    END $index_prepare$;
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
DO $sequence_cache_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      SELECT s.seqrelid,s.seqcache INTO item FROM pg_sequence s JOIN pg_attribute a
        ON a.attrelid='public.table6'::regclass AND a.attname='id_column_1'
      WHERE s.seqrelid=pg_get_serial_sequence('public.table6','id_column_1')::regclass
        AND a.attidentity IN ('a','d') AND a.atttypid='bigint'::regtype
        AND s.seqtypid='bigint'::regtype AND NOT s.seqcycle AND s.seqincrement=1 AND s.seqcache=1;
      IF item.seqrelid IS NOT NULL THEN
        CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_sequence_settings(table_name text,sequence_oid oid,cache_size bigint) ON COMMIT DROP;
        INSERT INTO pg_temp.pipeline_bulk_sequence_settings VALUES('table6',item.seqrelid,item.seqcache);
        EXECUTE format('ALTER SEQUENCE %s CACHE 1000',item.seqrelid::regclass);
      END IF;
      PERFORM pg_catalog.set_config('search_path',previous_path,true);
    END $sequence_cache_prepare$;
END IF; END $guarded_bulk$;
      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=160;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (206,'bulk_load','Transform and COPY table6 with native row receipt',clock_timestamp(),NULL,NULL,NULL,NULL);
      -- pipeline native psql copy v1
\if :pipeline_copy_safe
SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;
\if :pipeline_server_file_copy
COPY (SELECT main_row.id_column_1, reference_row.id_column_1 FROM pgdm_pipeline_work.pipeline_normalized_input AS source_row JOIN public.table1 AS main_row ON main_row.id_column_3 = source_row.raw_id JOIN public.table4 AS reference_row ON reference_row.text_column_1 = 'SYNTHETIC/GENERIC' AND reference_row.text_column_2 = ( (source_row.text_column_3) )) TO 'C:/Users/c9408/Desktop/Python & PostgreSQL Data Pipeline Optimization Challenge/.runtime/native_copy/copy_batch_dg652onj/table6.bin' (FORMAT BINARY);
\else
\copy (SELECT main_row.id_column_1, reference_row.id_column_1 FROM pgdm_pipeline_work.pipeline_normalized_input AS source_row JOIN public.table1 AS main_row ON main_row.id_column_3 = source_row.raw_id JOIN public.table4 AS reference_row ON reference_row.text_column_1 = 'SYNTHETIC/GENERIC' AND reference_row.text_column_2 = ( (source_row.text_column_3) )) TO 'table6.bin' (FORMAT BINARY)
\endif
\if :pipeline_server_file_copy
COPY public.table6 (id_column_2,id_column_3) FROM 'C:/Users/c9408/Desktop/Python & PostgreSQL Data Pipeline Optimization Challenge/.runtime/native_copy/copy_batch_dg652onj/table6.bin' (FORMAT BINARY);
\else
\copy public.table6 (id_column_2,id_column_3) FROM 'table6.bin' (FORMAT BINARY)
\endif
INSERT INTO pg_temp.pipeline_copy_receipts VALUES('table6',:'ROW_COUNT'::bigint);
DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;
\endif

      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=206;
DO $pgdm_managed_sql$
DECLARE
    pgdm_affected_rows bigint;
    pipeline_copy_fast boolean := (SELECT safe FROM pg_temp.pipeline_normalization_domain);
    pgdm_timing_started_at timestamptz := clock_timestamp();
BEGIN
    IF pipeline_copy_fast THEN
      SELECT affected_rows INTO STRICT pgdm_affected_rows FROM pg_temp.pipeline_copy_receipts WHERE table_name='table6';
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
      GET DIAGNOSTICS pgdm_affected_rows = ROW_COUNT;
    END IF
    ;
    

    UPDATE "public"."pgdm_table_row_counts"
    SET row_count = row_count + pgdm_affected_rows,
        updated_at = clock_timestamp()
    WHERE schema_name = 'public'
      AND table_name = 'table6';

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'Contador ausente para %.% durante insercao.',
            'public',
            'table6';
    END IF;


    INSERT INTO pg_temp.pgdm_expansion_timings (
        sequence_number,
        stage_key,
        item_name,
        started_at,
        finished_at,
        rows_affected,
        destination_name,
        statement_index
    ) VALUES (
        14,
        'execute',
        'Insert into public.table6 (statement 1)',
        pgdm_timing_started_at,
        clock_timestamp(),
        pgdm_affected_rows,
        'public.table6',
        1
    );
END
$pgdm_managed_sql$;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (161,'bulk_load','Rebuild and validate table6 keys',clock_timestamp(),NULL,NULL,NULL,NULL);
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
DO $sequence_cache_restore$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF to_regclass('pg_temp.pipeline_bulk_sequence_settings') IS NOT NULL THEN
        FOR item IN SELECT * FROM pg_temp.pipeline_bulk_sequence_settings WHERE table_name='table6' LOOP
          EXECUTE format('ALTER SEQUENCE %s CACHE %s',item.sequence_oid::regclass,item.cache_size);
          DELETE FROM pg_temp.pipeline_bulk_sequence_settings WHERE table_name=item.table_name;
        END LOOP;
      END IF;
      PERFORM pg_catalog.set_config('search_path',previous_path,true);
    END $sequence_cache_restore$;
END IF; END $guarded_bulk$;
      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=161;

INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (120,'bulk_load','Prepare table2 key bulk build',clock_timestamp(),NULL,NULL,NULL,NULL);
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
DO $sequence_cache_prepare$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      SELECT s.seqrelid,s.seqcache INTO item FROM pg_sequence s JOIN pg_attribute a
        ON a.attrelid='public.table2'::regclass AND a.attname='id_column_1'
      WHERE s.seqrelid=pg_get_serial_sequence('public.table2','id_column_1')::regclass
        AND a.attidentity IN ('a','d') AND a.atttypid='bigint'::regtype
        AND s.seqtypid='bigint'::regtype AND NOT s.seqcycle AND s.seqincrement=1 AND s.seqcache=1;
      IF item.seqrelid IS NOT NULL THEN
        CREATE TEMP TABLE IF NOT EXISTS pipeline_bulk_sequence_settings(table_name text,sequence_oid oid,cache_size bigint) ON COMMIT DROP;
        INSERT INTO pg_temp.pipeline_bulk_sequence_settings VALUES('table2',item.seqrelid,item.seqcache);
        EXECUTE format('ALTER SEQUENCE %s CACHE 1000',item.seqrelid::regclass);
      END IF;
      PERFORM pg_catalog.set_config('search_path',previous_path,true);
    END $sequence_cache_prepare$;
END IF; END $guarded_bulk$;
      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=120;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (202,'bulk_load','Transform and COPY table2 with native row receipt',clock_timestamp(),NULL,NULL,NULL,NULL);
      -- pipeline native psql copy v1
\if :pipeline_copy_safe
SET LOCAL max_parallel_workers_per_gather=8; SET LOCAL min_parallel_table_scan_size=0; SET LOCAL parallel_setup_cost=0; SET LOCAL parallel_tuple_cost=0;
\if :pipeline_server_file_copy
COPY (SELECT main_row.id_column_1,position_keys.position_number, parsed_weight.scaled,raw_axis.axis_kind, NULL::integer,NULL::integer,NULL::integer, CASE WHEN parsed_gap.scaled>0 THEN parsed_gap.scaled ELSE NULL::integer END FROM pgdm_pipeline_work.pipeline_normalized_input source_row JOIN public.table1 main_row ON main_row.id_column_3=source_row.raw_id CROSS JOIN LATERAL pg_catalog.unnest((ARRAY[1::smallint,2::smallint,3::smallint,4::smallint,5::smallint,6::smallint,7::smallint,8::smallint,9::smallint,10::smallint,11::smallint,12::smallint,13::smallint,14::smallint])[1:LEAST(COALESCE(main_row.integer_column_1,0),14)]) position_keys(position_number) CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW(CASE position_keys.position_number WHEN 1 THEN NULLIF(BTRIM(source_row.integer_column_2),'')::smallint WHEN 2 THEN NULLIF(BTRIM(source_row.integer_column_3),'')::smallint WHEN 3 THEN NULLIF(BTRIM(source_row.integer_column_4),'')::smallint WHEN 4 THEN NULLIF(BTRIM(source_row.integer_column_5),'')::smallint WHEN 5 THEN NULLIF(BTRIM(source_row.integer_column_6),'')::smallint WHEN 6 THEN NULLIF(BTRIM(source_row.integer_column_7),'')::smallint WHEN 7 THEN NULLIF(BTRIM(source_row.integer_column_8),'')::smallint WHEN 8 THEN NULLIF(BTRIM(source_row.integer_column_9),'')::smallint WHEN 9 THEN NULLIF(BTRIM(source_row.integer_column_10),'')::smallint WHEN 10 THEN NULLIF(BTRIM(source_row.integer_column_11),'')::smallint WHEN 11 THEN NULLIF(BTRIM(source_row.integer_column_12),'')::smallint WHEN 12 THEN NULLIF(BTRIM(source_row.integer_column_13),'')::smallint WHEN 13 THEN NULLIF(BTRIM(source_row.integer_column_14),'')::smallint WHEN 14 THEN NULLIF(BTRIM(source_row.integer_column_15),'')::smallint END,CASE position_keys.position_number WHEN 1 THEN source_row.text_column_4 WHEN 2 THEN source_row.text_column_5 WHEN 3 THEN source_row.text_column_6 WHEN 4 THEN source_row.text_column_7 WHEN 5 THEN source_row.text_column_8 WHEN 6 THEN source_row.text_column_9 WHEN 7 THEN source_row.text_column_10 WHEN 8 THEN source_row.text_column_11 WHEN 9 THEN source_row.text_column_12 WHEN 10 THEN source_row.text_column_13 WHEN 11 THEN source_row.text_column_14 WHEN 12 THEN source_row.text_column_15 WHEN 13 THEN source_row.text_column_16 WHEN 14 THEN source_row.text_column_17 END,CASE position_keys.position_number WHEN 1 THEN source_row.numeric_column_3 WHEN 2 THEN source_row.numeric_column_5 WHEN 3 THEN source_row.numeric_column_7 WHEN 4 THEN source_row.numeric_column_9 WHEN 5 THEN source_row.numeric_column_11 WHEN 6 THEN source_row.numeric_column_13 WHEN 7 THEN source_row.numeric_column_15 WHEN 8 THEN source_row.numeric_column_17 WHEN 9 THEN source_row.numeric_column_19 WHEN 10 THEN source_row.numeric_column_21 WHEN 11 THEN source_row.numeric_column_23 WHEN 12 THEN source_row.numeric_column_25 WHEN 13 THEN source_row.numeric_column_27 WHEN 14 THEN source_row.numeric_column_29 END,CASE position_keys.position_number WHEN 1 THEN source_row.numeric_column_4 WHEN 2 THEN source_row.numeric_column_6 WHEN 3 THEN source_row.numeric_column_8 WHEN 4 THEN source_row.numeric_column_10 WHEN 5 THEN source_row.numeric_column_12 WHEN 6 THEN source_row.numeric_column_14 WHEN 7 THEN source_row.numeric_column_16 WHEN 8 THEN source_row.numeric_column_18 WHEN 9 THEN source_row.numeric_column_20 WHEN 10 THEN source_row.numeric_column_22 WHEN 11 THEN source_row.numeric_column_24 WHEN 12 THEN source_row.numeric_column_26 WHEN 13 THEN source_row.numeric_column_28 WHEN 14 THEN source_row.numeric_column_30 END)]) raw_axis(declared smallint,axis_kind text,weight_text text,gap_text text) CROSS JOIN LATERAL pg_catalog.unnest(pg_catalog.array_append(ARRAY[]::integer[],(NULLIF(raw_axis.weight_text,'')::numeric*1000)::integer)) parsed_weight(scaled) CROSS JOIN LATERAL pg_catalog.unnest(pg_catalog.array_append(ARRAY[]::integer[],(NULLIF(raw_axis.gap_text,'')::numeric*1000)::integer)) parsed_gap(scaled) WHERE raw_axis.declared=position_keys.position_number AND raw_axis.axis_kind IN ('X','Y','Z') AND parsed_weight.scaled>=0) TO 'C:/Users/c9408/Desktop/Python & PostgreSQL Data Pipeline Optimization Challenge/.runtime/native_copy/copy_batch_dg652onj/table2.bin' (FORMAT BINARY);
\else
\copy (SELECT main_row.id_column_1,position_keys.position_number, parsed_weight.scaled,raw_axis.axis_kind, NULL::integer,NULL::integer,NULL::integer, CASE WHEN parsed_gap.scaled>0 THEN parsed_gap.scaled ELSE NULL::integer END FROM pgdm_pipeline_work.pipeline_normalized_input source_row JOIN public.table1 main_row ON main_row.id_column_3=source_row.raw_id CROSS JOIN LATERAL pg_catalog.unnest((ARRAY[1::smallint,2::smallint,3::smallint,4::smallint,5::smallint,6::smallint,7::smallint,8::smallint,9::smallint,10::smallint,11::smallint,12::smallint,13::smallint,14::smallint])[1:LEAST(COALESCE(main_row.integer_column_1,0),14)]) position_keys(position_number) CROSS JOIN LATERAL pg_catalog.unnest(ARRAY[ROW(CASE position_keys.position_number WHEN 1 THEN NULLIF(BTRIM(source_row.integer_column_2),'')::smallint WHEN 2 THEN NULLIF(BTRIM(source_row.integer_column_3),'')::smallint WHEN 3 THEN NULLIF(BTRIM(source_row.integer_column_4),'')::smallint WHEN 4 THEN NULLIF(BTRIM(source_row.integer_column_5),'')::smallint WHEN 5 THEN NULLIF(BTRIM(source_row.integer_column_6),'')::smallint WHEN 6 THEN NULLIF(BTRIM(source_row.integer_column_7),'')::smallint WHEN 7 THEN NULLIF(BTRIM(source_row.integer_column_8),'')::smallint WHEN 8 THEN NULLIF(BTRIM(source_row.integer_column_9),'')::smallint WHEN 9 THEN NULLIF(BTRIM(source_row.integer_column_10),'')::smallint WHEN 10 THEN NULLIF(BTRIM(source_row.integer_column_11),'')::smallint WHEN 11 THEN NULLIF(BTRIM(source_row.integer_column_12),'')::smallint WHEN 12 THEN NULLIF(BTRIM(source_row.integer_column_13),'')::smallint WHEN 13 THEN NULLIF(BTRIM(source_row.integer_column_14),'')::smallint WHEN 14 THEN NULLIF(BTRIM(source_row.integer_column_15),'')::smallint END,CASE position_keys.position_number WHEN 1 THEN source_row.text_column_4 WHEN 2 THEN source_row.text_column_5 WHEN 3 THEN source_row.text_column_6 WHEN 4 THEN source_row.text_column_7 WHEN 5 THEN source_row.text_column_8 WHEN 6 THEN source_row.text_column_9 WHEN 7 THEN source_row.text_column_10 WHEN 8 THEN source_row.text_column_11 WHEN 9 THEN source_row.text_column_12 WHEN 10 THEN source_row.text_column_13 WHEN 11 THEN source_row.text_column_14 WHEN 12 THEN source_row.text_column_15 WHEN 13 THEN source_row.text_column_16 WHEN 14 THEN source_row.text_column_17 END,CASE position_keys.position_number WHEN 1 THEN source_row.numeric_column_3 WHEN 2 THEN source_row.numeric_column_5 WHEN 3 THEN source_row.numeric_column_7 WHEN 4 THEN source_row.numeric_column_9 WHEN 5 THEN source_row.numeric_column_11 WHEN 6 THEN source_row.numeric_column_13 WHEN 7 THEN source_row.numeric_column_15 WHEN 8 THEN source_row.numeric_column_17 WHEN 9 THEN source_row.numeric_column_19 WHEN 10 THEN source_row.numeric_column_21 WHEN 11 THEN source_row.numeric_column_23 WHEN 12 THEN source_row.numeric_column_25 WHEN 13 THEN source_row.numeric_column_27 WHEN 14 THEN source_row.numeric_column_29 END,CASE position_keys.position_number WHEN 1 THEN source_row.numeric_column_4 WHEN 2 THEN source_row.numeric_column_6 WHEN 3 THEN source_row.numeric_column_8 WHEN 4 THEN source_row.numeric_column_10 WHEN 5 THEN source_row.numeric_column_12 WHEN 6 THEN source_row.numeric_column_14 WHEN 7 THEN source_row.numeric_column_16 WHEN 8 THEN source_row.numeric_column_18 WHEN 9 THEN source_row.numeric_column_20 WHEN 10 THEN source_row.numeric_column_22 WHEN 11 THEN source_row.numeric_column_24 WHEN 12 THEN source_row.numeric_column_26 WHEN 13 THEN source_row.numeric_column_28 WHEN 14 THEN source_row.numeric_column_30 END)]) raw_axis(declared smallint,axis_kind text,weight_text text,gap_text text) CROSS JOIN LATERAL pg_catalog.unnest(pg_catalog.array_append(ARRAY[]::integer[],(NULLIF(raw_axis.weight_text,'')::numeric*1000)::integer)) parsed_weight(scaled) CROSS JOIN LATERAL pg_catalog.unnest(pg_catalog.array_append(ARRAY[]::integer[],(NULLIF(raw_axis.gap_text,'')::numeric*1000)::integer)) parsed_gap(scaled) WHERE raw_axis.declared=position_keys.position_number AND raw_axis.axis_kind IN ('X','Y','Z') AND parsed_weight.scaled>=0) TO 'table2.bin' (FORMAT BINARY)
\endif
\if :pipeline_server_file_copy
COPY public.table2 (id_column_2,integer_column_1,integer_column_2,text_column_1,integer_column_3,integer_column_4,integer_column_5,integer_column_6) FROM 'C:/Users/c9408/Desktop/Python & PostgreSQL Data Pipeline Optimization Challenge/.runtime/native_copy/copy_batch_dg652onj/table2.bin' (FORMAT BINARY);
\else
\copy public.table2 (id_column_2,integer_column_1,integer_column_2,text_column_1,integer_column_3,integer_column_4,integer_column_5,integer_column_6) FROM 'table2.bin' (FORMAT BINARY)
\endif
INSERT INTO pg_temp.pipeline_copy_receipts VALUES('table2',:'ROW_COUNT'::bigint);
DO $restore_parallel$ DECLARE r record; BEGIN FOR r IN SELECT * FROM pg_temp.pipeline_parallel_settings LOOP PERFORM pg_catalog.set_config(r.name,r.setting,true); END LOOP; END $restore_parallel$;
\endif

      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=202;
DO $pgdm_managed_sql$
DECLARE
    pgdm_affected_rows bigint;
    pipeline_copy_fast boolean := (SELECT safe FROM pg_temp.pipeline_normalization_domain);
    pgdm_timing_started_at timestamptz := clock_timestamp();
BEGIN
    IF pipeline_copy_fast THEN
      SELECT affected_rows INTO STRICT pgdm_affected_rows FROM pg_temp.pipeline_copy_receipts WHERE table_name='table2';
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
      GET DIAGNOSTICS pgdm_affected_rows = ROW_COUNT;
    END IF
    ;
    

    UPDATE "public"."pgdm_table_row_counts"
    SET row_count = row_count + pgdm_affected_rows,
        updated_at = clock_timestamp()
    WHERE schema_name = 'public'
      AND table_name = 'table2';

    IF NOT FOUND THEN
        RAISE EXCEPTION
            'Contador ausente para %.% durante insercao.',
            'public',
            'table2';
    END IF;


    INSERT INTO pg_temp.pgdm_expansion_timings (
        sequence_number,
        stage_key,
        item_name,
        started_at,
        finished_at,
        rows_affected,
        destination_name,
        statement_index
    ) VALUES (
        15,
        'execute',
        'Insert into public.table2 (statement 1)',
        pgdm_timing_started_at,
        clock_timestamp(),
        pgdm_affected_rows,
        'public.table2',
        1
    );
END
$pgdm_managed_sql$;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (121,'bulk_load','Rebuild and validate table2 keys',clock_timestamp(),NULL,NULL,NULL,NULL);
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
DO $sequence_cache_restore$ DECLARE item record; previous_path text:=pg_catalog.current_setting('search_path'); BEGIN
      PERFORM pg_catalog.set_config('search_path','pg_catalog',true);
      IF to_regclass('pg_temp.pipeline_bulk_sequence_settings') IS NOT NULL THEN
        FOR item IN SELECT * FROM pg_temp.pipeline_bulk_sequence_settings WHERE table_name='table2' LOOP
          EXECUTE format('ALTER SEQUENCE %s CACHE %s',item.sequence_oid::regclass,item.cache_size);
          DELETE FROM pg_temp.pipeline_bulk_sequence_settings WHERE table_name=item.table_name;
        END LOOP;
      END IF;
      PERFORM pg_catalog.set_config('search_path',previous_path,true);
    END $sequence_cache_restore$;
END IF; END $guarded_bulk$;
      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=121;

INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (8000,'bulk_load','Restore and validate all bulk foreign keys',clock_timestamp(),NULL,NULL,NULL,NULL);
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
      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=8000;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES
      (8500,'bulk_load','Remove transaction-owned shared workspace',clock_timestamp(),NULL,NULL,NULL,NULL);
      DO $work_cleanup$ BEGIN
      IF (SELECT owned FROM pg_temp.pipeline_work_ownership) THEN
        DROP TABLE pgdm_pipeline_work.pipeline_normalized_input RESTRICT;
        DROP SCHEMA pgdm_pipeline_work RESTRICT;
      END IF;
    END $work_cleanup$;
      UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=8500;
INSERT INTO pg_temp.pgdm_expansion_timings VALUES (9000, 'execute', 'Commit expansion transaction', clock_timestamp(), NULL, NULL, NULL, NULL);
COMMIT;
UPDATE pg_temp.pgdm_expansion_timings SET finished_at = clock_timestamp() WHERE sequence_number = 9000;

SELECT json_build_object(
    'pgdm_expansion_timings', COALESCE(
        json_agg(
            json_build_object(
                'sequence', sequence_number,
                'stage', stage_key,
                'name', item_name,
                'duration_seconds', EXTRACT(
                    EPOCH FROM (finished_at - started_at)
                ),
                'rows', rows_affected,
                'destination', destination_name,
                'statement_index', statement_index
            ) ORDER BY sequence_number
        ),
        '[]'::json
    )
)::text
FROM pg_temp.pgdm_expansion_timings;
DROP TABLE pg_temp.pgdm_expansion_timings;
