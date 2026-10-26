INSERT INTO public.table3 (
    text_column_1,
    text_column_2,
    numeric_column_1,
    text_column_3,
    text_column_4
)
SELECT DISTINCT
    BTRIM(qf_payload.payload ->> 'text_column_1'),
    'SYNTHETIC/REFERENCE',
    NULL::numeric,
    LEFT(UPPER(BTRIM(qf_payload.payload ->> 'text_column_2')), 1),
    RIGHT(UPPER(BTRIM(qf_payload.payload ->> 'text_column_2')), 1)
FROM (
    SELECT *
    FROM "public"."raw_data"
    WHERE "raw_schema" IN ('group001')
) AS source_row
CROSS JOIN LATERAL (
    SELECT source_row.raw -> 'values' AS payload OFFSET 0
) AS qf_payload
WHERE BTRIM(qf_payload.payload ->> 'text_column_1') <> ''
  AND UPPER(BTRIM(qf_payload.payload ->> 'text_column_2')) ~ '^[A-Z]{2}$';

INSERT INTO public.table4 (
    id_column_1,
    text_column_1,
    text_column_2
)
SELECT DISTINCT
    (
        (ASCII(LEFT(UPPER(BTRIM(qf_payload.payload ->> 'text_column_3')), 1))
            - ASCII('A')) * 26
        + ASCII(RIGHT(UPPER(BTRIM(qf_payload.payload ->> 'text_column_3')), 1))
        - ASCII('A')
        + 1
    )::smallint,
    'SYNTHETIC/GENERIC',
    UPPER(BTRIM(qf_payload.payload ->> 'text_column_3'))
FROM (
    SELECT *
    FROM "public"."raw_data"
    WHERE "raw_schema" IN ('group001')
) AS source_row
CROSS JOIN LATERAL (
    SELECT source_row.raw -> 'values' AS payload OFFSET 0
) AS qf_payload
WHERE UPPER(BTRIM(qf_payload.payload ->> 'text_column_3')) ~ '^[A-Z]{2}$';

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
    NULLIF(BTRIM(source_row.raw #>> '{values,id_column_1}'), '')::integer,
    NULL::character varying,
    NULLIF(BTRIM(source_row.raw #>> '{values,datetime_column_1}'), '')::timestamptz
        AT TIME ZONE 'America/Sao_Paulo',
    NULL::character varying,
    GREATEST(
        NULLIF(BTRIM(source_row.raw #>> '{values,integer_column_2}'), '')::smallint,
        NULLIF(BTRIM(source_row.raw #>> '{values,integer_column_3}'), '')::smallint,
        NULLIF(BTRIM(source_row.raw #>> '{values,integer_column_4}'), '')::smallint,
        NULLIF(BTRIM(source_row.raw #>> '{values,integer_column_5}'), '')::smallint,
        NULLIF(BTRIM(source_row.raw #>> '{values,integer_column_6}'), '')::smallint,
        NULLIF(BTRIM(source_row.raw #>> '{values,integer_column_7}'), '')::smallint,
        NULLIF(BTRIM(source_row.raw #>> '{values,integer_column_8}'), '')::smallint,
        NULLIF(BTRIM(source_row.raw #>> '{values,integer_column_9}'), '')::smallint,
        NULLIF(BTRIM(source_row.raw #>> '{values,integer_column_10}'), '')::smallint,
        NULLIF(BTRIM(source_row.raw #>> '{values,integer_column_11}'), '')::smallint,
        NULLIF(BTRIM(source_row.raw #>> '{values,integer_column_12}'), '')::smallint,
        NULLIF(BTRIM(source_row.raw #>> '{values,integer_column_13}'), '')::smallint,
        NULLIF(BTRIM(source_row.raw #>> '{values,integer_column_14}'), '')::smallint,
        NULLIF(BTRIM(source_row.raw #>> '{values,integer_column_15}'), '')::smallint
    ),
    NULL::smallint,
    CASE
        WHEN NULLIF(BTRIM(source_row.raw #>> '{values,numeric_column_2}'), '')::numeric > 0
        THEN ROUND(
            NULLIF(BTRIM(source_row.raw #>> '{values,numeric_column_2}'), '')::numeric * 1000
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
        WHEN NULLIF(BTRIM(source_row.raw #>> '{values,numeric_column_1}'), '')::numeric > 0
        THEN ROUND(
            NULLIF(BTRIM(source_row.raw #>> '{values,numeric_column_1}'), '')::numeric * 1000
        )::integer
        ELSE NULL::integer
    END
FROM (
    SELECT *
    FROM "public"."raw_data"
    WHERE "raw_schema" IN ('group001')
) AS source_row
JOIN public.table3 AS dimension_row
  ON dimension_row.text_column_1 = BTRIM(source_row.raw #>> '{values,text_column_1}')
 AND dimension_row.text_column_2 = 'SYNTHETIC/REFERENCE'
 AND dimension_row.numeric_column_1 IS NULL
 AND dimension_row.text_column_3 = LEFT(
        UPPER(BTRIM(source_row.raw #>> '{values,text_column_2}')),
        1
    )
 AND dimension_row.text_column_4 = RIGHT(
        UPPER(BTRIM(source_row.raw #>> '{values,text_column_2}')),
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
        NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_1'), '')::numeric,
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
CROSS JOIN LATERAL (
    SELECT source_row.raw -> 'values' AS payload OFFSET 0
) AS qf_payload
JOIN public.table1 AS main_row
  ON main_row.id_column_3 = source_row.raw_id
WHERE NULLIF(BTRIM(qf_payload.payload ->> 'integer_column_1'), '')::numeric > 0;

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
CROSS JOIN LATERAL (
    SELECT source_row.raw -> 'values' AS payload OFFSET 0
) AS qf_payload
JOIN public.table1 AS main_row
  ON main_row.id_column_3 = source_row.raw_id
JOIN public.table4 AS reference_row
  ON reference_row.text_column_1 = 'SYNTHETIC/GENERIC'
 AND reference_row.text_column_2 = UPPER(
        BTRIM(qf_payload.payload ->> 'text_column_3')
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
    position_keys.position_number,
    ROUND(
        NULLIF(BTRIM((qf_payload.payload ->> position_keys.weight_key)), '')::numeric * 1000
    )::integer,
    UPPER(BTRIM((qf_payload.payload ->> position_keys.type_key))),
    NULL::integer,
    NULL::integer,
    NULL::integer,
    CASE
        WHEN NULLIF(BTRIM((qf_payload.payload ->> position_keys.gap_key)), '')::numeric > 0
        THEN ROUND(
            NULLIF(BTRIM((qf_payload.payload ->> position_keys.gap_key)), '')::numeric * 1000
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
CROSS JOIN LATERAL (
    VALUES
        (1::smallint, 'integer_column_2', 'text_column_4', 'numeric_column_3', 'numeric_column_4'),
        (2::smallint, 'integer_column_3', 'text_column_5', 'numeric_column_5', 'numeric_column_6'),
        (3::smallint, 'integer_column_4', 'text_column_6', 'numeric_column_7', 'numeric_column_8'),
        (4::smallint, 'integer_column_5', 'text_column_7', 'numeric_column_9', 'numeric_column_10'),
        (5::smallint, 'integer_column_6', 'text_column_8', 'numeric_column_11', 'numeric_column_12'),
        (6::smallint, 'integer_column_7', 'text_column_9', 'numeric_column_13', 'numeric_column_14'),
        (7::smallint, 'integer_column_8', 'text_column_10', 'numeric_column_15', 'numeric_column_16'),
        (8::smallint, 'integer_column_9', 'text_column_11', 'numeric_column_17', 'numeric_column_18'),
        (9::smallint, 'integer_column_10', 'text_column_12', 'numeric_column_19', 'numeric_column_20'),
        (10::smallint, 'integer_column_11', 'text_column_13', 'numeric_column_21', 'numeric_column_22'),
        (11::smallint, 'integer_column_12', 'text_column_14', 'numeric_column_23', 'numeric_column_24'),
        (12::smallint, 'integer_column_13', 'text_column_15', 'numeric_column_25', 'numeric_column_26'),
        (13::smallint, 'integer_column_14', 'text_column_16', 'numeric_column_27', 'numeric_column_28'),
        (14::smallint, 'integer_column_15', 'text_column_17', 'numeric_column_29', 'numeric_column_30')
) AS position_keys(position_number, declared_key, type_key, weight_key, gap_key)
JOIN public.table1 AS main_row
  ON main_row.id_column_3 = source_row.raw_id
WHERE NULLIF(BTRIM((qf_payload.payload ->> position_keys.declared_key)), '')::smallint
        = position_keys.position_number
  AND NULLIF(BTRIM((qf_payload.payload ->> position_keys.weight_key)), '')::numeric >= 0
  AND UPPER(BTRIM((qf_payload.payload ->> position_keys.type_key))) IN ('X', 'Y', 'Z');