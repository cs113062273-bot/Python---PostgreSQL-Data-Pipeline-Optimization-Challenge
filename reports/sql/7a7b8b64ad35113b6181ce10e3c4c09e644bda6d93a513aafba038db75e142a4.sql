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
    SELECT source_row.raw -> 'values' AS payload OFFSET 0
) AS qf_payload
CROSS JOIN LATERAL (
    VALUES
        (1::smallint, qf_payload.payload ->> 'integer_column_2', qf_payload.payload ->> 'text_column_4', qf_payload.payload ->> 'numeric_column_3', qf_payload.payload ->> 'numeric_column_4'),
        (2::smallint, qf_payload.payload ->> 'integer_column_3', qf_payload.payload ->> 'text_column_5', qf_payload.payload ->> 'numeric_column_5', qf_payload.payload ->> 'numeric_column_6'),
        (3::smallint, qf_payload.payload ->> 'integer_column_4', qf_payload.payload ->> 'text_column_6', qf_payload.payload ->> 'numeric_column_7', qf_payload.payload ->> 'numeric_column_8'),
        (4::smallint, qf_payload.payload ->> 'integer_column_5', qf_payload.payload ->> 'text_column_7', qf_payload.payload ->> 'numeric_column_9', qf_payload.payload ->> 'numeric_column_10'),
        (5::smallint, qf_payload.payload ->> 'integer_column_6', qf_payload.payload ->> 'text_column_8', qf_payload.payload ->> 'numeric_column_11', qf_payload.payload ->> 'numeric_column_12'),
        (6::smallint, qf_payload.payload ->> 'integer_column_7', qf_payload.payload ->> 'text_column_9', qf_payload.payload ->> 'numeric_column_13', qf_payload.payload ->> 'numeric_column_14'),
        (7::smallint, qf_payload.payload ->> 'integer_column_8', qf_payload.payload ->> 'text_column_10', qf_payload.payload ->> 'numeric_column_15', qf_payload.payload ->> 'numeric_column_16'),
        (8::smallint, qf_payload.payload ->> 'integer_column_9', qf_payload.payload ->> 'text_column_11', qf_payload.payload ->> 'numeric_column_17', qf_payload.payload ->> 'numeric_column_18'),
        (9::smallint, qf_payload.payload ->> 'integer_column_10', qf_payload.payload ->> 'text_column_12', qf_payload.payload ->> 'numeric_column_19', qf_payload.payload ->> 'numeric_column_20'),
        (10::smallint, qf_payload.payload ->> 'integer_column_11', qf_payload.payload ->> 'text_column_13', qf_payload.payload ->> 'numeric_column_21', qf_payload.payload ->> 'numeric_column_22'),
        (11::smallint, qf_payload.payload ->> 'integer_column_12', qf_payload.payload ->> 'text_column_14', qf_payload.payload ->> 'numeric_column_23', qf_payload.payload ->> 'numeric_column_24'),
        (12::smallint, qf_payload.payload ->> 'integer_column_13', qf_payload.payload ->> 'text_column_15', qf_payload.payload ->> 'numeric_column_25', qf_payload.payload ->> 'numeric_column_26'),
        (13::smallint, qf_payload.payload ->> 'integer_column_14', qf_payload.payload ->> 'text_column_16', qf_payload.payload ->> 'numeric_column_27', qf_payload.payload ->> 'numeric_column_28'),
        (14::smallint, qf_payload.payload ->> 'integer_column_15', qf_payload.payload ->> 'text_column_17', qf_payload.payload ->> 'numeric_column_29', qf_payload.payload ->> 'numeric_column_30')
) AS position_data(position_number, declared_number_text, type_text, weight_text, gap_text)
JOIN public.table1 AS main_row
  ON main_row.id_column_3 = source_row.raw_id
WHERE NULLIF(BTRIM(position_data.declared_number_text), '')::smallint
        = position_data.position_number
  AND NULLIF(BTRIM(position_data.weight_text), '')::numeric >= 0
  AND UPPER(BTRIM(position_data.type_text)) IN ('X', 'Y', 'Z');