-- Row-exact fingerprint of an expanded kaggle_challenge database.
-- Run after the original and after this version on identical generated data;
-- the outputs must be equal (identity values included).
--   psql -d kaggle_challenge -XAt -f tools/output_fingerprint.sql
SELECT 'table1', count(*), md5(string_agg(t::text, '|' ORDER BY id_column_1)) FROM public.table1 t
UNION ALL SELECT 'table2', count(*), md5(string_agg(t::text, '|' ORDER BY id_column_1)) FROM public.table2 t
UNION ALL SELECT 'table3', count(*), md5(string_agg(t::text, '|' ORDER BY id_column_1)) FROM public.table3 t
UNION ALL SELECT 'table4', count(*), md5(string_agg(t::text, '|' ORDER BY id_column_1)) FROM public.table4 t
UNION ALL SELECT 'table5', count(*), md5(string_agg(t::text, '|' ORDER BY id_column_1)) FROM public.table5 t
UNION ALL SELECT 'table6', count(*), md5(string_agg(t::text, '|' ORDER BY id_column_1)) FROM public.table6 t
UNION ALL SELECT 'row_counters', count(*), md5(string_agg(schema_name || '.' || table_name || '=' || row_count, '|' ORDER BY table_name))
    FROM public.pgdm_table_row_counts
UNION ALL SELECT 'constraints', count(*), md5(string_agg(conrelid::regclass || ':' || conname || ':' || pg_get_constraintdef(oid) || ':' || convalidated, '|' ORDER BY conrelid::regclass::text, conname))
    FROM pg_constraint WHERE connamespace = 'public'::regnamespace AND conrelid::regclass::text ~ '^table[1-6]$'
UNION ALL SELECT 'indexes', count(*), md5(string_agg(pg_get_indexdef(indexrelid), '|' ORDER BY indexrelid::regclass::text))
    FROM pg_index WHERE indrelid::regclass::text ~ '^table[1-6]$'
UNION ALL SELECT 'sequences', count(*), md5(string_agg(sequencename || '=' || coalesce(last_value, 0), '|' ORDER BY sequencename))
    FROM pg_sequences WHERE schemaname = 'public' AND sequencename ~ '^table[1-6]_';
