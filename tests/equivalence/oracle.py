from pipeline.recipes import ORDER


def canonical(table):
    if table == "table3":
        return "SELECT to_jsonb(t)-'id_column_1' AS value FROM public.table3 t"
    if table == "table4":
        return "SELECT to_jsonb(t) AS value FROM public.table4 t"
    if table == "table1":
        return """SELECT (to_jsonb(t)-'id_column_1'-'id_column_4') ||
            jsonb_build_object('dimension',to_jsonb(d)-'id_column_1') AS value
            FROM public.table1 t LEFT JOIN public.table3 d ON d.id_column_1=t.id_column_4"""
    return f"""SELECT (to_jsonb(t)-'id_column_1'-'id_column_2') ||
        jsonb_build_object('source_raw_id',m.id_column_3) AS value
        FROM public.{table} t LEFT JOIN public.table1 m ON m.id_column_1=t.id_column_2"""


def fingerprints(conn):
    # Two independent typed 64-bit sums, plus count. Diagnostic, not a proof.
    return {table: conn.execute(f"""SELECT count(*),
        coalesce(sum(hash_record_extended(ROW(value),17)::numeric),0)::text,
        coalesce(sum(hash_record_extended(ROW(value),97)::numeric),0)::text
        FROM ({canonical(table)}) c""").fetchone() for table in ORDER}


def snapshot(conn):
    conn.execute("CREATE SCHEMA IF NOT EXISTS challenge_reference")
    for table in ORDER:
        conn.execute(f"DROP TABLE IF EXISTS challenge_reference.{table}")
        conn.execute(f"CREATE TABLE challenge_reference.{table} AS {canonical(table)}")


def assert_exact(conn):
    with conn.transaction():
        # Oracle-only setting; transaction-scoped so benchmark settings retain
        # their recorded values. This comparison is outside every timed stage.
        conn.execute("SET LOCAL work_mem='256MB'")
        for table in ORDER:
            different = conn.execute(f"""SELECT EXISTS(
                ({canonical(table)} EXCEPT ALL SELECT value FROM challenge_reference.{table})
                UNION ALL (SELECT value FROM challenge_reference.{table} EXCEPT ALL {canonical(table)})
            )""").fetchone()[0]
            if different:
                raise AssertionError(f"Bidirectional EXCEPT ALL mismatch: {table}")


def schema_signature(conn):
    return conn.execute("""SELECT jsonb_build_object(
      'columns', (SELECT jsonb_agg(row_to_json(c) ORDER BY table_name,ordinal_position)
        FROM information_schema.columns c WHERE table_schema='public'),
      'constraints',(SELECT jsonb_agg(jsonb_build_array(c.relname,k.conname,pg_get_constraintdef(k.oid),k.convalidated)
        ORDER BY c.relname,k.conname) FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
        WHERE c.relnamespace='public'::regnamespace),
      'indexes',(SELECT jsonb_agg(jsonb_build_array(tablename,indexname,indexdef) ORDER BY indexname)
        FROM pg_indexes WHERE schemaname='public'))""").fetchone()[0]


def assert_relationships(conn):
    for child in ("table2", "table5", "table6"):
        if conn.execute(f"SELECT EXISTS(SELECT 1 FROM public.{child} c LEFT JOIN public.table1 p ON p.id_column_1=c.id_column_2 WHERE p.id_column_1 IS NULL)").fetchone()[0]:
            raise AssertionError(f"Orphan in {child}")
    if conn.execute("SELECT EXISTS(SELECT 1 FROM pg_constraint WHERE connamespace='public'::regnamespace AND NOT convalidated)").fetchone()[0]:
        raise AssertionError("Unvalidated constraint")
