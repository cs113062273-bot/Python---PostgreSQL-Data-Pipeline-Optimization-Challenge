"""Small official-generator data for MUTATED functional tests, not benchmarks."""
import importlib.util
import sys
from psycopg import sql
from bench.common import ROOT, connect


def generator():
    spec = importlib.util.spec_from_file_location("official_generator", ROOT / "app/generate_database.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def create_fixture(database, rows=64):
    if not database.startswith("lab_"):
        raise ValueError("Fixture requires lab_ database")
    with connect("postgres", autocommit=True) as admin:
        if admin.execute("SELECT 1 FROM pg_database WHERE datname=%s", (database,)).fetchone():
            raise RuntimeError(f"Fixture {database} already exists; use a new disposable database name")
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    gen = generator()
    with connect(database) as conn:
        conn.execute(gen.SCHEMA_SQL)
        conn.execute("CREATE SCHEMA challenge_lab; CREATE TABLE challenge_lab.disposable(created_at timestamptz DEFAULT now())")
        conn.execute("INSERT INTO challenge_lab.disposable DEFAULT VALUES")
        with conn.cursor().copy("COPY public.raw_data(raw_id,raw_hash,raw_ingested_at,raw_schema,raw_tab,raw) FROM STDIN") as copy:
            for row in gen.iter_generated_rows(rows):
                copy.write_row((row.raw_id,row.raw_hash,row.raw_ingested_at,row.raw_schema,row.raw_tab, row.raw_json))
        conn.execute("ANALYZE public.raw_data")
