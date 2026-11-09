import unittest
from pathlib import Path
from unittest.mock import patch

from services.expansion_engine import (
    defer_foreign_keys_sql,
    defer_indexes_sql,
    plan_staged_insert,
    restore_foreign_keys_sql,
    restore_indexes_sql,
    rewrite_json_paths,
    staged_insert_sql,
)

SQL_DIR = Path(__file__).resolve().parents[1] / "SQL files"
SOURCE_SQL = (
    "(\n    SELECT *\n    FROM \"public\".\"raw_data\"\n"
    "    WHERE \"raw_schema\" IN ('group001')\n)"
)


def staged(body):
    return plan_staged_insert(body.replace("{{source}}", SOURCE_SQL), SOURCE_SQL, {"count"})


def rewrite(sql):
    return rewrite_json_paths(sql, {"raw"})[0]


class JsonPathRewriteTests(unittest.TestCase):
    def test_collapses_two_step_chain(self):
        self.assertEqual(
            rewrite("SELECT s.raw -> 'values' ->> 'k' FROM t s"),
            "SELECT (s.raw #>> ARRAY['values', 'k']) FROM t s",
        )

    def test_collapses_longer_chain_and_keeps_following_comparison(self):
        self.assertEqual(
            rewrite("SELECT s.raw -> 'a' -> 'b' ->> 'c' = 'x' FROM t s"),
            "SELECT (s.raw #>> ARRAY['a', 'b', 'c']) = 'x' FROM t s",
        )

    def test_json_result_uses_path_operator(self):
        self.assertEqual(
            rewrite("SELECT s.raw -> 'a' -> 'b' FROM t s"),
            "SELECT (s.raw #> ARRAY['a', 'b']) FROM t s",
        )

    def test_quoted_key_is_preserved(self):
        self.assertEqual(
            rewrite("SELECT s.raw -> 'v' ->> 'it''s' FROM t s"),
            "SELECT (s.raw #>> ARRAY['v', 'it''s']) FROM t s",
        )

    def test_single_step_is_unchanged(self):
        sql = "SELECT s.raw -> 'values' FROM t s"
        self.assertEqual(rewrite(sql), sql)

    def test_integer_like_key_is_unchanged(self):
        # On arrays a path element is an index, while ->> 'text' is not.
        sql = "SELECT s.raw -> 'values' ->> '0' FROM t s"
        self.assertEqual(rewrite(sql), sql)

    def test_tighter_binding_neighbours_are_unchanged(self):
        for sql in (
            "SELECT s.raw -> 'v' ->> 'k'::int FROM t s",
            "SELECT 1 + s.raw -> 'v' ->> 'k' FROM t s",
            "SELECT x || s.raw -> 'v' ->> 'k' FROM t s",
            "SELECT s.raw -> 'v' ->> 'k' AT TIME ZONE 'UTC' FROM t s",
            "SELECT s.raw -> 'v' ->> 'k' COLLATE \"C\" FROM t s",
            "SELECT s.raw -> 'v' ->> 'k' - 1 FROM t s",
        ):
            self.assertEqual(rewrite(sql), sql, sql)

    def test_non_json_column_and_escape_strings_are_unchanged(self):
        for sql in (
            "SELECT s.other -> 'v' ->> 'k' FROM t s",
            "SELECT s.raw -> E'v' ->> 'k' FROM t s",
        ):
            self.assertEqual(rewrite(sql), sql, sql)

    def test_comments_and_literals_are_not_rewritten(self):
        sql = "SELECT '-> ''v'' ->> ''k''' /* s.raw -> 'a' ->> 'b' */ FROM t s"
        self.assertEqual(rewrite(sql), sql)

    def test_official_recipes_have_no_remaining_chains(self):
        for path in sorted(SQL_DIR.glob("*.sql")):
            original = path.read_text(encoding="utf-8")
            rewritten, count = rewrite_json_paths(original, {"raw"})
            self.assertGreater(count, 0, path.name)
            self.assertNotIn("->", rewritten, path.name)
            self.assertEqual(rewritten.count("#>>"), original.count("->>"), path.name)


class ForeignKeyDeferralTests(unittest.TestCase):
    def test_only_recreatable_validated_constraints_are_deferred(self):
        sql = defer_foreign_keys_sql(["'\"public\".\"table1\"'"])
        self.assertIn("constraint_data.convalidated", sql)
        self.assertIn("constraint_data.conparentid = 0", sql)
        self.assertIn("pg_has_role(relation.relowner, 'USAGE')", sql)
        self.assertIn("has_table_privilege(constraint_data.confrelid, 'REFERENCES')", sql)
        self.assertIn("'\"public\".\"table1\"'::regclass", sql)

    def test_restore_recreates_definition_and_comment(self):
        sql = restore_foreign_keys_sql()
        self.assertIn("ADD CONSTRAINT %I %s", sql)
        self.assertIn("COMMENT ON CONSTRAINT", sql)


class ParallelStageTests(unittest.TestCase):
    def test_official_recipes_are_staged(self):
        for path in sorted(SQL_DIR.glob("*.sql")):
            recipe = rewrite_json_paths(path.read_text(encoding="utf-8"), {"raw"})[0]
            plan = staged(recipe)
            self.assertIsNotNone(plan, path.name)
            self.assertEqual(plan["distinct"], "DISTINCT" in recipe, path.name)

    def test_row_recipe_is_ordered_by_source_position_then_output(self):
        plan = staged("INSERT INTO public.t (a, b) SELECT s.raw ->> 'x', ARRAY[1, 2] FROM {{source}} AS s")
        self.assertEqual(plan["stage_columns"], ["pgdm_source_ctid", "pgdm_c1", "pgdm_c2"])
        self.assertIn("s.pgdm_source_ctid, s.raw", plan["select_sql"])
        self.assertIn("SELECT *, ctid AS pgdm_source_ctid", plan["select_sql"])
        build, insert = staged_insert_sql(plan, "pgdm_stage_10")
        self.assertIn("WITH NO DATA", build)
        self.assertIn("\\gset", build)
        self.assertTrue(build.endswith(":pgdm_stage_10_order;"))
        self.assertTrue(insert.endswith("SELECT pgdm_c1, pgdm_c2 FROM pg_temp.pgdm_stage_10"))

    def test_distinct_recipe_does_not_add_the_source_position(self):
        plan = staged("INSERT INTO public.t (a) SELECT DISTINCT s.raw ->> 'x' FROM {{source}} s")
        self.assertEqual(plan["stage_columns"], ["pgdm_c1"])
        self.assertNotIn("ctid", plan["select_sql"])

    def test_statements_without_a_source_order_keep_single_statement_execution(self):
        for body in (
            "INSERT INTO public.t (a) SELECT s.raw ->> 'x' FROM {{source}} s GROUP BY 1",
            "INSERT INTO public.t (a) SELECT s.raw ->> 'x' FROM {{source}} s ORDER BY 1",
            "INSERT INTO public.t (a) SELECT s.raw ->> 'x' FROM {{source}} s LIMIT 5",
            "INSERT INTO public.t (a) SELECT s.raw ->> 'x' FROM {{source}} s UNION SELECT 'y'",
            "INSERT INTO public.t (a) SELECT DISTINCT ON (1) s.raw ->> 'x' FROM {{source}} s",
            "INSERT INTO public.t (a) SELECT row_number() OVER () FROM {{source}} s",
            "INSERT INTO public.t (a) SELECT count(*) FROM {{source}} s",
            "INSERT INTO public.t (a) SELECT to_jsonb(s) FROM {{source}} s",
            "INSERT INTO public.t (a, b) SELECT s.* FROM {{source}} s",
            "INSERT INTO public.t (a) SELECT * FROM {{source}} s",
            "INSERT INTO public.t SELECT s.raw FROM {{source}} s",
            "INSERT INTO public.t (a, b) SELECT s.raw FROM {{source}} s",
            "INSERT INTO public.t (a) SELECT x.raw FROM {{source}} x JOIN {{source}} y USING (raw_id)",
            "INSERT INTO public.t (a) SELECT raw FROM {{source}}",
            "INSERT INTO public.t (a) SELECT s.c FROM {{source}} s (c)",
        ):
            self.assertIsNone(staged(body), body)

    def test_index_deferral_keeps_indexes_it_cannot_rebuild_identically(self):
        sql = defer_indexes_sql(["'\"public\".\"table1\"'"])
        for condition in (
            "method.amname = 'btree'",
            "pg_has_role(relation.relowner, 'USAGE')",
            "NOT index_data.indisclustered",
            "NOT index_data.indisreplident",
            "index_class.reltablespace = 0",
            "pg_inherits.inhrelid = index_data.indexrelid",
            "other.conindid = index_data.indexrelid",
        ):
            self.assertIn(condition, sql)
        restore = restore_indexes_sql()
        self.assertIn("ADD CONSTRAINT %I %s", restore)
        self.assertIn("COMMENT ON INDEX", restore)


class PreflightBatchTests(unittest.TestCase):
    def setUp(self):
        from services.postgres_service import PostgresAdminService

        self.service = PostgresAdminService()
        self.service.ssh_client = object()
        self.service.sql_username = "challenge"
        self.service.sql_password = "x"

    def test_batched_output_is_split_per_statement(self):
        marker = self.service.PREFETCH_SPLIT_MARKER
        with patch.object(
            self.service,
            "run_remote_command",
            return_value=f"t\r\n{marker}\r\n\r\n{marker}\r\na,b\r\n1,2\r\n{marker}",
        ):
            outputs = self.service._run_batched_psql("psql", ["SELECT 1;", "SELECT 2;", "SELECT 3;"])
        self.assertEqual(outputs, ["t", "", "a,b\r\n1,2"])

    def test_wrong_part_count_is_rejected(self):
        with patch.object(self.service, "run_remote_command", return_value="t"):
            with self.assertRaises(RuntimeError):
                self.service._run_batched_psql("psql", ["SELECT 1;"])

    def test_prefetched_output_is_served_once(self):
        command = self.service._table_exists_command("db", "public.table1")
        self.service._expansion_timing_local.prefetched = {(command, None): "t"}
        self.assertEqual(self.service.run_remote_command(command), "t")
        self.assertEqual(self.service._expansion_timing_local.prefetched, {})


if __name__ == "__main__":
    unittest.main()
