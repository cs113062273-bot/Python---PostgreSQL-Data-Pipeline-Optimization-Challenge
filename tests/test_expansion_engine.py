import unittest
from pathlib import Path

from services.expansion_engine import (
    defer_foreign_keys_sql,
    restore_foreign_keys_sql,
    rewrite_json_paths,
)

SQL_DIR = Path(__file__).resolve().parents[1] / "SQL files"


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


if __name__ == "__main__":
    unittest.main()
