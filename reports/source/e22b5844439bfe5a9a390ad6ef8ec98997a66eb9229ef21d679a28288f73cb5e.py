"""Sufficient error-free domain for the original QueryFoundry table2 fast path.

This gate never rejects input. Outside the proven bounded-text domain, execute
the exact organizer statement. Every other destination remains unchanged.
"""
from functools import lru_cache
from pipeline.recipes import destinations
from pipeline.upstream import PostgresAdminService

# Four decimal digits always fit smallint. Six integer digits and up to three
# fractional digits remain inside int32 after multiplication by 1000/rounding.
# Empty and JSON-null values cast to SQL NULL. Other representations fall back.
UNSAFE = '''$.keyvalue() ? (
 (@.key like_regex "^integer_column_([2-9]|1[0-5])$" &&
  !(@.value == null || @.value == "" ||
    (@.value.type() == "string" && @.value like_regex "^[-+]?[0-9]{1,4}$"))) ||
 (@.key like_regex "^numeric_column_([3-9]|[12][0-9]|30)$" &&
  !(@.value == null || @.value == "" ||
    (@.value.type() == "string" && @.value like_regex "^[-+]?[0-9]{1,6}([.][0-9]{1,3})?$")))
)'''


def gate_sql(scopes):
    from utils import sql_literal
    scope_sql=",".join(sql_literal(s) for s in scopes)
    return """SELECT coalesce(bool_and(coalesce(
        jsonb_typeof(payload)='object' AND
        NOT jsonb_path_exists(payload,"""+sql_literal(UNSAFE)+"""::jsonpath,'{}'::jsonb,true),false)),false)
        FROM public.raw_data source_row
        CROSS JOIN LATERAL (SELECT source_row.raw->'values' AS payload OFFSET 0) p
        WHERE raw_schema IN ("""+scope_sql+")"


@lru_cache(maxsize=64)
def fast_statement(scopes):
    return PostgresAdminService._prepare_cross_table_expansion("public.raw_data",list(scopes),destinations("qf_lazy"))["destinations"][-1]["statements"][0]


def choose(conn,baseline,scopes):
    safe=conn.execute(gate_sql(scopes)).fetchone()[0]
    return (fast_statement(tuple(scopes)) if safe else baseline),safe
