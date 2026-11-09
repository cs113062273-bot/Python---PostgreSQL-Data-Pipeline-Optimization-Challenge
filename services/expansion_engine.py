"""SQL generation for the optimized Expand path.

Everything here produces SQL that runs inside the single expansion
transaction built by PostgresAdminService.execute_cross_table_expansion.
"""
import re

OPERATOR_CHARS = set("+-*/<>=~!@#%^&|`?")
COMPARISON_OPERATORS = {"=", "<", ">", "<=", ">=", "<>", "!="}
JSON_STEP_OPERATORS = {"->", "->>"}
INTEGER_LIKE = re.compile(r"^\s*[+-]?\d+\s*$")

# Keywords that bind tighter than the JSON operators when they appear next to
# an operand (AT TIME ZONE, COLLATE) or that introduce operator syntax.
TIGHT_KEYWORDS = {"at", "collate", "operator"}


def sql_string(value):
    return "'" + str(value).replace("'", "''") + "'"


def lex(sql):
    """Return (kind, text, start, end) tokens; comments are skipped."""
    tokens = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c.isspace():
            i += 1
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            i = n if j < 0 else j + 1
        elif sql.startswith("/*", i):
            depth, i = 1, i + 2
            while i < n and depth:
                if sql.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                elif sql.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                else:
                    i += 1
        elif c == "'" or (c in "eEuUbBxXnN" and sql[i + 1:i + 2] == "'") or \
                (c in "uU" and sql[i + 1:i + 3] == "&'"):
            start = i
            plain = c == "'"
            escaped = c in "eE"
            i = sql.index("'", i) + 1
            while i < n:
                if escaped and sql[i] == "\\":
                    i += 2
                elif sql[i] == "'" and sql[i + 1:i + 2] == "'":
                    i += 2
                elif sql[i] == "'":
                    i += 1
                    break
                else:
                    i += 1
            tokens.append(("string" if plain else "other_string", sql[start:i], start, i))
        elif c == "$":
            match = re.match(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$", sql[i:])
            if match:
                tag = match.group(0)
                end = sql.find(tag, i + len(tag))
                end = n if end < 0 else end + len(tag)
                tokens.append(("other_string", sql[i:end], i, end))
                i = end
            else:
                j = i + 1
                while j < n and sql[j].isdigit():
                    j += 1
                tokens.append(("param", sql[i:j], i, j))
                i = j
        elif c == '"':
            j = i + 1
            while j < n:
                if sql[j] == '"' and sql[j + 1:j + 2] == '"':
                    j += 2
                elif sql[j] == '"':
                    j += 1
                    break
                else:
                    j += 1
            tokens.append(("ident", sql[i:j], i, j))
            i = j
        elif c.isalpha() or c == "_" or ord(c) > 127:
            j = i + 1
            while j < n and (sql[j].isalnum() or sql[j] in "_$" or ord(sql[j]) > 127):
                j += 1
            tokens.append(("word", sql[i:j], i, j))
            i = j
        elif c.isdigit() or (c == "." and sql[i + 1:i + 2].isdigit()):
            match = re.match(r"(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?", sql[i:])
            tokens.append(("number", match.group(0), i, i + match.end()))
            i += match.end()
        elif c in OPERATOR_CHARS:
            j = i
            while j < n and sql[j] in OPERATOR_CHARS and not sql.startswith("--", j) \
                    and not sql.startswith("/*", j):
                j += 1
            tokens.append(("op", sql[i:j], i, j))
            i = j
        elif sql.startswith("::", i):
            tokens.append(("op", "::", i, i + 2))
            i += 2
        else:
            tokens.append(("symbol", c, i, i + 1))
            i += 1
    return tokens


def _name(token):
    kind, text = token[0], token[1]
    if kind == "word":
        return text.lower()
    if kind == "ident":
        return text[1:-1].replace('""', '"')
    return None


def _left_context_ok(token):
    if token is None:
        return True
    kind, text = token[0], token[1]
    if kind == "symbol":
        return text in {"(", ",", "["}
    if kind == "op":
        return text in COMPARISON_OPERATORS
    if kind == "word":
        return text.lower() not in TIGHT_KEYWORDS
    return False


def _right_context_ok(token):
    if token is None:
        return True
    kind, text = token[0], token[1]
    if kind == "symbol":
        return text in {")", ",", "]", ";"}
    if kind == "op":
        return text in COMPARISON_OPERATORS or text in {"->", "->>", "#>", "#>>"}
    if kind == "word":
        return text.lower() not in TIGHT_KEYWORDS
    return False


def rewrite_json_paths(statement, json_columns):
    """Collapse `col -> 'a' ->> 'b'` chains into `(col #>> ARRAY['a', 'b'])`.

    `->` copies the intermediate JSON value for every step; the path operator
    walks the stored document once. For json and jsonb the two forms return
    the same value whenever no key looks like an array index, so chains with
    integer-like keys are left untouched. Only column references whose final
    name is a json/jsonb column of the source are rewritten, and only when the
    neighbouring tokens cannot bind tighter than the JSON operators.

    Returns (rewritten_statement, number_of_chains_rewritten).
    """
    json_columns = {str(name) for name in json_columns}
    if not json_columns:
        return statement, 0
    tokens = lex(statement)
    replacements = []
    i = 0
    while i < len(tokens):
        # Column reference: name(.name){0,2}
        if _name(tokens[i]) is None or (i > 0 and tokens[i - 1][1] == "."):
            i += 1
            continue
        end = i + 1
        while end + 1 < len(tokens) and tokens[end][1] == "." and _name(tokens[end + 1]) is not None \
                and end - i < 4:
            end += 2
        column = _name(tokens[end - 1])
        if column not in json_columns:
            i = end
            continue
        keys = []
        j = end
        last_operator = None
        while j + 1 < len(tokens) and tokens[j][0] == "op" and tokens[j][1] in JSON_STEP_OPERATORS \
                and tokens[j + 1][0] == "string":
            key_value = tokens[j + 1][1][1:-1].replace("''", "'")
            if INTEGER_LIKE.match(key_value):
                break
            keys.append(tokens[j + 1][1])
            last_operator = tokens[j][1]
            j += 2
            if last_operator == "->>":
                break
        if len(keys) >= 2:
            left = tokens[i - 1] if i > 0 else None
            right = tokens[j] if j < len(tokens) else None
            if _left_context_ok(left) and _right_context_ok(right):
                reference = statement[tokens[i][2]:tokens[end - 1][3]]
                operator = "#>>" if last_operator == "->>" else "#>"
                replacements.append((tokens[i][2], tokens[j - 1][3],
                                     f"({reference} {operator} ARRAY[{', '.join(keys)}])"))
                i = j
                continue
        i = end
    for start, stop, text in reversed(replacements):
        statement = statement[:start] + text + statement[stop:]
    return statement, len(replacements)


def defer_foreign_keys_sql(relation_literals):
    """Drop validated FKs of the destinations; they are re-added before COMMIT.

    `relation_literals` are SQL string literals of the qualified destination names.
    Only constraints the current role can recreate are deferred; anything else
    keeps its per-row triggers.
    """
    return f"""
CREATE TEMP TABLE pgdm_deferred_foreign_keys ON COMMIT DROP AS
SELECT
    row_number() OVER (ORDER BY constraint_data.conrelid, constraint_data.conname) AS position,
    constraint_data.conrelid::regclass::text AS relation_name,
    constraint_data.conname::text AS constraint_name,
    pg_get_constraintdef(constraint_data.oid) AS definition,
    obj_description(constraint_data.oid, 'pg_constraint') AS comment
FROM pg_constraint AS constraint_data
JOIN pg_class AS relation ON relation.oid = constraint_data.conrelid
WHERE constraint_data.contype = 'f'
  AND constraint_data.convalidated
  AND constraint_data.conparentid = 0
  AND relation.relkind = 'r'
  AND constraint_data.conrelid IN ({", ".join(f"{name}::regclass" for name in relation_literals)})
  AND pg_has_role(relation.relowner, 'USAGE')
  AND has_table_privilege(constraint_data.confrelid, 'REFERENCES');

DO $pgdm_defer_fk$
DECLARE
    item record;
BEGIN
    FOR item IN SELECT * FROM pg_temp.pgdm_deferred_foreign_keys ORDER BY position LOOP
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', item.relation_name, item.constraint_name);
    END LOOP;
END
$pgdm_defer_fk$;""".strip()


def restore_foreign_keys_sql():
    """Re-create and fully validate every deferred FK (one scan per constraint)."""
    return """
DO $pgdm_restore_fk$
DECLARE
    item record;
BEGIN
    FOR item IN SELECT * FROM pg_temp.pgdm_deferred_foreign_keys ORDER BY position LOOP
        EXECUTE format(
            'ALTER TABLE %s ADD CONSTRAINT %I %s',
            item.relation_name, item.constraint_name, item.definition
        );
        IF item.comment IS NOT NULL THEN
            EXECUTE format(
                'COMMENT ON CONSTRAINT %I ON %s IS %L',
                item.constraint_name, item.relation_name, item.comment
            );
        END IF;
    END LOOP;
END
$pgdm_restore_fk$;""".strip()


# ---------------------------------------------------------------------------
# Parallel staging
#
# PostgreSQL never parallelizes the SELECT part of INSERT ... SELECT, but it
# does parallelize CREATE TABLE AS. A recipe is therefore evaluated by a
# parallel CREATE TEMP TABLE ... AS ... ORDER BY, and the destination is then
# filled from that table in its physical order. The sort key reproduces the
# order in which the original single statement produced its rows (source scan
# order, or the sorted output of SELECT DISTINCT), so identity values are
# assigned to the same rows. Statements whose order cannot be described that
# way keep the original single-statement execution.

STAGE_BLOCKING_WORDS = {
    "union", "intersect", "except", "order", "limit", "offset", "fetch", "for",
    "group", "having", "window", "over", "into", "tablesample",
}
ALIAS_STOP_WORDS = {
    "where", "join", "inner", "left", "right", "full", "cross", "natural", "on",
    "using", "group", "order", "limit", "offset", "having", "window", "union",
    "intersect", "except", "fetch", "for", "lateral", "outer",
}
SOURCE_CTID_COLUMN = "pgdm_source_ctid"


def _with_depth(tokens):
    depth, result = 0, []
    for token in tokens:
        if token[0] == "symbol" and token[1] in ")]":
            depth -= 1
        result.append((token, depth))
        if token[0] == "symbol" and token[1] in "([":
            depth += 1
    return result


def _top_level_items(tokens, start, stop, level=0):
    """Number of comma separated items in tokens[start:stop] at nesting `level`."""
    if start >= stop:
        return 0
    return 1 + sum(
        1 for token, depth in tokens[start:stop]
        if depth == level and token[0] == "symbol" and token[1] == ","
    )


def plan_staged_insert(statement, source_sql, blocked_functions=()):
    """Describe how `INSERT INTO t (cols) SELECT ...` can run through a parallel stage.

    `source_sql` is the exact source relation text substituted for {{source}}.
    Returns None when the statement has to keep the original execution.
    """
    if not source_sql or statement.count(source_sql) != 1:
        return None
    if not source_sql.lstrip().startswith("(") or "SELECT *" not in source_sql:
        return None
    tokens = _with_depth(lex(statement))
    words = [(index, token[1].lower()) for index, (token, _depth) in enumerate(tokens)
             if token[0] == "word"]
    blocked_functions = {str(name).lower() for name in blocked_functions}
    for index, word in words:
        if word.startswith("pgdm_") or word == "over":
            return None
        if word in blocked_functions and index + 1 < len(tokens) \
                and tokens[index + 1][0][1] == "(":
            return None
    for index, (token, _depth) in enumerate(tokens):
        if token[0] == "op" and token[1] == "*" and index and tokens[index - 1][0][1] == ".":
            return None
        if token[0] == "ident" and _name(token).lower().startswith("pgdm_"):
            return None

    select_index = next((index for index, word in words
                         if word == "select" and tokens[index][1] == 0), None)
    if select_index is None or select_index < 2:
        return None
    closing = tokens[select_index - 1]
    if closing[0][1] != ")" or closing[1] != 0:
        return None
    opening_index = max(index for index in range(select_index - 1)
                        if tokens[index][0][1] == "(" and tokens[index][1] == 0)
    column_count = _top_level_items(tokens, opening_index + 1, select_index - 1, level=1)
    column_tokens = [token for token, _depth in tokens[opening_index + 1:select_index - 1]]
    column_names = [_name(token) for token in column_tokens[::2]]
    if (
        len(column_tokens) != 2 * column_count - 1
        or None in column_names
        or any(token[1] != "," for token in column_tokens[1::2])
    ):
        return None

    list_start = select_index + 1
    distinct = False
    if list_start < len(tokens) and tokens[list_start][0][0] == "word":
        modifier = tokens[list_start][0][1].lower()
        if modifier == "distinct":
            if list_start + 1 < len(tokens) and tokens[list_start + 1][0][1].lower() == "on":
                return None
            distinct = True
            list_start += 1
        elif modifier == "all":
            list_start += 1
    top_words = [word for index, word in words if index > select_index and tokens[index][1] == 0]
    if STAGE_BLOCKING_WORDS.intersection(top_words) or "from" not in top_words:
        return None
    from_index = next(index for index, word in words
                      if index > select_index and word == "from" and tokens[index][1] == 0)
    for index in range(list_start, from_index):
        token, depth = tokens[index]
        if token[0] == "op" and token[1] == "*" and depth == 0:
            if index == list_start or tokens[index - 1][0][1] == ",":
                return None
    if column_count == 0 or _top_level_items(tokens, list_start, from_index) != column_count:
        return None

    source_start = statement.index(source_sql)
    source_end = source_start + len(source_sql)
    after = [index for index, (token, _depth) in enumerate(tokens) if token[2] >= source_end]
    if not after:
        return None
    alias_index = after[0]
    if tokens[alias_index][0][0] == "word" and tokens[alias_index][0][1].lower() == "as":
        alias_index += 1
    if alias_index >= len(tokens):
        return None
    alias_token = tokens[alias_index][0]
    alias = _name(alias_token)
    if alias is None or (alias_token[0] == "word" and alias in ALIAS_STOP_WORDS):
        return None
    if alias_index + 1 < len(tokens) and tokens[alias_index + 1][0][1] == "(":
        return None  # a column alias list renames the source columns
    for index, (token, _depth) in enumerate(tokens):
        if index == alias_index or (token[2] < source_end and token[3] > source_start):
            continue
        if _name(token) != alias or (index and tokens[index - 1][0][1] == "."):
            continue
        following = tokens[index + 1][0] if index + 1 < len(tokens) else None
        if following is None or following[1] != ".":
            return None  # whole-row reference to the source

    select_start = tokens[select_index][0][2]
    list_start_offset = tokens[list_start][0][2]
    names = [f"pgdm_c{position}" for position in range(1, column_count + 1)]
    if distinct:
        select_sql = statement[select_start:]
        stage_columns = names
    else:
        stage_source = source_sql.replace(
            "SELECT *", f"SELECT *, ctid AS {SOURCE_CTID_COLUMN}", 1
        )
        select_sql = (
            statement[select_start:list_start_offset]
            + f"{statement[alias_token[2]:alias_token[3]]}.{SOURCE_CTID_COLUMN}, "
            + statement[list_start_offset:source_start]
            + stage_source
            + statement[source_end:]
        )
        stage_columns = [SOURCE_CTID_COLUMN] + names
    into_index = next(index for index, word in words if word == "into")
    return {
        "insert_prefix": statement[:select_start].rstrip(),
        "target_sql": _one_line(statement[tokens[into_index][0][3]:closing[0][3]]),
        "column_names": column_names,
        "select_sql": select_sql.rstrip().rstrip(";").rstrip(),
        "stage_columns": stage_columns,
        "output_columns": names,
        "distinct": distinct,
    }


def _one_line(sql):
    """Rebuild `sql` on one line from its tokens (comments dropped), or None."""
    tokens = lex(sql)
    if any("\n" in token[1] or "\r" in token[1] for token in tokens):
        return None
    return " ".join(token[1] for token in tokens)


def _stage_query(plan):
    columns = ", ".join(plan["stage_columns"])
    return f"SELECT * FROM (\n{plan['select_sql']}\n) AS pgdm_stage_query ({columns})"


def stage_probe_sql(plan, stage_name, destination_literal):
    """Plan the recipe (CREATE ... WITH NO DATA) and set three psql variables.

    `<stage>_order`  ORDER BY clause over the sortable stage columns.
    `<stage>_copy`   the evaluated rows can be loaded with binary COPY: every
                     output has the destination column's exact type (text may
                     go to varchar/bpchar, whose binary format is the same and
                     whose length rules are applied on input as on assignment),
                     every stage column is sortable, and the destination is a
                     plain table without row security or rules.
    `<stage>_insert` INSERT from the stage behaves like the original statement:
                     no output became text where the destination is not a
                     character type (an untyped literal resolves to text in a
                     subquery but to the column type in INSERT ... SELECT).
    """
    names = ", ".join(sql_string(name) for name in plan["column_names"])
    pairs = f"""
        FROM unnest(ARRAY[{names}]::text[]) WITH ORDINALITY AS target(name, position)
        LEFT JOIN pg_attribute AS destination
          ON destination.attrelid = {destination_literal}::regclass
         AND destination.attname = target.name AND NOT destination.attisdropped
        LEFT JOIN pg_attribute AS stage
          ON stage.attrelid = 'pg_temp.{stage_name}'::regclass
         AND stage.attname = 'pgdm_c' || target.position"""
    return f"""
CREATE TEMP TABLE {stage_name} ON COMMIT DROP AS {_stage_query(plan)} WITH NO DATA;
SELECT
    (SELECT COALESCE(' ORDER BY ' || string_agg(quote_ident(attribute.attname), ', '
                     ORDER BY attribute.attnum)
                     FILTER (WHERE pg_temp.pgdm_is_sortable(attribute.atttypid)), '')
     FROM pg_attribute AS attribute
     WHERE attribute.attrelid = 'pg_temp.{stage_name}'::regclass
       AND attribute.attnum > 0 AND NOT attribute.attisdropped) AS {stage_name}_order,
    (
        NOT EXISTS (
            SELECT 1 {pairs}
            WHERE destination.atttypid IS NULL
               OR destination.attidentity = 'a'
               OR NOT (stage.atttypid = destination.atttypid
                       OR (stage.atttypid = 'text'::regtype
                           AND destination.atttypid IN ('varchar'::regtype, 'bpchar'::regtype)))
        )
        AND NOT EXISTS (
            SELECT 1 FROM pg_attribute AS attribute
            WHERE attribute.attrelid = 'pg_temp.{stage_name}'::regclass
              AND attribute.attnum > 0 AND NOT attribute.attisdropped
              AND NOT pg_temp.pgdm_is_sortable(attribute.atttypid)
        )
        AND EXISTS (
            SELECT 1 FROM pg_class AS relation
            WHERE relation.oid = {destination_literal}::regclass
              AND relation.relkind = 'r'
              AND NOT relation.relrowsecurity
              AND NOT relation.relhasrules
        )
    ) AS {stage_name}_copy,
    NOT EXISTS (
        SELECT 1 {pairs}
        WHERE stage.atttypid = 'text'::regtype
          AND destination.atttypid NOT IN ('text'::regtype, 'varchar'::regtype, 'bpchar'::regtype)
    ) AS {stage_name}_insert
\\gset
DROP TABLE pg_temp.{stage_name};""".strip()


def stage_copy_sql(plan, file_name):
    """Return (copy_out, copy_in) psql commands, or None when the SQL cannot be one line.

    The ordered query runs with parallel workers inside COPY TO; psql writes the
    rows to `file_name` next to the batch and COPY FROM loads them in that order,
    so the identity default numbers them exactly as the INSERT would.
    """
    columns = ", ".join(plan["stage_columns"])
    query = _one_line(
        f"SELECT {', '.join(plan['output_columns'])} FROM ({plan['select_sql']}) "
        f"AS pgdm_stage_query ({columns}) ORDER BY {columns}"
    )
    if query is None or plan["target_sql"] is None or "'" in file_name:
        return None
    return (
        f"\\copy ({query}) TO '{file_name}' WITH (FORMAT binary)",
        f"\\copy {plan['target_sql']} FROM '{file_name}' WITH (FORMAT binary)",
    )


def staged_insert_sql(plan, stage_name):
    """Return (build_stage_sql, insert_from_stage_sql); run after stage_probe_sql()."""
    build = (
        f"CREATE TEMP TABLE {stage_name} ON COMMIT DROP AS {_stage_query(plan)}\n"
        f":{stage_name}_order;"
    )
    insert = (
        f"{plan['insert_prefix']}\n"
        f"SELECT {', '.join(plan['output_columns'])} FROM pg_temp.{stage_name}"
    )
    return build, insert


def distinct_order_check_sql(statement, variable):
    """Set psql variable `variable` to whether the original statement's DISTINCT is sorted.

    The original SELECT DISTINCT emits rows in sort-key order only when the
    planner implements it with Sort + Unique on the select list; a hashed
    DISTINCT emits them in hash-table order. Only the sorted case is staged.
    """
    tag = "$pgdm_statement$"
    suffix = 0
    while tag in statement:
        suffix += 1
        tag = f"$pgdm_statement_{suffix}$"
    return (
        f"SELECT pg_temp.pgdm_distinct_is_sorted({tag}{statement}{tag}) AS {variable}\n"
        "\\gset"
    )


def parallel_settings_sql():
    """Transaction-local settings for the expansion.

    `:pgdm_cpus` is set by the job launcher to the number of online CPUs;
    workers are capped by the server's own max_parallel_workers.
    """
    return """
SELECT
    set_config('max_parallel_workers_per_gather', settings.workers::text, true) AS gather,
    set_config('max_parallel_maintenance_workers', settings.workers::text, true) AS maintenance,
    set_config('min_parallel_table_scan_size', '1MB', true) AS scan_size
FROM (
    SELECT LEAST(GREATEST(:'pgdm_cpus'::integer - 1, 1),
                 current_setting('max_parallel_workers')::integer) AS workers
) AS settings
\\gset pgdm_parallel_
CREATE FUNCTION pg_temp.pgdm_distinct_is_sorted(statement text) RETURNS boolean
LANGUAGE plpgsql AS $pgdm_distinct$
DECLARE
    node jsonb;
    sort_key jsonb;
    previous_position bigint := 0;
    matched_position bigint;
BEGIN
    EXECUTE 'EXPLAIN (VERBOSE, FORMAT JSON) ' || statement INTO node;
    node := node -> 0 -> 'Plan';
    WHILE node ->> 'Node Type' IN ('ModifyTable', 'Subquery Scan', 'Result')
          AND jsonb_array_length(COALESCE(node -> 'Plans', '[]')) = 1 LOOP
        node := node -> 'Plans' -> 0;
    END LOOP;
    IF node ->> 'Node Type' IS DISTINCT FROM 'Unique' THEN
        RETURN false;
    END IF;
    node := node -> 'Plans' -> 0;
    IF node ->> 'Node Type' IS DISTINCT FROM 'Sort' THEN
        RETURN false;
    END IF;
    -- The sort key must follow the select list order. The planner leaves out
    -- constant and duplicate columns, which do not change the order.
    FOR sort_key IN SELECT value FROM jsonb_array_elements(node -> 'Sort Key') LOOP
        SELECT min(item.position) INTO matched_position
        FROM jsonb_array_elements(node -> 'Output') WITH ORDINALITY AS item(value, position)
        WHERE item.position > previous_position AND item.value = sort_key;
        IF matched_position IS NULL THEN
            RETURN false;
        END IF;
        previous_position := matched_position;
    END LOOP;
    RETURN true;
END
$pgdm_distinct$;
CREATE FUNCTION pg_temp.pgdm_is_sortable(type_oid oid) RETURNS boolean
LANGUAGE sql STABLE AS $pgdm_sortable$
    SELECT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_type AS type_data
        LEFT JOIN pg_catalog.pg_type AS element
          ON element.oid = type_data.typelem AND type_data.typcategory = 'A'
        CROSS JOIN LATERAL (
            SELECT COALESCE(element.oid, type_data.oid) AS input_type,
                   COALESCE(element.typtype, type_data.typtype) AS input_kind
        ) AS checked
        WHERE type_data.oid = type_oid
          AND (
              checked.input_kind = 'e'
              OR EXISTS (
                  SELECT 1
                  FROM pg_catalog.pg_opclass AS class
                  JOIN pg_catalog.pg_am AS method ON method.oid = class.opcmethod
                  WHERE method.amname = 'btree'
                    AND class.opcdefault
                    AND (
                        class.opcintype = checked.input_type
                        OR class.opcintype IN (
                            SELECT cast_data.casttarget
                            FROM pg_catalog.pg_cast AS cast_data
                            WHERE cast_data.castsource = checked.input_type
                              AND cast_data.castmethod = 'b'
                        )
                    )
              )
          )
    )
$pgdm_sortable$;""".strip()


def defer_indexes_sql(relation_literals):
    """Drop the destinations' btree indexes; restore_indexes_sql() rebuilds them.

    Run after defer_foreign_keys_sql(): an index still used by a foreign key
    that was not deferred is kept, as are indexes the role does not own,
    partition indexes, clustered or replica-identity indexes and indexes
    outside the default tablespace. A duplicate makes the rebuild fail, which
    aborts the transaction like a failing row insert.
    """
    return f"""
CREATE TEMP TABLE pgdm_deferred_indexes ON COMMIT DROP AS
SELECT
    row_number() OVER (ORDER BY index_data.indrelid, index_class.relname) AS position,
    index_data.indrelid::regclass::text AS relation_name,
    index_data.indexrelid::regclass::text AS qualified_index_name,
    constraint_data.conname::text AS constraint_name,
    CASE
        WHEN constraint_data.oid IS NULL THEN pg_get_indexdef(index_data.indexrelid)
        ELSE pg_get_constraintdef(constraint_data.oid)
    END AS definition,
    obj_description(index_data.indexrelid, 'pg_class') AS index_comment,
    CASE WHEN constraint_data.oid IS NOT NULL
         THEN obj_description(constraint_data.oid, 'pg_constraint') END AS constraint_comment
FROM pg_index AS index_data
JOIN pg_class AS index_class ON index_class.oid = index_data.indexrelid
JOIN pg_class AS relation ON relation.oid = index_data.indrelid
JOIN pg_am AS method ON method.oid = index_class.relam
LEFT JOIN pg_constraint AS constraint_data
  ON constraint_data.conindid = index_data.indexrelid
 AND constraint_data.conrelid = index_data.indrelid
 AND constraint_data.contype IN ('p', 'u')
WHERE index_data.indrelid IN ({", ".join(f"{name}::regclass" for name in relation_literals)})
  AND relation.relkind = 'r'
  AND method.amname = 'btree'
  AND pg_has_role(relation.relowner, 'USAGE')
  AND index_data.indisvalid AND index_data.indisready AND index_data.indislive
  AND NOT index_data.indisclustered
  AND NOT index_data.indisreplident
  AND index_class.reltablespace = 0
  AND (constraint_data.oid IS NULL OR constraint_data.conparentid = 0)
  AND NOT EXISTS (SELECT 1 FROM pg_inherits WHERE pg_inherits.inhrelid = index_data.indexrelid)
  AND NOT EXISTS (
      SELECT 1 FROM pg_constraint AS other
      WHERE other.conindid = index_data.indexrelid
        AND (other.contype NOT IN ('p', 'u') OR other.conrelid <> index_data.indrelid)
  );

DO $pgdm_defer_index$
DECLARE
    item record;
BEGIN
    FOR item IN SELECT * FROM pg_temp.pgdm_deferred_indexes ORDER BY position LOOP
        IF item.constraint_name IS NULL THEN
            EXECUTE format('DROP INDEX %s', item.qualified_index_name);
        ELSE
            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I',
                           item.relation_name, item.constraint_name);
        END IF;
    END LOOP;
END
$pgdm_defer_index$;""".strip()


def restore_indexes_sql():
    """Rebuild every deferred index with its original definition and comments."""
    return """
DO $pgdm_restore_index$
DECLARE
    item record;
BEGIN
    FOR item IN SELECT * FROM pg_temp.pgdm_deferred_indexes ORDER BY position LOOP
        IF item.constraint_name IS NULL THEN
            EXECUTE item.definition;
        ELSE
            EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I %s',
                           item.relation_name, item.constraint_name, item.definition);
            IF item.constraint_comment IS NOT NULL THEN
                EXECUTE format('COMMENT ON CONSTRAINT %I ON %s IS %L',
                               item.constraint_name, item.relation_name,
                               item.constraint_comment);
            END IF;
        END IF;
        IF item.index_comment IS NOT NULL THEN
            EXECUTE format('COMMENT ON INDEX %s IS %L',
                           item.qualified_index_name, item.index_comment);
        END IF;
    END LOOP;
END
$pgdm_restore_index$;""".strip()
