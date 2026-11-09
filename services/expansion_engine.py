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
