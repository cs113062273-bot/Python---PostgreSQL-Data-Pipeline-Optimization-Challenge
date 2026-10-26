"""Guarded SQL experiments. No casts, filters or output expressions are removed.

Public controls independently implement the QueryFoundry extraction strategies.
All modes except baseline are research candidates until measured and validated.
"""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
ORDER = ("table3", "table4", "table1", "table5", "table6", "table2")
ANCHOR = "FROM {{source}} AS source_row"
EXTRACT = re.compile(r"source_row\.raw\s*->\s*'values'\s*->>\s*'([a-z_]+\d+)'")
MODES = ("baseline", "payload_once", "fields_once", "lazy_keys", "record_once",
         "record_table1", "record_table2", "each_once", "path_once", "array_values")


def rewrite(sql, mode):
    if mode not in MODES:
        raise ValueError(f"Unknown recipe mode: {mode}")
    if mode == "baseline":
        return sql
    if mode in ("record_table1", "record_table2"):
        target = "table1" if mode == "record_table1" else "table2"
        mode = "record_once" if sql.startswith(f"INSERT INTO public.{target} (") else "baseline"
        return rewrite(sql, mode)
    fields = sorted(set(EXTRACT.findall(sql)))
    if not fields or sql.count(ANCHOR) != 1:
        raise ValueError("Unrecognized official recipe; refusing partial rewrite")
    if mode == "path_once":
        # A different operator implementation, without changing relational shape.
        return EXTRACT.sub(lambda m: "source_row.raw #>> '{values," + m[1] + "}'", sql)
    payload = "\nCROSS JOIN LATERAL (SELECT source_row.raw -> 'values' AS payload OFFSET 0) AS projected"
    if mode == "array_values":
        if "AS position_data(" not in sql:
            return rewrite(sql,"payload_once")
        matrix = re.search(r"CROSS JOIN LATERAL \(\s*VALUES\s*(.*?)\s*\) AS position_data\([^)]*\)",sql,re.S)
        if not matrix or len(EXTRACT.findall(matrix[1])) != 56:
            raise ValueError("Unrecognized table2 matrix")
        arrays = ["ARRAY["+",".join(f"{n}::smallint" for n in range(1,15))+"]"]
        keys = EXTRACT.findall(matrix[1])
        for attribute in range(4):
            arrays.append("ARRAY["+",".join(f"projected.payload ->> '{key}'" for key in keys[attribute::4])+"]")
        expansion = "CROSS JOIN LATERAL unnest("+",\n".join(arrays)+") AS position_data(position_number, declared_number_text, type_text, weight_text, gap_text)"
        sql = sql[:matrix.start()]+expansion+sql[matrix.end():]
        return sql.replace(ANCHOR,ANCHOR+payload)
    if mode == "lazy_keys" and "AS position_data(" in sql:
        matrix = re.search(r"CROSS JOIN LATERAL \(\s*VALUES\s*(.*?)\s*\) AS position_data\([^)]*\)", sql, re.S)
        if not matrix:
            raise ValueError("Unrecognized table2 matrix")
        tuples = []
        for n in range(1, 15):
            keys = [f"integer_column_{n+1}", f"text_column_{n+3}",
                    f"numeric_column_{2*n+1}", f"numeric_column_{2*n+2}"]
            tuples.append(f"({n}::smallint, " + ", ".join(f"'{key}'" for key in keys) + ")")
        expected = [key for n in range(1, 15) for key in
                    (f"integer_column_{n+1}", f"text_column_{n+3}", f"numeric_column_{2*n+1}", f"numeric_column_{2*n+2}")]
        if EXTRACT.findall(matrix[1]) != expected:
            raise ValueError("Official position mapping changed")
        replacement = "CROSS JOIN LATERAL (VALUES\n" + ",\n".join(tuples) + ") AS keys(position_number, declared_key, type_key, weight_key, gap_key)"
        sql = sql[:matrix.start()] + replacement + sql[matrix.end():]
        for field, key in (("declared_number", "declared"), ("type", "type"), ("weight", "weight"), ("gap", "gap")):
            sql = sql.replace(f"position_data.{field}_text", f"(projected.payload ->> keys.{key}_key)")
        sql = sql.replace("position_data.position_number", "keys.position_number")
        return sql.replace(ANCHOR, ANCHOR + payload)
    if mode in ("payload_once", "lazy_keys"):
        return EXTRACT.sub(lambda m: "projected.payload ->> '" + m[1] + "'", sql).replace(ANCHOR, ANCHOR + payload)
    # Extraction operators yield NULL for non-object inputs. SRFs throw on them;
    # normalize only the SRF's input shape to retain the operator behavior.
    safe = "CASE WHEN jsonb_typeof(projected.payload) = 'object' THEN projected.payload ELSE '{}'::jsonb END"
    if mode == "record_once":
        columns = ", ".join(f"{field} text" for field in fields)
        extraction = f"\nCROSS JOIN LATERAL jsonb_to_record({safe}) AS fields({columns})"
    elif mode == "fields_once":
        columns = ", ".join(f"projected.payload ->> '{field}' AS {field}" for field in fields)
        extraction = f"\nCROSS JOIN LATERAL (SELECT {columns} OFFSET 0) AS fields"
    elif mode == "each_once":
        columns = ", ".join(f"max(value) FILTER (WHERE key = '{field}') AS {field}" for field in fields)
        extraction = f"\nCROSS JOIN LATERAL (SELECT {columns} FROM jsonb_each_text({safe})) AS fields"
    else:
        raise AssertionError(mode)
    return EXTRACT.sub(lambda m: "fields." + m[1], sql).replace(ANCHOR, ANCHOR + payload + extraction)


def destinations(mode="baseline"):
    files = sorted((ROOT / "app" / "SQL files").glob("*.sql"))
    if len(files) != len(ORDER):
        raise RuntimeError("Expected six official SQL files")
    return [{"table_name": f"public.{table}", "version_title": f"Expansion {mode}",
             "sql": rewrite(path.read_text(encoding="utf-8"), mode)}
            for table, path in zip(ORDER, files)]
