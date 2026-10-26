"""Guarded SQL experiments. No casts, filters or output expressions are removed.

Lazy controls call the pinned QueryFoundry submission directly. The initial
independent payload projection remains a separate historical experiment.
All modes except baseline are research candidates until measured and validated.
"""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
ORDER = ("table3", "table4", "table1", "table5", "table6", "table2")
ANCHOR = "FROM {{source}} AS source_row"
EXTRACT = re.compile(r"source_row\.raw\s*->\s*'values'\s*->>\s*'([a-z_]+\d+)'")
MODES = ("baseline", "payload_once", "fields_once", "lazy_keys", "record_once",
         "record_table1", "record_table2", "each_once", "path_once", "array_values", "path_table1", "path_array",
         "qf_payload", "qf_lazy", "qf_lazy_path1")


def rewrite(sql, mode):
    if mode not in MODES:
        raise ValueError(f"Unknown recipe mode: {mode}")
    if mode == "baseline":
        return sql
    if mode in ("qf_payload","qf_lazy","qf_lazy_path1","lazy_keys"):
        from vendor.queryfoundry_recipes import rewrite as original_queryfoundry
        if mode=="qf_lazy_path1" and sql.startswith("INSERT INTO public.table1 ("):
            return rewrite(sql,"path_once")
        return original_queryfoundry(sql,"payload_once" if mode=="qf_payload" else "lazy_keys")
    if mode == "path_table1":
        return rewrite(sql,"path_once") if sql.startswith("INSERT INTO public.table1 (") else sql
    if mode == "path_array":
        return rewrite(sql,"path_once") if sql.startswith("INSERT INTO public.table1 (") else rewrite(sql,"array_values")
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
            return sql
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
    if mode == "payload_once":
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
        columns = ", ".join(f"max(CASE WHEN key = '{field}' THEN value END) AS {field}" for field in fields)
        extraction = f"\nCROSS JOIN LATERAL (SELECT {columns} FROM jsonb_each_text({safe}) OFFSET 0) AS fields"
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
