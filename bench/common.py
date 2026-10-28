import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import uuid

import psycopg
from psycopg.conninfo import make_conninfo

ROOT = Path(__file__).resolve().parents[1]


def connect(database="lab_100k", **kwargs):
    # Credentials stay in libpq environment / pgpass, never in reports.
    kwargs.setdefault("connect_timeout", 10)
    return psycopg.connect(make_conninfo(os.environ.get("CHALLENGE_DSN", ""), dbname=database), **kwargs)


def require_lab(conn):
    name = conn.execute("SELECT current_database()").fetchone()[0]
    if not name.startswith("lab_"):
        raise RuntimeError("Destructive diagnostics require an explicitly created lab_* database")
    marker = conn.execute("SELECT to_regclass('challenge_lab.disposable')").fetchone()[0]
    if not marker:
        raise RuntimeError("Missing disposable-lab marker; run python -m bench.prepare first")


def write_report(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        with temp.open('x', encoding='utf-8', newline='\n') as writer:
            writer.write(json.dumps(value, indent=2, ensure_ascii=False, default=str) + "\n")
            writer.flush()
            os.fsync(writer.fileno())
        temp.replace(path)
    except BaseException:
        temp.unlink(missing_ok=True)
        raise


def environment(conn):
    settings = dict(conn.execute("SELECT name, setting FROM pg_settings WHERE name = ANY(%s)",
        (["server_version", "jit", "work_mem", "maintenance_work_mem", "shared_buffers",
          "fsync", "synchronous_commit", "full_page_writes", "wal_level", "max_wal_size",
          "max_parallel_workers_per_gather", "max_parallel_workers", "TimeZone", "lc_collate"],)).fetchall())
    return {"python": sys.version, "platform": platform.platform(), "cpu": platform.processor(),
            "server": conn.execute("SELECT version()").fetchone()[0], "settings": settings,
            "jit_available": conn.execute("SELECT pg_jit_available()").fetchone()[0],
            "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "git_status": subprocess.check_output(["git", "status", "--short"], cwd=ROOT, text=True).strip(),
            "generator_sha256": hashlib.sha256((ROOT / "app/generate_database.py").read_bytes()).hexdigest()}
