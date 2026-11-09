"""Run the Expand flow without the GUI and print the performance report.

Uses the same service calls as main.py (_execute_cross_table_expansion_thread):
recovery check, execute_cross_table_expansion, version registration.

  python tools/run_expansion.py --ssh 10.0.0.5 --ssh-user ubuntu --user challenge
  python tools/run_expansion.py --app /path/to/original/checkout --ssh ...   # baseline

Passwords: SSH_PASSWORD and PG_KAGGLE_CHALLENGE_PASS environment variables
(prompted for when unset).

--local-bash runs the "remote" commands in a local bash instead of SSH. It is
only meant for a lab where PostgreSQL and bash run on the same machine.
"""
import argparse
import getpass
import importlib
import os
import queue
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RECIPES = [
    ("public.table3", "01_table3.sql"),
    ("public.table4", "02_table4.sql"),
    ("public.table1", "03_table1.sql"),
    ("public.table5", "04_table5.sql"),
    ("public.table6", "05_table6.sql"),
    ("public.table2", "06_table2.sql"),
]


class _LocalChannel:
    def __init__(self, process):
        self.process = process
        self.stdout = queue.Queue()
        self.stderr = queue.Queue()
        self.readers = [
            threading.Thread(target=self._pump, args=(process.stdout, self.stdout), daemon=True),
            threading.Thread(target=self._pump, args=(process.stderr, self.stderr), daemon=True),
        ]
        for reader in self.readers:
            reader.start()

    @staticmethod
    def _pump(stream, target):
        for chunk in iter(lambda: stream.read1(65536), b""):
            target.put(chunk)

    def recv_ready(self):
        return not self.stdout.empty()

    def recv_stderr_ready(self):
        return not self.stderr.empty()

    def recv(self, _size):
        return self.stdout.get_nowait()

    def recv_stderr(self, _size):
        return self.stderr.get_nowait()

    def exit_status_ready(self):
        return self.process.poll() is not None and not any(r.is_alive() for r in self.readers)

    def recv_exit_status(self):
        return self.process.wait()

    def sendall(self, data):
        self.process.stdin.write(data)

    def shutdown_write(self):
        self.process.stdin.close()

    def close(self):
        if self.process.poll() is None:
            self.process.kill()


class _Stream:
    def __init__(self, channel):
        self.channel = channel


class LocalBashClient:
    """Stand-in for paramiko.SSHClient.exec_command backed by a local bash."""

    def __init__(self, path_prefix=None):
        self.environment = dict(os.environ)
        if path_prefix:
            self.environment["PATH"] = path_prefix + os.pathsep + self.environment["PATH"]

    def exec_command(self, command):
        process = subprocess.Popen(["bash", "-c", command], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   env=self.environment)
        channel = _LocalChannel(process)
        return _Stream(channel), _Stream(channel), _Stream(channel)

    def close(self):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--app", default=str(ROOT), help="checkout to run (default: this repository)")
    transport = parser.add_mutually_exclusive_group(required=True)
    transport.add_argument("--ssh", metavar="HOST")
    transport.add_argument("--local-bash", action="store_true")
    parser.add_argument("--ssh-port", type=int, default=22)
    parser.add_argument("--ssh-user", default=getpass.getuser())
    parser.add_argument("--postgres-port", type=int, default=5432)
    parser.add_argument("--user", required=True, help="PostgreSQL role")
    parser.add_argument("--database", default="kaggle_challenge")
    parser.add_argument("--source", default="public.raw_data")
    parser.add_argument("--pg-bin", help="--local-bash only: PostgreSQL bin directory to put on PATH")
    parser.add_argument("--storage", help="--local-bash only: directory used as remote storage")
    parser.add_argument("--report", help="write the report text to this file")
    args = parser.parse_args()

    app = Path(args.app).resolve()
    sys.path.insert(0, str(app))
    os.chdir(app)
    service_module = importlib.import_module("services.postgres_service")
    service = service_module.PostgresAdminService()

    sql_password = os.environ.get("PG_KAGGLE_CHALLENGE_PASS")
    if sql_password is None:
        sql_password = getpass.getpass("PostgreSQL password: ")
    if args.ssh:
        ssh_password = os.environ.get("SSH_PASSWORD") or getpass.getpass("SSH password: ")
        service.connect(args.ssh, args.ssh_port, args.ssh_user, ssh_password,
                        args.postgres_port, args.user, sql_password)
    else:
        if args.storage:
            storage = args.storage
            type(service).remote_storage_root = property(lambda self: storage)
        service.ssh_client = LocalBashClient(args.pg_bin)
        service.ssh_username = args.ssh_user
        service.postgres_port = args.postgres_port
        service.sql_username = args.user
        service.sql_password = sql_password

    destinations = [
        {"table_name": table, "version_title": f"Expand {table}",
         "sql": (app / "SQL files" / name).read_text(encoding="utf-8")}
        for table, name in RECIPES
    ]
    available = service.list_table_raw_schemas(args.database, args.source)
    raw_schemas = service.resolve_raw_schema_interval(available, available[0], available[-1])

    started = time.perf_counter()
    report = service.create_cross_table_expansion_timing_report(
        args.database, args.source, raw_schemas, [item["table_name"] for item in destinations])
    with service.expansion_timing_scope(report, "prepare", "Ensure control database metadata"):
        service.ensure_admin_schema()

    optimized = hasattr(service, "recover_cross_table_expansions")
    if optimized:
        recovered = service.recover_cross_table_expansions(args.database, timing_report=report)
        fingerprint = service.cross_table_expansion_fingerprint(
            args.database, args.source, raw_schemas,
            service._prepare_cross_table_expansion(args.source, raw_schemas, destinations)["destinations"])
        for job in recovered:
            versions = ", ".join(f"{v['table_name']}={v['version_code']}" for v in job["versions"])
            print(f"recovered interrupted job {job['job_id']}: {versions}")
        if any(job["request_fingerprint"] == fingerprint for job in recovered):
            print("this expansion was already committed by the interrupted run; not executed again")
            return

    extra = {"requested_by": getpass.getuser(), "workstation_name": socket.gethostname()} if optimized else {}
    result = service.execute_cross_table_expansion(
        database_name=args.database, source_full_table_name=args.source,
        raw_schemas=raw_schemas, destinations=destinations, timing_report=report, **extra)
    versions = service.register_cross_table_expansion_versions(
        expansion_result=result, requested_by=getpass.getuser(),
        workstation_name=socket.gethostname(), timing_report=report)
    wall = time.perf_counter() - started
    report["completed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S%z")

    dialogs = importlib.import_module("ui.dialogs")
    text = dialogs.ExpansionTimingReportDialog.build_report_text(report)
    if args.report:
        Path(args.report).write_text(text, encoding="utf-8")
    else:
        print(text)
    total = dialogs.ExpansionTimingReportDialog.measured_total_seconds(report)
    print(f"versions={len(versions)} client_wall={wall:.3f}s MEASURED PROCESSING TOTAL={total:.3f}s")


if __name__ == "__main__":
    main()
