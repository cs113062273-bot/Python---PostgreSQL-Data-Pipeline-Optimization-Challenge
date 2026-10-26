"""Recoverable data/control saga retaining organizer version/dependency SQL.

The narrow SQL capture pattern follows the documented public QueryFoundry
approach (Apache-2.0); source and chunk execution here are independently built.
No distributed atomicity is claimed. Data stays fenced until control completion.
"""
import json
import shlex
from psycopg.types.json import Jsonb
from pipeline.checkpoints import LOCK_KEY, catalog, source_digest
from pipeline.upstream import PostgresAdminService
from config import CONTROL_DB
from utils import sql_literal

INSTALL = """CREATE SCHEMA IF NOT EXISTS pipeline_recovery;
CREATE TABLE IF NOT EXISTS pipeline_recovery.version_receipts(
 job_id uuid PRIMARY KEY, request_sha256 text NOT NULL, result jsonb NOT NULL,
 committed_at timestamptz NOT NULL DEFAULT clock_timestamp());"""


def audit_query(versions_expression,database):
    return """SELECT coalesce(jsonb_agg(jsonb_build_object('version',to_jsonb(v),
       'dependencies',(SELECT coalesce(jsonb_agg(to_jsonb(d) ORDER BY d.id),'[]'::jsonb)
          FROM app_control.table_version_dependencies d WHERE d.table_version_id=v.id)) ORDER BY v.table_name),'[]'::jsonb)
       FROM app_control.table_versions v JOIN jsonb_array_elements(""" + versions_expression + """) e
       ON v.schema_name||'.'||v.table_name=e->>'table_name' AND v.version_code=e->>'version_code'
       WHERE v.database_name=""" + sql_literal(database)


class VersionReceiptMixin:
    def _receipt_command(self,sql):
        command = f"psql -h localhost -U {shlex.quote(self.sql_username)} -d {shlex.quote(CONTROL_DB)} -X -qAt -v ON_ERROR_STOP=1 -f -"
        return self.run_remote_command(command,stdin_text="SET client_min_messages=warning;\n"+sql)

    def _read_version_receipt(self,job,signature,database):
        output = self._receipt_command("SELECT jsonb_build_object('signature',request_sha256,'result',result)::text FROM pipeline_recovery.version_receipts WHERE job_id="+sql_literal(job)+"::uuid;")
        if not output.strip():
            return None
        receipt = json.loads(output.splitlines()[-1])
        if receipt["signature"]!=signature:
            raise RuntimeError("Control receipt request identity differs")
        result = receipt["result"]
        query = audit_query(sql_literal(json.dumps(result["versions"]))+"::jsonb",database)
        audit = json.loads(self._receipt_command(query+";").splitlines()[-1])
        if audit!=result["audit"] or len(audit)!=6:
            raise RuntimeError("Registered versions/dependencies changed or disappeared")
        return result["versions"]

    def run_remote_command(self,command,**kwargs):
        context = getattr(self,"_checkpoint_version_context",None)
        text = kwargs.get("stdin_text")
        if context and text and "Commit atomic version batch" in text:
            prefix = "SELECT json_build_object(\n    'table_name'"
            suffix = ")::text\nFROM inserted_version;"
            if text.count(prefix)!=6 or text.count(suffix)!=6 or text.count("COMMIT;")!=1:
                raise RuntimeError("Organizer version SQL changed; refusing unsafe rewrite")
            text = text.replace(prefix,"INSERT INTO pg_temp.pipeline_registered(result)\nSELECT json_build_object(\n    'table_name'")
            text = text.replace(suffix,")::jsonb\nFROM inserted_version RETURNING result::text;")
            job,signature,database = context
            guard = "SELECT pg_advisory_xact_lock(hashtextextended("+sql_literal(job)+",42));\nDO $g$ BEGIN IF EXISTS(SELECT 1 FROM pipeline_recovery.version_receipts WHERE job_id="+sql_literal(job)+"::uuid) THEN RAISE EXCEPTION 'Version receipt already committed'; END IF; END $g$;"
            text = text.replace("BEGIN;","CREATE TEMP TABLE pipeline_registered(ordinal bigint GENERATED ALWAYS AS IDENTITY,result jsonb) ON COMMIT PRESERVE ROWS;\nBEGIN;\n"+guard,1)
            versions = "(SELECT jsonb_agg(result ORDER BY ordinal) FROM pg_temp.pipeline_registered)"
            audit = "("+audit_query(versions,database)+")"
            journal = "INSERT INTO pipeline_recovery.version_receipts(job_id,request_sha256,result) VALUES("+sql_literal(job)+"::uuid,"+sql_literal(signature)+",jsonb_build_object('versions',"+versions+",'audit',"+audit+"));\n"
            # Test-only injection, empty in normal operation; it runs within the
            # version transaction so the failure must roll back all six rows.
            injected = getattr(self,"_version_failpoint_sql","")
            text = text.replace("COMMIT;",injected+"\n"+journal+"COMMIT;",1)
            kwargs["stdin_text"] = text
        return super().run_remote_command(command,**kwargs)

    def finalize_checkpoint_job(self,runner,database,job_id):
        self._receipt_command(INSTALL)
        with runner.connection_factory(database,autocommit=True) as conn:
            conn.execute("SELECT pg_advisory_lock(%s)",(LOCK_KEY,))
            from pipeline.checkpoints import RELATIONS
            conn.execute("BEGIN")
            conn.execute("LOCK TABLE "+",".join("public."+table for table in RELATIONS)+" IN SHARE ROW EXCLUSIVE MODE")
            row = conn.execute("SELECT request_sha256,request,state,result,source_sha256,schema_signature FROM pipeline_recovery.jobs WHERE job_id=%s",(job_id,)).fetchone()
            if not row or row[2] not in ("data_complete","complete"):
                raise RuntimeError("Data is not complete")
            signature,request,state,result,source_hash,schema = row
            if catalog(conn)!=schema or source_digest(conn,request["scopes"])!=source_hash:
                raise RuntimeError("Source or schema changed before version finalization")
            runner.verify_outputs(conn,result)
            versions = self._read_version_receipt(job_id,signature,database)
            if state=="complete" and versions is not None:
                conn.commit()
                return versions
            if versions is None:
                if state=="complete":
                    raise RuntimeError("Complete data job is missing its control receipt")
                self._checkpoint_version_context = (job_id,signature,database)
                try:
                    versions = super().register_cross_table_expansion_versions(result,request["requested_by"],request["workstation"])
                except Exception:
                    versions = self._read_version_receipt(job_id,signature,database)
                    if versions is None:
                        raise
                finally:
                    self._checkpoint_version_context = None
            runner.failpoint("after_versions_commit",{})
            with conn.transaction():
                result["registered_versions"] = versions
                # Restore compatibility with the organizer's blanket trigger
                # guard only once both durable saga phases have completed.
                for table in RELATIONS:
                    conn.execute(f"DROP TRIGGER IF EXISTS pipeline_write_fence ON public.{table}")
                conn.execute("UPDATE pipeline_recovery.jobs SET state='complete',result=%s,completed_at=clock_timestamp() WHERE job_id=%s",
                    (Jsonb(result),job_id))
                conn.execute("DELETE FROM pipeline_recovery.active WHERE job_id=%s",(job_id,))
                conn.execute("UPDATE pipeline_recovery.jobs SET schema_signature=%s WHERE job_id=%s",(Jsonb(catalog(conn)),job_id))
            conn.commit()
            runner.failpoint("after_complete_commit",{})
            return versions


class ResumablePostgresService(VersionReceiptMixin,PostgresAdminService):
    """Available to a real SSH integration; not enabled in the default GUI."""
