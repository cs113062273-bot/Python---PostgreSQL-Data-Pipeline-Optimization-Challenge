"""Experimental durable chunk runner for the exact six organizer recipes.

Preflight and managed DML are inherited from the organizer. Logged state and
each chunk's data commit atomically. A persistent write fence protects source
and partial destinations across disconnected sessions. No source copy is made.
Only initial empty destinations are supported. Not enabled in the GUI.
"""
import hashlib
import json
import uuid
from datetime import datetime, timezone

from psycopg.types.json import Jsonb
from pipeline.recipes import ORDER, destinations
from pipeline.upstream import PostgresAdminService

INSTALL = """
CREATE SCHEMA IF NOT EXISTS pipeline_recovery;
CREATE TABLE IF NOT EXISTS pipeline_recovery.jobs (
 job_id uuid PRIMARY KEY, request_sha256 text NOT NULL, request jsonb NOT NULL,
 state text NOT NULL CHECK(state IN ('loading','data_complete','complete')),
 source_sha256 text NOT NULL, schema_signature jsonb NOT NULL,
 last_raw_id bigint NOT NULL DEFAULT 0, max_raw_id bigint NOT NULL,
 chunk_rows integer NOT NULL CHECK(chunk_rows>0), result jsonb,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), completed_at timestamptz
);
CREATE TABLE IF NOT EXISTS pipeline_recovery.active (
 singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
 job_id uuid NOT NULL UNIQUE REFERENCES pipeline_recovery.jobs(job_id),
 backend_pid integer NOT NULL
);
CREATE TABLE IF NOT EXISTS pipeline_recovery.chunks (
 job_id uuid NOT NULL REFERENCES pipeline_recovery.jobs(job_id),
 lo bigint NOT NULL, hi bigint NOT NULL CHECK(hi>lo),
 rows_inserted jsonb NOT NULL, completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(job_id,lo), UNIQUE(job_id,hi)
);
CREATE OR REPLACE FUNCTION pipeline_recovery.write_fence() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $fence$
BEGIN
 IF EXISTS(SELECT 1 FROM pipeline_recovery.active a WHERE
     TG_TABLE_NAME='raw_data' OR a.backend_pid<>pg_backend_pid()
     OR current_setting('pipeline.job_id',true) IS DISTINCT FROM a.job_id::text) THEN
   RAISE EXCEPTION 'Expansion job write fence: resume or investigate active job' USING ERRCODE='55000';
 END IF;
 RETURN NULL;
END $fence$;
"""
LOCK_KEY = 761549992137
RELATIONS = ("raw_data",) + ORDER


def validate_dependencies(service,connection_factory,database,table):
    """Run every organizer dependency guard, excluding only our exact read-only fence.

    Generic user triggers remain blocked. Definitions are rechecked after table
    locks via the persisted catalog signature before any chunk is resumed.
    """
    with connection_factory(database,autocommit=True) as conn:
        triggers = conn.execute("""SELECT t.tgname,t.tgtype,t.tgenabled,n.nspname,p.proname,
            p.prosrc,p.prosecdef,p.proconfig,octet_length(t.tgargs)
            FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE t.tgrelid=%s::regclass AND NOT t.tgisinternal""",(table,)).fetchall()
    own = [t for t in triggers if t[0]=="pipeline_write_fence"]
    if not own:
        return service._validate_managed_sql_dependencies(database,table,{"insert"})
    expected_source = INSTALL.split("$fence$")[1]
    if own!=[("pipeline_write_fence",62,"O","pipeline_recovery","write_fence",expected_source,False,["search_path=pg_catalog"],0)]:
        raise RuntimeError("Unrecognized pipeline write fence; refusing exemption")
    class Proxy:
        def __getattr__(self,name):
            return getattr(service,name)
        def run_remote_command(self,command,**kwargs):
            output=service.run_remote_command(command,**kwargs)
            metadata=json.loads(output.strip().splitlines()[-1])
            if "relation_kind" not in metadata or "triggers" not in metadata:
                raise RuntimeError("Organizer dependency metadata shape changed")
            metadata["triggers"]=[t for t in metadata["triggers"] if t!="pipeline_write_fence"]
            return json.dumps(metadata)
    return PostgresAdminService._validate_managed_sql_dependencies(Proxy(),database,table,{"insert"})


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False,default=str).encode()).hexdigest()


def source_digest(conn, scopes):
    # Ordered server-side SHA-256 per actual row, reduced in bounded 10k batches.
    # Binds payload AND metadata. Stored raw_hash alone is never trusted.
    result = hashlib.sha256()
    last = None
    while True:
        rows = conn.execute("""SELECT raw_id, encode(sha256(convert_to(to_jsonb(t)::text,'UTF8')),'hex')
            FROM public.raw_data t WHERE raw_schema=ANY(%s) AND (%s::bigint IS NULL OR raw_id>%s)
            ORDER BY raw_id LIMIT 10000""",(scopes,last,last)).fetchall()
        if not rows:
            break
        for raw_id, checksum in rows:
            result.update(f"{raw_id}:{checksum}\n".encode("ascii"))
        last = rows[-1][0]
    return result.hexdigest()


def normalize_catalog(value):
    """Canonicalize the one known pg_dump CHECK cast representation change.

    ARRAY[varchar constants]::text[] and ARRAY[each constant::text] express the
    same organizer IN check. Do not normalize arbitrary SQL or other checks.
    """
    value=json.loads(json.dumps(value,default=str))
    equivalent={
        "CHECK (((text_column_1 IS NULL) OR ((text_column_1)::text = ANY ((ARRAY['X'::character varying, 'Y'::character varying, 'Z'::character varying])::text[]))))",
        "CHECK (((text_column_1 IS NULL) OR ((text_column_1)::text = ANY (ARRAY[('X'::character varying)::text, ('Y'::character varying)::text, ('Z'::character varying)::text]))))",
    }
    for constraint in value.get("constraints",[]):
        if constraint[0:2]==["table2","table2_text_column_1_check"] and constraint[2] in equivalent:
            constraint[2]="organizer_table2_type_check_nullable_X_Y_Z_v1"
    return value


def catalog(conn):
    from tests.equivalence.oracle import schema_signature
    value = schema_signature(conn)
    value["server_version_num"] = conn.execute("SHOW server_version_num").fetchone()[0]
    value["fences"] = conn.execute("""SELECT c.relname,pg_get_triggerdef(t.oid),t.tgenabled,p.prosrc
        FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_proc p ON p.oid=t.tgfoid
        WHERE t.tgname='pipeline_write_fence' ORDER BY c.relname""").fetchall()
    return normalize_catalog(value)


def physical_fingerprints(conn):
    """Recovery compares the exact committed rows, including their stored IDs.

    It needs neither cross-run surrogate normalization nor JSON construction.
    Native composite hashing is bounded-memory and keeps dump-restored IDs.
    """
    return {table:conn.execute(f"""SELECT count(*),
        coalesce(sum(hash_record_extended(t,17)::numeric),0)::text,
        coalesce(sum(hash_record_extended(t,97)::numeric),0)::text
        FROM public.{table} t""").fetchone() for table in ORDER}


class ChunkRunner:
    def __init__(self, service, connection_factory, failpoint=None):
        self.service = service
        self.connection_factory = connection_factory
        self.failpoint = failpoint or (lambda event, detail: None)

    def preflight(self, database, scopes, mode):
        if mode not in ("baseline", "payload_once", "fields_once", "path_once"):
            raise ValueError("Recovery prototype accepts conservative, immutable-function recipes only")
        service = self.service
        recipes = service._prepare_cross_table_expansion("public.raw_data",scopes,destinations(mode))
        if not service.table_exists(database,"public.raw_data"):
            raise RuntimeError("Missing source")
        columns = {c["name"] for c in service.get_table_column_definitions(database,"public.raw_data")}
        if not {"raw_schema","raw_hash","raw_id","raw"} <= columns:
            raise RuntimeError("Missing source columns")
        versions = service.get_raw_schema_version_references(database,"public.raw_data",scopes)
        if {v["raw_schema"] for v in versions} != set(scopes):
            raise RuntimeError("Missing source version references")
        service._validate_cross_table_expansion_functions(database,recipes["destinations"])
        service.ensure_data_database_metadata(database)
        for d in recipes["destinations"]:
            if not service.table_exists(database,d["table_name"]):
                raise RuntimeError("Missing destination " + d["table_name"])
            service.get_table_row_count(database,d["table_name"])
            validate_dependencies(service,self.connection_factory,database,d["table_name"])
        return recipes, versions

    def run(self, database, scopes, job_id, chunk_rows=100000, mode="baseline", requested_by="research", workstation="research"):
        job_id = str(uuid.UUID(job_id))
        if not isinstance(chunk_rows,int) or chunk_rows<1:
            raise ValueError("chunk_rows must be positive")
        scopes = sorted(set(scopes))
        prepared, versions = self.preflight(database,scopes,mode)
        request = {"database":database,"scopes":scopes,"destinations":prepared["destinations"],
                   "source_versions":versions,"chunk_rows":chunk_rows,"requested_by":requested_by,"workstation":workstation,
                   "protocol":2}
        signature = digest(request)
        self.failpoint("before_start",{})
        with self.connection_factory(database,autocommit=True) as conn:
            conn.execute("SELECT pg_advisory_lock(%s)",(LOCK_KEY,))
            if not conn.execute("SELECT to_regclass('pipeline_recovery.jobs')").fetchone()[0]:
                conn.execute(INSTALL)
            previous=conn.execute("SELECT request->>'protocol' FROM pipeline_recovery.jobs WHERE job_id=%s",(job_id,)).fetchone()
            if previous:
                protocol=int(previous[0])
                if protocol not in (1,2):raise RuntimeError("Unsupported recovery protocol")
                request["protocol"]=protocol
                signature=digest(request)
            # Durability is mandatory, including the receipt commits.
            if any(conn.execute("SHOW " + setting).fetchone()[0] != "on" for setting in ("fsync","full_page_writes","synchronous_commit")):
                raise RuntimeError("Durable PostgreSQL configuration required")
            with conn.transaction():
                conn.execute("LOCK TABLE " + ",".join("public."+t for t in RELATIONS) + " IN SHARE ROW EXCLUSIVE MODE")
                existing = conn.execute("SELECT request_sha256,state,result FROM pipeline_recovery.jobs WHERE job_id=%s FOR UPDATE",(job_id,)).fetchone()
                if existing:
                    if existing[0] != signature:
                        raise RuntimeError("Job UUID reused with different request/recipes/source versions")
                    job = conn.execute("SELECT source_sha256,schema_signature FROM pipeline_recovery.jobs WHERE job_id=%s",(job_id,)).fetchone()
                    if catalog(conn)!=normalize_catalog(job[1]) or source_digest(conn,scopes)!=job[0]:
                        raise RuntimeError("Source content, schema or write fence changed")
                    if existing[1] == "complete":
                        self.verify_outputs(conn,existing[2])
                        return existing[2]
                    active = conn.execute("SELECT job_id FROM pipeline_recovery.active").fetchone()
                    if not active or str(active[0]) != job_id:
                        raise RuntimeError("Missing or mismatched active job fence")
                    conn.execute("UPDATE pipeline_recovery.active SET backend_pid=pg_backend_pid() WHERE job_id=%s",(job_id,))
                else:
                    if conn.execute("SELECT 1 FROM pipeline_recovery.active").fetchone():
                        raise RuntimeError("Another job owns the database")
                    for table in ORDER:
                        if conn.execute(f"SELECT EXISTS(SELECT 1 FROM public.{table})").fetchone()[0]:
                            raise RuntimeError("Chunk prototype requires empty destinations")
                    # Statement fences include TRUNCATE and persist across crashes.
                    for table in RELATIONS:
                        conn.execute(f"CREATE OR REPLACE TRIGGER pipeline_write_fence BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.{table} FOR EACH STATEMENT EXECUTE FUNCTION pipeline_recovery.write_fence()")
                    max_id = conn.execute("SELECT max(raw_id) FROM public.raw_data WHERE raw_schema=ANY(%s)",(scopes,)).fetchone()[0]
                    if max_id is None:
                        raise RuntimeError("Empty source scope")
                    if conn.execute("SELECT min(raw_id) FROM public.raw_data WHERE raw_schema=ANY(%s)",(scopes,)).fetchone()[0]<1:
                        raise RuntimeError("Experimental chunk runner requires positive raw_id values")
                    conn.execute("INSERT INTO pipeline_recovery.jobs(job_id,request_sha256,request,state,source_sha256,schema_signature,max_raw_id,chunk_rows) VALUES(%s,%s,%s,'loading',%s,%s,%s,%s)",
                        (job_id,signature,Jsonb(request),source_digest(conn,scopes),Jsonb(catalog(conn)),max_id,chunk_rows))
                    conn.execute("INSERT INTO pipeline_recovery.active(job_id,backend_pid) VALUES(%s,pg_backend_pid())",(job_id,))
                    conn.execute("SELECT set_config('pipeline.job_id',%s,false)",(job_id,))
                    for d in prepared["destinations"][:2]:
                        self._insert(conn,d,None,None)
                        self.failpoint("during_"+d["pure_table_name"],{"initial_dimensions":True,"backend_pid":conn.info.backend_pid})
                    # A committed tiny dimension with unknown statistics can
                    # otherwise force wide external sorts in every later chunk.
                    conn.execute("ANALYZE public.table3; ANALYZE public.table4")
                conn.execute("SELECT set_config('pipeline.job_id',%s,false)",(job_id,))
            self.failpoint("after_dimensions_commit",{})
            while True:
                with conn.transaction():
                    job = conn.execute("SELECT last_raw_id,max_raw_id,state,result FROM pipeline_recovery.jobs WHERE job_id=%s FOR UPDATE",(job_id,)).fetchone()
                    if job[2] == "data_complete":
                        self.verify_outputs(conn,job[3])
                        return job[3]
                    lo = job[0]
                    ids = conn.execute("SELECT raw_id FROM public.raw_data WHERE raw_schema=ANY(%s) AND raw_id>%s ORDER BY raw_id LIMIT %s",(scopes,lo,chunk_rows)).fetchall()
                    if not ids:
                        result = self._finish_data(conn,database,scopes,versions,prepared,job_id)
                        conn.execute("UPDATE pipeline_recovery.jobs SET state='data_complete',result=%s WHERE job_id=%s",(Jsonb(result),job_id))
                    else:
                        hi = ids[-1][0]
                        counts = {}
                        for d in prepared["destinations"][2:]:
                            counts[d["table_name"]] = self._insert(conn,d,lo,hi)
                            self.failpoint("during_"+d["pure_table_name"],{"lo":lo,"hi":hi,"backend_pid":conn.info.backend_pid})
                        conn.execute("INSERT INTO pipeline_recovery.chunks(job_id,lo,hi,rows_inserted) VALUES(%s,%s,%s,%s)",(job_id,lo,hi,Jsonb(counts)))
                        conn.execute("UPDATE pipeline_recovery.jobs SET last_raw_id=%s WHERE job_id=%s",(hi,job_id))
                        self.failpoint("before_chunk_commit",{"lo":lo,"hi":hi,"backend_pid":conn.info.backend_pid})
                if not ids:
                    self.failpoint("after_data_commit",{})
                    return result
                self.failpoint("after_chunk_commit",{"lo":lo,"hi":hi})

    def _insert(self,conn,d,lo,hi):
        before = conn.execute("SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s",(d["pure_table_name"],)).fetchone()[0]
        for statement in d["statements"]:
            if lo is not None:
                marker = 'WHERE "raw_schema" IN ('
                if statement.count(marker)!=1:
                    raise RuntimeError("Protected source scope structure changed")
                statement = statement.replace(marker,f'WHERE "raw_id" > {int(lo)} AND "raw_id" <= {int(hi)} AND "raw_schema" IN (',1)
                join_key="ON main_row.id_column_3 = source_row.raw_id"
                if join_key in statement:
                    # The source range and equality imply this predicate. Make
                    # it explicit so later chunks need not scan an ever-growing
                    # complete table1 for each dependent destination.
                    if statement.count(join_key)!=1:
                        raise RuntimeError("Unrecognized main-row relationship")
                    statement=statement.replace(join_key,join_key+f"\n AND main_row.id_column_3 > {int(lo)} AND main_row.id_column_3 <= {int(hi)}",1)
            # Upstream counter effects and row insertion remain in this chunk tx.
            conn.execute(self.service._build_managed_dml_sql(statement,"public",d["pure_table_name"],"add"))
        after = conn.execute("SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s",(d["pure_table_name"],)).fetchone()[0]
        return after-before

    @staticmethod
    def verify_outputs(conn,result):
        from tests.equivalence.oracle import fingerprints
        algorithm=result.get("destination_fingerprint_algorithm","canonical_graph_v1")
        if algorithm not in ("canonical_graph_v1","physical_record_v2"):
            raise RuntimeError("Unknown destination fingerprint algorithm")
        current=physical_fingerprints(conn) if algorithm=="physical_record_v2" else fingerprints(conn)
        actual = json.loads(json.dumps(current,default=str))
        expected = json.loads(json.dumps(result["destination_fingerprints"],default=str))
        if actual != expected:
            raise RuntimeError("Committed destination counts/content changed")
        ChunkRunner.verify_counters(conn,actual)

    @staticmethod
    def verify_counters(conn,footprints):
        for table,footprint in footprints.items():
            counter=conn.execute("SELECT row_count FROM public.pgdm_table_row_counts WHERE schema_name='public' AND table_name=%s",(table,)).fetchone()
            if not counter or counter[0]!=footprint[0]:
                raise RuntimeError("Managed row counter differs from actual output: "+table)

    def _finish_data(self,conn,database,scopes,versions,prepared,job_id):
        from tests.equivalence.oracle import fingerprints, assert_relationships
        assert_relationships(conn)
        identity = conn.execute("SELECT source_sha256,schema_signature FROM pipeline_recovery.jobs WHERE job_id=%s",(job_id,)).fetchone()
        if catalog(conn)!=normalize_catalog(identity[1]) or source_digest(conn,scopes)!=identity[0]:
            raise RuntimeError("Source content/schema changed before completion")
        manifest_sql = self.service._build_expansion_manifest_sql("public","raw_data",scopes)
        # Execute multi-statements and retain the final JSON manifest.
        cursor = conn.execute(manifest_sql)
        manifest = None
        while True:
            if cursor.description:
                manifest = json.loads(cursor.fetchone()[0])
            if not cursor.nextset():
                break
        protocol=int(conn.execute("SELECT request->>'protocol' FROM pipeline_recovery.jobs WHERE job_id=%s",(job_id,)).fetchone()[0])
        footprints=physical_fingerprints(conn) if protocol==2 else fingerprints(conn)
        self.verify_counters(conn,footprints)
        return {"source_database_name":database,"source_schema_name":"public","source_table_name":"raw_data",
                "raw_schemas":scopes,"source_versions":versions,"dependency_manifest":manifest,
                "destinations":prepared["destinations"],"checkpoint_job_id":job_id,
                "destination_fingerprints":footprints,"destination_fingerprint_algorithm":"physical_record_v2" if protocol==2 else "canonical_graph_v1","completed_at":datetime.now(timezone.utc).isoformat()}
