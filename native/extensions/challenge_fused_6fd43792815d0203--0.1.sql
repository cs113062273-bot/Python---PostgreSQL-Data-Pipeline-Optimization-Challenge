CREATE FUNCTION @extschema@.main_load(pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass)
RETURNS pg_catalog.int8[] AS 'MODULE_PATHNAME','challenge_fused_ingest'
LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT SECURITY INVOKER;
CREATE FUNCTION @extschema@.main_identity() RETURNS pg_catalog.text AS 'MODULE_PATHNAME','challenge_fused_identity'
LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER;
