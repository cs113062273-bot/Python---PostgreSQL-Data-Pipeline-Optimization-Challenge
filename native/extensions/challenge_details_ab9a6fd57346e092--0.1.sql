CREATE FUNCTION @extschema@.details_load(pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass)
      RETURNS pg_catalog.int8[] AS 'MODULE_PATHNAME','challenge_details_ingest'
      LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT SECURITY INVOKER;
CREATE FUNCTION @extschema@.details_identity() RETURNS pg_catalog.text
      AS 'MODULE_PATHNAME','challenge_details_identity' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER;
