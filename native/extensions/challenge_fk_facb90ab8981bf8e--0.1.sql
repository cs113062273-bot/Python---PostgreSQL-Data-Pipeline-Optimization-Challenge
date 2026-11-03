CREATE FUNCTION @extschema@.fk_validate(pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass,pg_catalog.regclass)
      RETURNS pg_catalog.int8[] AS 'MODULE_PATHNAME','challenge_fk_validate'
      LANGUAGE C VOLATILE PARALLEL UNSAFE STRICT SECURITY INVOKER;
CREATE FUNCTION @extschema@.fk_identity() RETURNS pg_catalog.text
      AS 'MODULE_PATHNAME','challenge_fk_identity' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER;
