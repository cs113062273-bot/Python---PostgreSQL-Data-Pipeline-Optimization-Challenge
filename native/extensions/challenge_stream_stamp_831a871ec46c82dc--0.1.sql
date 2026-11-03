CREATE FUNCTION @extschema@.normalize(pg_catalog.jsonb) RETURNS pg_catalog.record
 AS 'MODULE_PATHNAME','challenge_compact13' LANGUAGE C STABLE PARALLEL SAFE SECURITY INVOKER CALLED ON NULL INPUT;
CREATE FUNCTION @extschema@.axes(pg_catalog.bytea) RETURNS TABLE(position_number pg_catalog.int2,weight pg_catalog.int4,axis_kind pg_catalog.text,gap pg_catalog.int4)
 AS 'MODULE_PATHNAME','challenge_axis_rows' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER ROWS 14;
CREATE FUNCTION @extschema@.identity() RETURNS pg_catalog.text
 AS 'MODULE_PATHNAME','challenge_probe_identity' LANGUAGE C IMMUTABLE PARALLEL SAFE SECURITY INVOKER;
