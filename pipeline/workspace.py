"""Logged shared work relation with a transaction-owned namespace.

An existing namespace or missing CREATE privilege selects the exact original
fallback and is never overwritten. The namespace is dropped before COMMIT.
"""
from pipeline.parallel_normalization import prepare_sql as parallel_sql

SCHEMA='pgdm_pipeline_work'
RELATION=SCHEMA+'.pipeline_normalized_input'


def prepare_sql(scopes):
    body=parallel_sql(scopes,logged=True)
    namespace='CREATE SCHEMA IF NOT EXISTS challenge_lab;\n'
    if body.count(namespace)!=1 or body.count('challenge_lab.pipeline_normalized_input')!=2:
        raise RuntimeError('Unknown logged projection boundaries')
    body=body.replace(namespace,'',1).replace('challenge_lab.pipeline_normalized_input',RELATION)
    return """CREATE TEMP TABLE pipeline_work_ownership(owned boolean NOT NULL) ON COMMIT DROP;
DO $work_namespace$ BEGIN
  IF pg_catalog.to_regnamespace('pgdm_pipeline_work') IS NULL
     AND pg_catalog.has_database_privilege(current_user,current_database(),'CREATE') THEN
    CREATE SCHEMA pgdm_pipeline_work;
    INSERT INTO pg_temp.pipeline_work_ownership VALUES(true);
    """+body+"""
  ELSE
    INSERT INTO pg_temp.pipeline_work_ownership VALUES(false);
    CREATE TEMP TABLE pipeline_normalization_domain ON COMMIT DROP AS SELECT false AS safe;
  END IF;
END $work_namespace$;"""


def cleanup_sql():
    # RESTRICT: unexpected objects never cascade into unrelated data.
    return """DO $work_cleanup$ BEGIN
      IF (SELECT owned FROM pg_temp.pipeline_work_ownership) THEN
        DROP TABLE pgdm_pipeline_work.pipeline_normalized_input RESTRICT;
        DROP SCHEMA pgdm_pipeline_work RESTRICT;
      END IF;
    END $work_cleanup$;"""
