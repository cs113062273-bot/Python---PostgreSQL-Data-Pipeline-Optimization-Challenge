"""Seed initial version lineage for an isolated diagnostic clone, never production."""
import os
from bench.common import connect


def seed_initial_versions(database, template="kaggle_challenge"):
    if not database.startswith("lab_"):
        raise ValueError("Only disposable lab_ clones may use this helper")
    control = os.environ.get("PDM_CONTROL_DB", "postgres_data_manager")
    with connect(control) as conn:
        if conn.execute("SELECT 1 FROM app_control.table_versions WHERE database_name=%s LIMIT 1",(database,)).fetchone():
            return
        conn.execute("""INSERT INTO app_control.table_versions(
            database_name,schema_name,table_name,version_code,version_title,created_by,workstation_name,
            sql_recipe,version_history_log,version_history_format,raw_schema,operation_kind)
            SELECT %s,schema_name,table_name,version_code,version_title,created_by,workstation_name,
                   sql_recipe,version_history_log,version_history_format,raw_schema,operation_kind
            FROM app_control.table_versions WHERE database_name=%s AND version_code='0000'""",(database,template))
