"""Dump/restore a completed guarded run, retaining its database/version identity."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import uuid
from psycopg import sql
from bench.common import ROOT,connect,require_lab,write_report
from pipeline.checkpoints import source_digest,normalize_catalog,physical_fingerprints
from tests.equivalence.oracle import schema_signature,sequence_signature,fingerprints,assert_exact,assert_relationships


def history(conn,database):
    return conn.execute('SELECT to_jsonb(v) FROM app_control.table_versions v WHERE database_name=%s ORDER BY schema_name,table_name,version_code',(database,)).fetchall()


def sequence_state(conn):
    result={}
    for schema,name in conn.execute("SELECT schemaname,sequencename FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename").fetchall():
        result[schema+'.'+name]=conn.execute(sql.SQL('SELECT last_value,is_called FROM {}').format(sql.Identifier(schema,name))).fetchone()
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database')
    parser.add_argument('--pg-dump',required=True)
    parser.add_argument('--pg-restore',required=True)
    parser.add_argument('--output',default='reports/recovery-guarded-dump.json')
    args=parser.parse_args()
    database=args.database or json.loads((ROOT/'reports/recovery-guarded-server-crash.json').read_text(encoding='utf-8'))['database']
    control_database=os.environ.get('PDM_CONTROL_DB','postgres_data_manager')
    with connect(database,autocommit=True) as conn:
        require_lab(conn)
        assert conn.info.host in ('127.0.0.1','localhost','::1')
        assert (ROOT/'.runtime').resolve() in Path(conn.execute('SHOW data_directory').fetchone()[0]).resolve().parents
        before=schema_signature(conn);sequence_before=sequence_signature(conn);sequence_values=sequence_state(conn);footprints=fingerprints(conn)
        physical_before=physical_fingerprints(conn)
        scopes=[r[0] for r in conn.execute('SELECT DISTINCT raw_schema FROM public.raw_data ORDER BY 1')]
        source=source_digest(conn,scopes)
        counters=conn.execute('SELECT to_jsonb(c) FROM public.pgdm_table_row_counts c ORDER BY schema_name,table_name').fetchall()
        encoding,collation,ctype=conn.execute('SELECT pg_encoding_to_char(encoding),datcollate,datctype FROM pg_database WHERE datname=current_database()').fetchone()
        assert_exact(conn);assert_relationships(conn)
    with connect(control_database,autocommit=True) as control:
        versions=history(control,database)
        expanded=[row[0] for row in versions if row[0]['version_code']!='0000']
        assert len(expanded)==6 and all('compiler_sha256=' in row['sql_recipe'] for row in expanded)
    directory=ROOT/'.runtime'/'dumps';directory.mkdir(parents=True,exist_ok=True)
    archive=directory/('guarded_'+uuid.uuid4().hex+'.dump')
    subprocess.run([args.pg_dump,'-Fc','--no-owner','--no-acl','-f',str(archive),database],check=True)
    preserved='lab_dump_archive_'+uuid.uuid4().hex[:10]
    with connect('postgres',autocommit=True) as admin:
        admin.execute(sql.SQL('ALTER DATABASE {} RENAME TO {}').format(sql.Identifier(database),sql.Identifier(preserved)))
        admin.execute(sql.SQL('CREATE DATABASE {} TEMPLATE template0 ENCODING {} LC_COLLATE {} LC_CTYPE {}').format(sql.Identifier(database),sql.Literal(encoding),sql.Literal(collation),sql.Literal(ctype)))
    subprocess.run([args.pg_restore,'--exit-on-error','--no-owner','--no-acl','-d',database,str(archive)],check=True)
    with connect(database,autocommit=True) as conn:
        require_lab(conn)
        assert normalize_catalog(schema_signature(conn))==normalize_catalog(before)
        assert sequence_signature(conn)==sequence_before
        assert sequence_state(conn)==sequence_values
        assert physical_fingerprints(conn)==physical_before
        assert fingerprints(conn)==footprints
        assert source_digest(conn,scopes)==source
        assert conn.execute('SELECT to_jsonb(c) FROM public.pgdm_table_row_counts c ORDER BY schema_name,table_name').fetchall()==counters
        assert_exact(conn);assert_relationships(conn)
    with connect(control_database,autocommit=True) as control:assert history(control,database)==versions
    write_report(args.output,{'kind':'actual_custom_dump_restore_completed_guarded_pipeline','passed':True,
        'database':database,'preserved_original':preserved,'dump_bytes':archive.stat().st_size,
        'exact_six_tables':True,'schema_indexes_validated_constraints':True,'actual_source_sha256_preserved':True,
        'counters_preserved':True,'native_sequence_options_and_ownership_preserved':True,'six_versions_and_compiler_provenance_preserved':True,
        'physical_rows_including_native_identity_values_preserved':True,'sequence_last_value_and_is_called_preserved':True,
        'scope':'same PostgreSQL 17, role and database name; control database retained; GUI snapshot filesystem flow not tested'})
    print('Completed guarded pipeline pg_dump/pg_restore: PASS; all six outputs, constraints, source, counters and versions preserved',flush=True)


if __name__=='__main__':main()
