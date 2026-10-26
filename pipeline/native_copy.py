"""Native psql COPY with real command-tag receipts for managed counters.

Binary files are client-side in a private per-batch working directory. Original
constraints, identity defaults and atomic transaction are retained. No server
file privilege, PROGRAM, UDF, manual identity or COPY FREEZE is used.
"""
import re
from pipeline.parallel_normalization import settings_sql,restore_settings_sql

MARKER='-- pipeline native psql copy v1'


def prepare_receipts_sql():
    return """CREATE TEMP TABLE pipeline_copy_receipts(table_name text PRIMARY KEY,affected_rows bigint NOT NULL CHECK(affected_rows>=0)) ON COMMIT DROP;
SELECT safe AS pipeline_copy_safe FROM pg_temp.pipeline_normalization_domain
\\gset
"""


def single_line_sql(query):
    """Normalize only whitespace outside SQL literals/quoted identifiers.

    The COPY SELECT is bound to the canonical recipes over the work relation;
    scope values stay in the separate ordinary SQL projection, never this line.
    Refuse embedded literal newlines rather than silently changing their value.
    """
    result=[];quote=None;i=0
    while i<len(query):
        ch=query[i]
        if quote:
            if ch in '\r\n':raise RuntimeError('Canonical COPY literal contains a newline')
            result.append(ch)
            if ch==quote:
                if i+1<len(query) and query[i+1]==quote:
                    result.append(query[i+1]);i+=1
                else:quote=None
        elif ch in ("'",'"'):
            quote=ch;result.append(ch)
        elif ch.isspace():
            if result and result[-1]!=' ':result.append(' ')
        else:result.append(ch)
        i+=1
    if quote:raise RuntimeError('Unclosed canonical COPY literal')
    return ''.join(result).strip()


def copy_sql(statement,table):
    m=re.fullmatch(r'INSERT INTO public\.'+re.escape(table)+r'\s*\((.*?)\)\s*(SELECT\s.*?);?',statement,re.S)
    if not m:raise RuntimeError('Unknown protected COPY source')
    columns=','.join(x.strip() for x in m[1].split(','))
    query=m[2].rstrip(';')
    tune='SET LOCAL enable_parallel_hash=off;\n' if table=='table1' else ''
    # \copy opens binary files correctly on Windows as well as Unix. Generic
    # \g redirection can use text-mode output and corrupt the binary signature.
    return MARKER+"\n\\if :pipeline_copy_safe\n"+settings_sql()+'\n'+tune+"\\copy ("+single_line_sql(query)+") TO '"+table+".bin' (FORMAT BINARY)\n"+f"\\copy public.{table} ({columns}) FROM '{table}.bin' (FORMAT BINARY)\n"+f"INSERT INTO pg_temp.pipeline_copy_receipts VALUES('{table}',:'ROW_COUNT'::bigint);\n"+restore_settings_sql()+"\n\\endif\n"


def managed_receipt_sql(managed,statement,table):
    declaration='pgdm_affected_rows bigint;'
    diagnostics='GET DIAGNOSTICS pgdm_affected_rows = ROW_COUNT;'
    if managed.count(statement)!=1 or managed.count(declaration)!=1 or managed.count(diagnostics)!=1:
        raise RuntimeError('Unknown native managed counter boundary')
    # The declaration caches the decision before any fallback INSERT. The
    # fallback GET DIAGNOSTICS immediately follows its actual DML, preserving
    # ROW_COUNT; COPY uses the server's command tag captured by psql.
    replacement=f"""IF pipeline_copy_fast THEN
      SELECT affected_rows INTO STRICT pgdm_affected_rows FROM pg_temp.pipeline_copy_receipts WHERE table_name='{table}';
    ELSE
      {statement};
      {diagnostics}
    END IF"""
    result=managed.replace(diagnostics,'',1)
    result=result.replace(declaration,declaration+'\n    pipeline_copy_fast boolean := (SELECT safe FROM pg_temp.pipeline_normalization_domain);',1)
    return result.replace(statement,replacement,1)
