/* Isolated typed-COPY source research. The PG 17.11 COPY engine below is
 * unmodified except for renaming symbols and substituting NextCopyFrom.
 * It retains native defaults, CHECK/NOT NULL, indexes, triggers and WAL.
 * Never installed into the existing trusted pure-normalizer package.
 */
#include "postgres.h"
#include "fmgr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_relation.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"

/* Rename only local engine symbols; never interpose on the server binary. */
#define CopyFrom AxisEngineCopyFrom
#define BeginCopyFrom AxisEngineBeginCopyFrom
#define EndCopyFrom AxisEngineEndCopyFrom
#define CopyFromErrorCallback AxisEngineErrorCallback
#define CopyLimitPrintoutLength AxisEnginePrintoutLength
#define NextCopyFrom AxisEngineNextTuple
#include "commands/copy.h"
#include "commands/copyfrom_internal.h"
#include "../../vendor/postgresql-17.11/copyfrom.c"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(challenge_axis_ingest);

typedef struct AxisParent { int64 raw_id; int64 parent_id; } AxisParent;
typedef struct AxisSource
{
    Relation cache;
    TableScanDesc scan;
    TupleTableSlot *slot;
    HTAB *parents;
    AttrNumber raw_att, proof_att, axes_att;
    unsigned char frame[140];
    int rows, next;
    int64 parent_id;
    int header_offset;
} AxisSource;

static AxisSource *active_source = NULL;

static AttrNumber
required_attribute(Relation rel, const char *name, Oid type)
{
    AttrNumber number = get_attnum(RelationGetRelid(rel),name);
    Form_pg_attribute attr;
    if (number <= 0) ereport(ERROR,(errmsg("Typed axis source column is missing: %s",name)));
    attr = TupleDescAttr(RelationGetDescr(rel),number-1);
    if (attr->attisdropped || attr->atttypid != type || attr->atttypmod != -1 || attr->attgenerated)
        ereport(ERROR,(errmsg("Typed axis source column has unsupported metadata: %s",name)));
    return number;
}

static void
check_relation(Relation rel, AclMode permission)
{
    AclResult result;
    if (rel->rd_rel->relkind != RELKIND_RELATION || rel->rd_rel->relispartition ||
        rel->rd_rel->relrowsecurity || has_subclass(RelationGetRelid(rel)))
        ereport(ERROR,(errmsg("Typed axis loader requires plain non-RLS tables")));
    result = pg_class_aclcheck(RelationGetRelid(rel),GetUserId(),permission);
    if (result != ACLCHECK_OK) aclcheck_error(result,OBJECT_TABLE,RelationGetRelationName(rel));
}

static uint32
frame_number(const unsigned char *p)
{
    return ((uint32)p[0]<<24) | ((uint32)p[1]<<16) | ((uint32)p[2]<<8) | p[3];
}

/* The ordinary BeginCopyFrom reads only this binary header. Subsequent rows
 * are typed Datums from AxisEngineNextTuple, so no encoded input is consumed.
 */
static int
empty_binary_header(void *buffer, int minread, int maxread)
{
    static const unsigned char header[19]={'P','G','C','O','P','Y','\n',255,'\r','\n',0,0,0,0,0,0,0,0,0};
    int amount;
    if (active_source == NULL) elog(ERROR,"Typed COPY source is not active");
    amount = Min(maxread,19-active_source->header_offset);
    if (amount > 0) memcpy(buffer,header+active_source->header_offset,amount);
    active_source->header_offset += amount;
    return amount;
}

bool
AxisEngineNextTuple(CopyFromState cstate, ExprContext *econtext, Datum *values, bool *nulls)
{
    AxisSource *source = active_source;
    const unsigned char *row;
    int i;
    if (source == NULL) elog(ERROR,"Typed COPY source is not active");
    while (source->next == source->rows)
    {
        Datum raw, safe, packed;
        bool isnull;
        int64 raw_id;
        AxisParent *parent;
        bytea *frame;
        int length;
        CHECK_FOR_INTERRUPTS();
        if (!table_scan_getnextslot(source->scan,ForwardScanDirection,source->slot)) return false;
        raw = slot_getattr(source->slot,source->raw_att,&isnull);
        if (isnull) ereport(ERROR,(errmsg("Typed axis cache raw_id is NULL")));
        raw_id = DatumGetInt64(raw);
        safe = slot_getattr(source->slot,source->proof_att,&isnull);
        if (isnull || !DatumGetBool(safe)) ereport(ERROR,(errmsg("Typed axis cache has an unproven row")));
        parent = hash_search(source->parents,&raw_id,HASH_FIND,NULL);
        if (parent == NULL) continue; /* exactly the original inner join */
        packed = slot_getattr(source->slot,source->axes_att,&isnull);
        if (isnull) ereport(ERROR,(errmsg("Proven axis frame is NULL")));
        frame = DatumGetByteaPP(packed); length = VARSIZE_ANY_EXHDR(frame);
        if (length > 140 || length % 10 != 0) ereport(ERROR,(errmsg("Native axis frame length is invalid")));
        memcpy(source->frame,VARDATA_ANY(frame),length);
        for (i=0;i<length/10;i++)
        {
            const unsigned char *p=source->frame+10*i;
            if (p[0]<1 || p[0]>14 || (p[1]!='X' && p[1]!='Y' && p[1]!='Z') ||
                frame_number(p+2)>999999999 || frame_number(p+6)>999999999 ||
                (i>0 && p[0]<=source->frame[10*(i-1)]))
                ereport(ERROR,(errmsg("Native axis frame payload is invalid")));
        }
        source->rows=length/10;source->next=0;source->parent_id=parent->parent_id;
        if (source->rows==0) continue;
    }
    row=source->frame+10*source->next++;
    memset(values,0,RelationGetDescr(cstate->rel)->natts*sizeof(Datum));
    memset(nulls,true,RelationGetDescr(cstate->rel)->natts*sizeof(bool));
    /* Exact canonical table2 ordinal layout, checked before opening engine. */
    values[1]=Int64GetDatum(source->parent_id);nulls[1]=false;
    values[2]=Int16GetDatum(row[0]);nulls[2]=false;
    values[3]=Int32GetDatum(frame_number(row+2));nulls[3]=false;
    values[4]=PointerGetDatum(cstring_to_text_with_len((const char*)row+1,1));nulls[4]=false;
    if (frame_number(row+6)>0) { values[8]=Int32GetDatum(frame_number(row+6));nulls[8]=false; }
    for (i=0;i<cstate->num_defaults;i++)
        values[cstate->defmap[i]]=ExecEvalExpr(cstate->defexprs[cstate->defmap[i]],econtext,&nulls[cstate->defmap[i]]);
    cstate->cur_lineno++; cstate->line_buf.len=40;
    return true;
}

Datum
challenge_axis_ingest(PG_FUNCTION_ARGS)
{
    Oid cache_oid=PG_GETARG_OID(0), parent_oid=PG_GETARG_OID(1), target_oid=PG_GETARG_OID(2);
    AxisSource source;
    Relation parent,target;
    TableScanDesc scan;
    TupleTableSlot *slot;
    AttrNumber raw_att,id_att;
    HASHCTL ctl;
    uint64 count=0,limit=(uint64)work_mem*1024/96, processed=0;
    CopyFromState copy;
    ParseState *pstate;
    ParseNamespaceItem *nsitem;
    List *columns=NIL,*options;
    ListCell *lc;
    int i;
    static const char *names[]={"id_column_1","id_column_2","integer_column_1","integer_column_2","text_column_1","integer_column_3","integer_column_4","integer_column_5","integer_column_6"};
    static const Oid types[]={INT8OID,INT8OID,INT2OID,INT4OID,VARCHAROID,INT4OID,INT4OID,INT4OID,INT4OID};
    if (!superuser()) ereport(ERROR,(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),errmsg("Unqualified typed COPY research requires an isolated superuser")));
    if (XactReadOnly || IsInParallelMode()) ereport(ERROR,(errmsg("Typed COPY cannot execute in a read-only or parallel transaction")));
    if (active_source != NULL || cache_oid==parent_oid || parent_oid==target_oid || cache_oid==target_oid)
        ereport(ERROR,(errmsg("Nested or aliased typed COPY is unsupported")));
    memset(&source,0,sizeof(source));memset(&ctl,0,sizeof(ctl));
    source.cache=table_open(cache_oid,AccessShareLock);parent=table_open(parent_oid,AccessShareLock);target=table_open(target_oid,RowExclusiveLock);
    check_relation(source.cache,ACL_SELECT);check_relation(parent,ACL_SELECT);check_relation(target,ACL_INSERT);
    if (RelationGetDescr(target)->natts!=9) ereport(ERROR,(errmsg("Unsupported typed axis destination layout")));
    for (i=0;i<9;i++) if (required_attribute(target,names[i],types[i])!=i+1) ereport(ERROR,(errmsg("Unsupported typed axis column order")));
    if (!TupleDescAttr(RelationGetDescr(target),0)->attidentity) ereport(ERROR,(errmsg("Typed axis destination requires a native identity")));
    source.raw_att=required_attribute(source.cache,"raw_id",INT8OID);
    source.proof_att=required_attribute(source.cache,"pipeline_domain_safe",BOOLOID);
    source.axes_att=required_attribute(source.cache,"pipeline_axes",BYTEAOID);
    raw_att=required_attribute(parent,"id_column_3",INT8OID);id_att=required_attribute(parent,"id_column_1",INT8OID);
    ctl.keysize=sizeof(int64);ctl.entrysize=sizeof(AxisParent);ctl.hcxt=CurrentMemoryContext;
    source.parents=hash_create("Typed COPY actual native parent IDs",Min(limit,1024),&ctl,HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
    scan=table_beginscan(parent,GetActiveSnapshot(),0,NULL);slot=table_slot_create(parent,NULL);
    while (table_scan_getnextslot(scan,ForwardScanDirection,slot))
    {
        bool null_id,null_raw,found;
        Datum raw=slot_getattr(slot,raw_att,&null_raw),id=slot_getattr(slot,id_att,&null_id);
        int64 key;
        AxisParent *entry;
        CHECK_FOR_INTERRUPTS();
        if (null_raw) continue;
        if (null_id) ereport(ERROR,(errmsg("Native parent identity is NULL")));
        if (++count>limit) ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("Typed COPY parent hash exceeds its work_mem bound")));
        key=DatumGetInt64(raw);entry=hash_search(source.parents,&key,HASH_ENTER,&found);
        if (found) ereport(ERROR,(errmsg("Typed COPY requires unique parent raw_id values")));
        entry->parent_id=DatumGetInt64(id);
    }
    ExecDropSingleTupleTableSlot(slot);table_endscan(scan);table_close(parent,NoLock);
    source.scan=table_beginscan(source.cache,GetActiveSnapshot(),0,NULL);source.slot=table_slot_create(source.cache,NULL);
    pstate=make_parsestate(NULL);nsitem=addRangeTableEntryForRelation(pstate,target,RowExclusiveLock,NULL,false,false);
    nsitem->p_perminfo->requiredPerms=ACL_INSERT;
    for(i=1;i<9;i++) columns=lappend(columns,makeString(pstrdup(names[i])));
    foreach(lc,CopyGetAttnums(RelationGetDescr(target),target,columns))
        nsitem->p_perminfo->insertedCols=bms_add_member(nsitem->p_perminfo->insertedCols,lfirst_int(lc)-FirstLowInvalidHeapAttributeNumber);
    ExecCheckPermissions(pstate->p_rtable,pstate->p_rteperminfos,true);
    options=list_make1(makeDefElem("format",(Node*)makeString("binary"),-1));
    active_source=&source;
    PG_TRY();
    {
        copy=AxisEngineBeginCopyFrom(pstate,target,NULL,NULL,false,empty_binary_header,columns,options);
        processed=AxisEngineCopyFrom(copy);
        AxisEngineEndCopyFrom(copy);
        active_source=NULL;
    }
    PG_CATCH(); { active_source=NULL;PG_RE_THROW(); } PG_END_TRY();
    ExecDropSingleTupleTableSlot(source.slot);table_endscan(source.scan);hash_destroy(source.parents);
    table_close(source.cache,NoLock);table_close(target,NoLock);free_parsestate(pstate);
    PG_RETURN_INT64(processed);
}
