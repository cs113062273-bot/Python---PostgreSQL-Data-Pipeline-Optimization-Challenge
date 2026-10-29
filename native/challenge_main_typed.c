/* Typed native table1 COPY over pre-normalized native timestamps.
 * Actual dimension IDs are read once; the target keeps PostgreSQL identity,
 * CHECK/NOT NULL, index, trigger and WAL machinery. No global result cache.
 * The vendored REL_17_11 engine is renamed locally, never server-interposed.
 */
#include "postgres.h"
#include "fmgr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_collation_d.h"
#include "catalog/pg_inherits.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "parser/parse_relation.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "mb/pg_wchar.h"
#include "utils/memutils.h"

#define CopyFrom MainCopyFrom
#define BeginCopyFrom MainBeginCopyFrom
#define EndCopyFrom MainEndCopyFrom
#define CopyFromErrorCallback MainErrorCallback
#define CopyLimitPrintoutLength MainPrintoutLength
#define NextCopyFrom MainNextTuple
#include "commands/copy.h"
#include "commands/copyfrom_internal.h"
#include "../vendor/postgresql-17.11/copyfrom.c"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(challenge_main_ingest);
PG_FUNCTION_INFO_V1(challenge_main_identity);
#ifndef CHALLENGE_PROBE_SOURCE_SHA
#define CHALLENGE_PROBE_SOURCE_SHA "unidentified-main-research"
#endif

typedef struct DimKey { uint32 length; unsigned char first,last,pad[2],name[128]; } DimKey;
typedef struct Dimension { DimKey key; int64 id; } Dimension;
typedef struct MainSource
{
    Relation cache;
    TableScanDesc scan;
    TupleTableSlot *slot;
    HTAB *dimensions;
    AttrNumber raw,proof,identifier,stamp,maximum,numeric1,numeric2,name,code;
    MemoryContext row_context;
} MainSource;

static AttrNumber
attribute(Relation rel,const char *name,Oid type)
{
    AttrNumber n=get_attnum(RelationGetRelid(rel),name);
    Form_pg_attribute a;
    if(n<=0) ereport(ERROR,(errmsg("Typed main column missing: %s",name)));
    a=TupleDescAttr(RelationGetDescr(rel),n-1);
    if(a->attisdropped || a->atttypid!=type || a->atttypmod!=-1 || a->attgenerated)
        ereport(ERROR,(errmsg("Unsupported typed main column: %s",name)));
    return n;
}

static void
permission(Relation rel,AclMode acl)
{
    AclResult result;
    if(rel->rd_rel->relkind!=RELKIND_RELATION || rel->rd_rel->relispartition ||
       rel->rd_rel->relrowsecurity || has_subclass(RelationGetRelid(rel)))
        ereport(ERROR,(errmsg("Typed main requires plain non-RLS tables")));
    result=pg_class_aclcheck(RelationGetRelid(rel),GetUserId(),acl);
    if(result!=ACLCHECK_OK) aclcheck_error(result,OBJECT_TABLE,RelationGetRelationName(rel));
}

static bool
key_from_text(DimKey *key,text *name,unsigned char first,unsigned char last)
{
    int length=VARSIZE_ANY_EXHDR(name);
    if(length>128) ereport(ERROR,(errmsg("Typed main dimension exceeds its proven byte bound")));
    if(length==0 || first<'A' || first>'Z' || last<'A' || last>'Z') return false;
    memset(key,0,sizeof(*key));key->length=length;key->first=first;key->last=last;
    memcpy(key->name,VARDATA_ANY(name),length);return true;
}

static int
no_encoded_input(void *buffer,int minread,int maxread)
{ elog(ERROR,"Typed main must not request encoded input");return 0; }

bool
MainNextTuple(CopyFromState copy,ExprContext *econtext,Datum *values,bool *nulls)
{
    MainSource *s=(MainSource*)copy->copy_file;
    int i;
    MemoryContext previous=CurrentMemoryContext;
    while(table_scan_getnextslot(s->scan,ForwardScanDirection,s->slot))
    {
        bool missing;
        Datum datum;
        text *name,*code;
        int length;
        DimKey key;
        Dimension *dimension;
        CHECK_FOR_INTERRUPTS();
        MemoryContextReset(s->row_context);MemoryContextSwitchTo(s->row_context);
        datum=slot_getattr(s->slot,s->proof,&missing);
        if(missing || !DatumGetBool(datum)) ereport(ERROR,(errmsg("Typed main has an unproven input row")));
        datum=slot_getattr(s->slot,s->name,&missing);if(missing) continue;
        name=DatumGetTextPP(datum);
        datum=slot_getattr(s->slot,s->code,&missing);if(missing) continue;
        code=DatumGetTextPP(datum);length=VARSIZE_ANY_EXHDR(code);if(length==0) continue;
        /* UTF8 ASCII endpoints are exactly native LEFT/RIGHT(...,1).
         * Interior characters can be arbitrary, including multibyte Unicode.
         */
        if(!key_from_text(&key,name,(unsigned char)VARDATA_ANY(code)[0],(unsigned char)VARDATA_ANY(code)[length-1])) continue;
        dimension=hash_search(s->dimensions,&key,HASH_FIND,NULL);if(dimension==NULL) continue;
        memset(values,0,17*sizeof(Datum));memset(nulls,true,17*sizeof(bool));
        values[1]=slot_getattr(s->slot,s->identifier,&nulls[1]);
        values[3]=slot_getattr(s->slot,s->stamp,&nulls[3]);
        values[5]=slot_getattr(s->slot,s->maximum,&nulls[5]);
        values[7]=slot_getattr(s->slot,s->numeric2,&nulls[7]);
        values[9]=slot_getattr(s->slot,s->raw,&nulls[9]);
        values[10]=Int64GetDatum(dimension->id);nulls[10]=false;
        values[16]=slot_getattr(s->slot,s->numeric1,&nulls[16]);
        MemoryContextSwitchTo(previous);
        for(i=0;i<copy->num_defaults;i++)
            values[copy->defmap[i]]=ExecEvalExpr(copy->defexprs[copy->defmap[i]],econtext,&nulls[copy->defmap[i]]);
        copy->cur_lineno++;copy->line_buf.len=64;return true;
    }
    MemoryContextSwitchTo(previous);return false;
}

Datum
challenge_main_identity(PG_FUNCTION_ARGS)
{ PG_RETURN_TEXT_P(cstring_to_text(CHALLENGE_PROBE_SOURCE_SHA)); }

Datum
challenge_main_ingest(PG_FUNCTION_ARGS)
{
    static const char *names[]={"id_column_1","id_column_2","text_column_1","datetime_column_1","text_column_2","integer_column_1","integer_column_2","integer_column_3","text_column_3","id_column_3","id_column_4","id_column_5","integer_column_4","integer_column_5","id_column_6","integer_column_6","integer_column_7"};
    static const Oid types[]={INT8OID,INT4OID,VARCHAROID,TIMESTAMPOID,VARCHAROID,INT2OID,INT2OID,INT4OID,VARCHAROID,INT8OID,INT8OID,INT8OID,INT4OID,INT4OID,INT4OID,INT4OID,INT4OID};
    MainSource source;
    Relation target,dimension;
    TableScanDesc scan;
    TupleTableSlot *slot;
    AttrNumber id,name,kind,number,first,last;
    HASHCTL ctl;
    uint64 count=0,limit=(uint64)work_mem*1024/512,processed;
    ParseState *pstate;
    ParseNamespaceItem *nsitem;
    CopyFromState copy;
    List *columns=NIL;
    ListCell *lc;
    int i;
    if(PG_GETARG_OID(0)==PG_GETARG_OID(1) || PG_GETARG_OID(0)==PG_GETARG_OID(2) || PG_GETARG_OID(1)==PG_GETARG_OID(2))
        ereport(ERROR,(errmsg("Aliased typed main relations are unsupported")));
    PreventCommandIfReadOnly("Typed main COPY");PreventCommandIfParallelMode("Typed main COPY");
    if(GetDatabaseEncoding()!=PG_UTF8 || !get_collation_isdeterministic(DEFAULT_COLLATION_OID))
        ereport(ERROR,(errmsg("Typed main requires UTF8 and deterministic default collation")));
    memset(&source,0,sizeof(source));memset(&ctl,0,sizeof(ctl));
    source.cache=table_open(PG_GETARG_OID(0),AccessShareLock);
    target=table_open(PG_GETARG_OID(1),RowExclusiveLock);
    dimension=table_open(PG_GETARG_OID(2),AccessShareLock);
    permission(source.cache,ACL_SELECT);permission(target,ACL_INSERT);permission(dimension,ACL_SELECT);
    if(RelationGetDescr(target)->natts!=17) ereport(ERROR,(errmsg("Unsupported typed main destination layout")));
    for(i=0;i<17;i++) if(attribute(target,names[i],types[i])!=i+1) ereport(ERROR,(errmsg("Unsupported typed main destination order")));
    if(!TupleDescAttr(RelationGetDescr(target),0)->attidentity) ereport(ERROR,(errmsg("Typed main requires native identity")));
    source.raw=attribute(source.cache,"raw_id",INT8OID);source.proof=attribute(source.cache,"pipeline_domain_safe",BOOLOID);
    source.identifier=attribute(source.cache,"pipeline_identifier",INT4OID);source.stamp=attribute(source.cache,"pipeline_local_stamp",TIMESTAMPOID);
    source.maximum=attribute(source.cache,"pipeline_axis_max",INT2OID);source.numeric1=attribute(source.cache,"pipeline_numeric_1",INT4OID);source.numeric2=attribute(source.cache,"pipeline_numeric_2",INT4OID);
    source.name=attribute(source.cache,"text_column_1",TEXTOID);source.code=attribute(source.cache,"text_column_2",TEXTOID);
    source.row_context=AllocSetContextCreate(CurrentMemoryContext,"Typed main source row",ALLOCSET_SMALL_SIZES);
    ctl.keysize=sizeof(DimKey);ctl.entrysize=sizeof(Dimension);ctl.hcxt=CurrentMemoryContext;
    source.dimensions=hash_create("Typed main actual dimension keys",Min(limit,1024),&ctl,HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
    id=attribute(dimension,"id_column_1",INT8OID);name=attribute(dimension,"text_column_1",VARCHAROID);
    kind=attribute(dimension,"text_column_2",VARCHAROID);number=attribute(dimension,"numeric_column_1",NUMERICOID);
    first=attribute(dimension,"text_column_3",VARCHAROID);last=attribute(dimension,"text_column_4",VARCHAROID);
    scan=table_beginscan(dimension,GetActiveSnapshot(),0,NULL);slot=table_slot_create(dimension,NULL);
    while(table_scan_getnextslot(scan,ForwardScanDirection,slot))
    {
        bool missing,found;
        Datum value;
        text *label,*left,*right;
        DimKey key;
        Dimension *entry;
        CHECK_FOR_INTERRUPTS();
        value=slot_getattr(slot,kind,&missing);
        if(missing || !DatumGetBool(DirectFunctionCall2Coll(texteq,DEFAULT_COLLATION_OID,value,CStringGetTextDatum("SYNTHETIC/REFERENCE")))) continue;
        slot_getattr(slot,number,&missing);if(!missing) continue;
        value=slot_getattr(slot,name,&missing);if(missing) continue;label=DatumGetTextPP(value);
        value=slot_getattr(slot,first,&missing);if(missing) continue;left=DatumGetTextPP(value);
        value=slot_getattr(slot,last,&missing);if(missing) continue;right=DatumGetTextPP(value);
        if(VARSIZE_ANY_EXHDR(left)!=1 || VARSIZE_ANY_EXHDR(right)!=1 ||
           !key_from_text(&key,label,(unsigned char)VARDATA_ANY(left)[0],(unsigned char)VARDATA_ANY(right)[0]))
            ereport(ERROR,(errmsg("Unsupported typed main dimension endpoints")));
        if(++count>limit) ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("Typed main dimension map exceeds work_mem bound")));
        entry=hash_search(source.dimensions,&key,HASH_ENTER,&found);
        if(found) ereport(ERROR,(errmsg("Typed main dimension key is duplicated")));
        value=slot_getattr(slot,id,&missing);if(missing) ereport(ERROR,(errmsg("Typed main dimension identity is NULL")));
        entry->id=DatumGetInt64(value);
    }
    ExecDropSingleTupleTableSlot(slot);table_endscan(scan);table_close(dimension,NoLock);
    pstate=make_parsestate(NULL);nsitem=addRangeTableEntryForRelation(pstate,target,RowExclusiveLock,NULL,false,false);
    nsitem->p_perminfo->requiredPerms=ACL_INSERT;
    for(i=1;i<17;i++) columns=lappend(columns,makeString(pstrdup(names[i])));
    foreach(lc,CopyGetAttnums(RelationGetDescr(target),target,columns))
        nsitem->p_perminfo->insertedCols=bms_add_member(nsitem->p_perminfo->insertedCols,lfirst_int(lc)-FirstLowInvalidHeapAttributeNumber);
    ExecCheckPermissions(pstate->p_rtable,pstate->p_rteperminfos,true);
    source.scan=table_beginscan(source.cache,GetActiveSnapshot(),0,NULL);source.slot=table_slot_create(source.cache,NULL);
    copy=MainBeginCopyFrom(pstate,target,NULL,NULL,false,no_encoded_input,columns,NIL);
    copy->opts.binary=true;copy->copy_file=(FILE*)&source;
    processed=MainCopyFrom(copy);copy->copy_file=NULL;MainEndCopyFrom(copy);
    ExecDropSingleTupleTableSlot(source.slot);table_endscan(source.scan);
    MemoryContextDelete(source.row_context);
    hash_destroy(source.dimensions);table_close(source.cache,NoLock);table_close(target,NoLock);free_parsestate(pstate);
    PG_RETURN_INT64(processed);
}

