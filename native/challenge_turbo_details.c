/* Per-call memoized native values for typed table5 -> table6 -> table2 COPY.
 * Native parent IDs are read once; every target keeps PostgreSQL identity,
 * CHECK/NOT NULL, index, trigger and WAL machinery. No global result cache.
 * The vendored REL_17_11 engine is renamed locally, never server-interposed.
 */
#include "postgres.h"
#include "fmgr.h"
#include "access/htup_details.h"
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
#include "catalog/dependency.h"
#include "catalog/pg_sequence.h"
#include "commands/sequence.h"
#include "storage/lmgr.h"
#include "utils/syscache.h"

#define CopyFrom DetailsCopyFrom
#define BeginCopyFrom DetailsBeginCopyFrom
#define EndCopyFrom DetailsEndCopyFrom
#define CopyFromErrorCallback DetailsErrorCallback
#define CopyLimitPrintoutLength DetailsPrintoutLength
#define NextCopyFrom DetailsNextTuple
#include "commands/copy.h"
#include "commands/copyfrom_internal.h"
#include "../vendor/postgresql-17.11/copyfrom.c"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(challenge_details_ingest);
PG_FUNCTION_INFO_V1(challenge_details_identity);
#ifndef CHALLENGE_PROBE_SOURCE_SHA
#define CHALLENGE_PROBE_SOURCE_SHA "unidentified-details-research"
#endif

/* Native first value, exact final setval, and per-row nextval whenever the
 * contiguous range is not provable. Numbers and final state match PostgreSQL. */
typedef struct LocalIdentity
{
    Oid sequence;
    bool eligible, active, started;
    int64 next, maximum;
    uint64 bound;
} LocalIdentity;

static void
identity_prepare(LocalIdentity *id, Relation target, uint64 bound)
{
    HeapTuple tuple;
    Form_pg_sequence form;
    memset(id,0,sizeof(*id));
    id->sequence=getIdentitySequence(target,1,false);id->bound=bound;
    tuple=SearchSysCache1(SEQRELID,ObjectIdGetDatum(id->sequence));
    if(!HeapTupleIsValid(tuple)) ereport(ERROR,(errmsg("Native identity sequence is missing")));
    form=(Form_pg_sequence)GETSTRUCT(tuple);
    id->eligible=form->seqincrement==1 && !form->seqcycle && form->seqtypid==INT8OID && form->seqcache>1 &&
        pg_class_aclcheck(id->sequence,GetUserId(),ACL_UPDATE)==ACLCHECK_OK;
    id->maximum=form->seqmax;ReleaseSysCache(tuple);
    if(id->eligible) LockRelationOid(id->sequence,ShareRowExclusiveLock);
}

static inline int64
identity_next(LocalIdentity *id)
{
    if(id->active) return id->next++;
    if(id->started) return nextval_internal(id->sequence,false);
    id->started=true;
    id->next=nextval_internal(id->sequence,false);
    if(id->eligible && (uint64)(id->maximum-id->next)>=id->bound)
    { id->active=true;return id->next++; }
    return id->next;
}

static void
identity_finish(LocalIdentity *id)
{
    if(id->active) DirectFunctionCall2(setval_oid,ObjectIdGetDatum(id->sequence),Int64GetDatum(id->next-1));
}

typedef struct DetailRow { int64 parent_id; uint64 frame; int16 integer; int16 code; uint8 rows; } DetailRow;
typedef struct ParentID { int64 raw_id; int64 parent_id; } ParentID;
typedef struct DetailsSource
{
    Relation cache;
    HTAB *parents;
    int64 *dense, minimum;
    uint64 dense_size, parent_count, count, at, frame_used;
    DetailRow *buffered;
    unsigned char *frames;
    const unsigned char *frame_row;
    LocalIdentity identity;
    AttrNumber raw_att, proof_att, axes_att, integer_att, code_att;
    int16 reference_ids[676];
    bool reference_found[676];
    int rows, next, mode;
    int64 parent_id;
    MemoryContext value_context;
    Datum *numeric_values;
    Datum kind_values[3];
} DetailsSource;

static AttrNumber
attribute(Relation rel, const char *name, Oid type)
{
    AttrNumber number=get_attnum(RelationGetRelid(rel),name);
    Form_pg_attribute att;
    if(number<=0) ereport(ERROR,(errmsg("Typed details column is missing: %s",name)));
    att=TupleDescAttr(RelationGetDescr(rel),number-1);
    if(att->attisdropped || att->atttypid!=type || att->atttypmod!=-1 || att->attgenerated)
        ereport(ERROR,(errmsg("Unsupported typed details column: %s",name)));
    return number;
}

static void
relation_permission(Relation rel, AclMode permission)
{
    AclResult result;
    if(rel->rd_rel->relkind!=RELKIND_RELATION || rel->rd_rel->relispartition ||
       rel->rd_rel->relrowsecurity || has_subclass(RelationGetRelid(rel)))
        ereport(ERROR,(errmsg("Typed details requires plain non-RLS tables")));
    result=pg_class_aclcheck(RelationGetRelid(rel),GetUserId(),permission);
    if(result!=ACLCHECK_OK) aclcheck_error(result,OBJECT_TABLE,RelationGetRelationName(rel));
}

static uint32
u32(const unsigned char *p)
{ return ((uint32)p[0]<<24)|((uint32)p[1]<<16)|((uint32)p[2]<<8)|p[3]; }

static int
code_number(text *value)
{
    const unsigned char *s=(const unsigned char*)VARDATA_ANY(value);
    int length=VARSIZE_ANY_EXHDR(value);
    if(length==0) return -1;
    if(length!=2 || s[0]<'A' || s[0]>'Z' || s[1]<'A' || s[1]>'Z')
        ereport(ERROR,(errmsg("Typed details reference code is outside its proven ASCII domain")));
    return (s[0]-'A')*26+s[1]-'A';
}

static int
no_encoded_input(void *buffer, int minread, int maxread)
{ elog(ERROR,"Typed details must not request encoded input");return 0; }

static inline int64 *
parent_of(DetailsSource *s, int64 key)
{
    if(s->dense!=NULL)
    {
        uint64 slot=(uint64)key-(uint64)s->minimum;
        return slot<s->dense_size && s->dense[slot]!=PG_INT64_MIN ? &s->dense[slot] : NULL;
    }
    else
    {
        ParentID *parent=hash_search(s->parents,&key,HASH_FIND,NULL);
        return parent==NULL ? NULL : &parent->parent_id;
    }
}

/* One stage scan buffers exact compact values for all three targets. */
static void
buffer_stage(DetailsSource *s)
{
    TableScanDesc scan=table_beginscan(s->cache,GetActiveSnapshot(),0,NULL);
    TupleTableSlot *slot=table_slot_create(s->cache,NULL);
    uint64 capacity=Max(s->parent_count,1),frame_capacity=capacity*30;
    s->buffered=palloc_extended(capacity*sizeof(DetailRow),MCXT_ALLOC_HUGE);
    s->frames=palloc_extended(frame_capacity,MCXT_ALLOC_HUGE);
    while(table_scan_getnextslot(scan,ForwardScanDirection,slot))
    {
        bool isnull;
        Datum raw=slot_getattr(slot,s->raw_att,&isnull),value;
        int64 *parent;
        DetailRow *row;
        bytea *frame;
        int length,i;
        const unsigned char *p;
        CHECK_FOR_INTERRUPTS();
        if(isnull) ereport(ERROR,(errmsg("Typed details raw_id is NULL")));
        value=slot_getattr(slot,s->proof_att,&isnull);
        if(isnull || !DatumGetBool(value)) ereport(ERROR,(errmsg("Typed details cache has an unproven row")));
        parent=parent_of(s,DatumGetInt64(raw));
        if(parent==NULL) continue;
        if(s->count==capacity) ereport(ERROR,(errmsg("Typed details parent count changed")));
        row=&s->buffered[s->count++];row->parent_id=*parent;
        value=slot_getattr(slot,s->integer_att,&isnull);row->integer=isnull?0:DatumGetInt16(value);
        value=slot_getattr(slot,s->code_att,&isnull);row->code=isnull?-1:code_number(DatumGetTextPP(value));
        value=slot_getattr(slot,s->axes_att,&isnull);
        if(isnull) ereport(ERROR,(errmsg("Proven details axis frame is NULL")));
        frame=DatumGetByteaPP(value);length=VARSIZE_ANY_EXHDR(frame);
        if(length>140 || length%10!=0) ereport(ERROR,(errmsg("Native axis frame length is invalid")));
        p=(const unsigned char*)VARDATA_ANY(frame);
        for(i=0;i<length/10;i++)
        {
            const unsigned char *r=p+10*i;
            if(r[0]<1 || r[0]>14 || (r[1]!='X' && r[1]!='Y' && r[1]!='Z') ||
               u32(r+2)>999999999 || u32(r+6)>999999999 || (i>0 && r[0]<=r[-10]))
                ereport(ERROR,(errmsg("Native axis frame payload is invalid")));
        }
        if(s->frame_used+length>frame_capacity)
        {
            frame_capacity=Max(frame_capacity*2,s->frame_used+length);
            s->frames=repalloc_huge(s->frames,frame_capacity);
        }
        memcpy(s->frames+s->frame_used,p,length);
        row->frame=s->frame_used;row->rows=length/10;s->frame_used+=length;
    }
    ExecDropSingleTupleTableSlot(slot);table_endscan(scan);
}

bool
DetailsNextTuple(CopyFromState copy, ExprContext *econtext, Datum *values, bool *nulls)
{
    DetailsSource *s=(DetailsSource*)copy->copy_file;
    int i;
    CHECK_FOR_INTERRUPTS();
    memset(values,0,RelationGetDescr(copy->rel)->natts*sizeof(Datum));
    memset(nulls,true,RelationGetDescr(copy->rel)->natts*sizeof(bool));
    if(s->mode==5)
    {
        for(;;)
        {
            const DetailRow *row;
            if(s->at==s->count) return false;
            row=&s->buffered[s->at++];
            if(row->integer<=0) continue;
            if(row->integer<10000)
            {
                int key=row->integer;
                if(s->numeric_values[key]==(Datum)0)
                {
                    MemoryContext previous=MemoryContextSwitchTo(s->value_context);
                    s->numeric_values[key]=DirectFunctionCall2(numeric_round,DirectFunctionCall1(int2_numeric,Int16GetDatum(row->integer)),Int32GetDatum(1));
                    MemoryContextSwitchTo(previous);
                }
                values[1]=s->numeric_values[key];
            }
            else values[1]=DirectFunctionCall2(numeric_round,DirectFunctionCall1(int2_numeric,Int16GetDatum(row->integer)),Int32GetDatum(1));
            nulls[1]=false;
            values[5]=Int64GetDatum(row->parent_id);nulls[5]=false;break;
        }
    }
    else if(s->mode==6)
    {
        for(;;)
        {
            const DetailRow *row;
            if(s->at==s->count) return false;
            row=&s->buffered[s->at++];
            if(row->code<0 || !s->reference_found[row->code]) continue;
            values[1]=Int64GetDatum(row->parent_id);nulls[1]=false;
            values[2]=Int16GetDatum(s->reference_ids[row->code]);nulls[2]=false;break;
        }
    }
    else
    {
        const unsigned char *row;
        while(s->next==s->rows)
        {
            const DetailRow *source;
            if(s->at==s->count) return false;
            source=&s->buffered[s->at++];
            s->frame_row=s->frames+source->frame;s->parent_id=source->parent_id;
            s->rows=source->rows;s->next=0;
        }
        row=s->frame_row+10*s->next++;
        values[1]=Int64GetDatum(s->parent_id);nulls[1]=false;
        values[2]=Int16GetDatum(row[0]);nulls[2]=false;
        values[3]=Int32GetDatum(u32(row+2));nulls[3]=false;
        values[4]=s->kind_values[row[1]=='X'?0:row[1]=='Y'?1:2];nulls[4]=false;
        if(u32(row+6)>0) { values[8]=Int32GetDatum(u32(row+6));nulls[8]=false; }
    }
    values[0]=Int64GetDatum(identity_next(&s->identity));nulls[0]=false;
    for(i=0;i<copy->num_defaults;i++)
        values[copy->defmap[i]]=ExecEvalExpr(copy->defexprs[copy->defmap[i]],econtext,&nulls[copy->defmap[i]]);
    copy->cur_lineno++;copy->line_buf.len=64;return true;
}

static uint64
load_target(DetailsSource *source, Oid oid, int mode)
{
    static const char *axis_names[]={"id_column_1","id_column_2","integer_column_1","integer_column_2","text_column_1","integer_column_3","integer_column_4","integer_column_5","integer_column_6"};
    static const Oid axis_types[]={INT8OID,INT8OID,INT2OID,INT4OID,VARCHAROID,INT4OID,INT4OID,INT4OID,INT4OID};
    static const char *five_names[]={"id_column_1","numeric_column_1","numeric_column_2","integer_column_1","integer_column_2","id_column_2","id_column_3"};
    static const Oid five_types[]={INT8OID,NUMERICOID,NUMERICOID,INT4OID,INT4OID,INT8OID,INT8OID};
    static const char *six_names[]={"id_column_1","id_column_2","id_column_3"};
    static const Oid six_types[]={INT8OID,INT8OID,INT2OID};
    const char **names=mode==5?five_names:mode==6?six_names:axis_names;
    const Oid *types=mode==5?five_types:mode==6?six_types:axis_types;
    int count=mode==5?7:mode==6?3:9,i;
    Relation target=table_open(oid,RowExclusiveLock);
    ParseState *pstate=make_parsestate(NULL);
    ParseNamespaceItem *nsitem;
    CopyFromState copy;
    List *columns=NIL,*copied;
    ListCell *lc;
    uint64 processed;
    relation_permission(target,ACL_INSERT);
    if(RelationGetDescr(target)->natts!=count) ereport(ERROR,(errmsg("Unsupported typed details destination layout")));
    for(i=0;i<count;i++) if(attribute(target,names[i],types[i])!=i+1) ereport(ERROR,(errmsg("Unsupported typed details column order")));
    if(!TupleDescAttr(RelationGetDescr(target),0)->attidentity) ereport(ERROR,(errmsg("Typed details destination requires a native identity")));
    nsitem=addRangeTableEntryForRelation(pstate,target,RowExclusiveLock,NULL,false,false);
    nsitem->p_perminfo->requiredPerms=ACL_INSERT;
    for(i=1;i<count;i++) columns=lappend(columns,makeString(pstrdup(names[i])));
    foreach(lc,CopyGetAttnums(RelationGetDescr(target),target,columns))
        nsitem->p_perminfo->insertedCols=bms_add_member(nsitem->p_perminfo->insertedCols,lfirst_int(lc)-FirstLowInvalidHeapAttributeNumber);
    ExecCheckPermissions(pstate->p_rtable,pstate->p_rteperminfos,true);
    source->mode=mode;source->rows=source->next=0;source->at=0;
    identity_prepare(&source->identity,target,source->count*(mode==2?14:1));
    copied=lcons(makeString(pstrdup(names[0])),list_copy(columns));
    copy=DetailsBeginCopyFrom(pstate,target,NULL,NULL,false,no_encoded_input,copied,NIL);
    copy->opts.binary=true;copy->copy_file=(FILE*)source;
    processed=DetailsCopyFrom(copy);copy->copy_file=NULL;DetailsEndCopyFrom(copy);
    identity_finish(&source->identity);
    table_close(target,NoLock);free_parsestate(pstate);return processed;
}

Datum
challenge_details_identity(PG_FUNCTION_ARGS)
{ PG_RETURN_TEXT_P(cstring_to_text(CHALLENGE_PROBE_SOURCE_SHA)); }

Datum
challenge_details_ingest(PG_FUNCTION_ARGS)
{
    Oid oids[6];
    DetailsSource source;
    Relation parent,reference;
    TableScanDesc scan;
    TupleTableSlot *slot;
    AttrNumber raw_att,id_att,ref_id,ref_kind,ref_code;
    HASHCTL ctl;
    uint64 count=0,limit=(uint64)work_mem*1024/96;
    Datum results[3];
    int i,j;
    for(i=0;i<6;i++) { oids[i]=PG_GETARG_OID(i);for(j=0;j<i;j++) if(oids[i]==oids[j]) ereport(ERROR,(errmsg("Aliased typed details relations are unsupported"))); }
    if(XactReadOnly || IsInParallelMode()) ereport(ERROR,(errmsg("Typed details cannot execute in a read-only or parallel transaction")));
    if(!get_collation_isdeterministic(DEFAULT_COLLATION_OID)) ereport(ERROR,(errmsg("Typed details requires deterministic default collation")));
    memset(&source,0,sizeof(source));memset(&ctl,0,sizeof(ctl));
    source.value_context=CurrentMemoryContext;
    source.numeric_values=palloc0(10000*sizeof(Datum));
    source.kind_values[0]=CStringGetTextDatum("X");source.kind_values[1]=CStringGetTextDatum("Y");source.kind_values[2]=CStringGetTextDatum("Z");
    source.cache=table_open(oids[0],AccessShareLock);parent=table_open(oids[1],AccessShareLock);reference=table_open(oids[5],AccessShareLock);
    relation_permission(source.cache,ACL_SELECT);relation_permission(parent,ACL_SELECT);relation_permission(reference,ACL_SELECT);
    source.raw_att=attribute(source.cache,"raw_id",INT8OID);source.proof_att=attribute(source.cache,"pipeline_domain_safe",BOOLOID);
    source.axes_att=attribute(source.cache,"pipeline_axes",BYTEAOID);source.integer_att=attribute(source.cache,"pipeline_integer_1",INT2OID);source.code_att=attribute(source.cache,"text_column_3",TEXTOID);
    raw_att=attribute(parent,"id_column_3",INT8OID);id_att=attribute(parent,"id_column_1",INT8OID);
    ctl.keysize=sizeof(int64);ctl.entrysize=sizeof(ParentID);ctl.hcxt=CurrentMemoryContext;
    source.parents=hash_create("Typed details actual native parent IDs",Min(limit,1024),&ctl,HASH_ELEM|HASH_BLOBS|HASH_CONTEXT);
    /* A first bounds pass keeps the original work_mem limit and chooses a
     * direct raw_id array whenever keys are dense; sparse keys keep the hash. */
    scan=table_beginscan(parent,GetActiveSnapshot(),0,NULL);slot=table_slot_create(parent,NULL);
    {
        bool any=false;
        int64 minimum=0,maximum=0;
        while(table_scan_getnextslot(scan,ForwardScanDirection,slot))
        {
            bool null_raw;
            Datum raw=slot_getattr(slot,raw_att,&null_raw);
            int64 key;
            CHECK_FOR_INTERRUPTS();if(null_raw) continue;
            if(++count>limit) ereport(ERROR,(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),errmsg("Typed details parent hash exceeds its work_mem bound")));
            key=DatumGetInt64(raw);
            if(!any || key<minimum) minimum=key;
            if(!any || key>maximum) maximum=key;
            any=true;
        }
        count=0;
        if(any && (uint64)maximum-(uint64)minimum<(uint64)work_mem*1024/sizeof(int64))
        {
            uint64 slots;
            source.minimum=minimum;source.dense_size=(uint64)maximum-(uint64)minimum+1;
            source.dense=palloc_extended(source.dense_size*sizeof(int64),MCXT_ALLOC_HUGE);
            for(slots=0;slots<source.dense_size;slots++) source.dense[slots]=PG_INT64_MIN;
        }
    }
    table_rescan(scan,NULL);
    while(table_scan_getnextslot(scan,ForwardScanDirection,slot))
    {
        bool null_raw,null_id,found;
        Datum raw=slot_getattr(slot,raw_att,&null_raw),id=slot_getattr(slot,id_att,&null_id);
        int64 key;
        ParentID *entry;
        CHECK_FOR_INTERRUPTS();if(null_raw) continue;
        if(null_id) ereport(ERROR,(errmsg("Native parent identity is NULL")));
        ++count;key=DatumGetInt64(raw);
        if(source.dense!=NULL)
        {
            int64 *entry_id=&source.dense[(uint64)key-(uint64)source.minimum];
            if(*entry_id!=PG_INT64_MIN) ereport(ERROR,(errmsg("Typed details requires unique parent raw_id values")));
            *entry_id=DatumGetInt64(id);
        }
        else
        {
            entry=hash_search(source.parents,&key,HASH_ENTER,&found);
            if(found) ereport(ERROR,(errmsg("Typed details requires unique parent raw_id values")));
            entry->parent_id=DatumGetInt64(id);
        }
    }
    ExecDropSingleTupleTableSlot(slot);table_endscan(scan);table_close(parent,NoLock);
    ref_id=attribute(reference,"id_column_1",INT2OID);ref_kind=attribute(reference,"text_column_1",VARCHAROID);ref_code=attribute(reference,"text_column_2",VARCHAROID);
    scan=table_beginscan(reference,GetActiveSnapshot(),0,NULL);slot=table_slot_create(reference,NULL);
    while(table_scan_getnextslot(scan,ForwardScanDirection,slot))
    {
        bool isnull;
        Datum kind=slot_getattr(slot,ref_kind,&isnull),value,id;
        int code;
        if(isnull || !DatumGetBool(DirectFunctionCall2Coll(texteq,DEFAULT_COLLATION_OID,kind,CStringGetTextDatum("SYNTHETIC/GENERIC")))) continue;
        value=slot_getattr(slot,ref_code,&isnull);if(isnull) continue;
        code=code_number(DatumGetTextPP(value));if(code<0) continue;
        id=slot_getattr(slot,ref_id,&isnull);if(isnull || source.reference_found[code]) ereport(ERROR,(errmsg("Typed details reference key is NULL or duplicate")));
        source.reference_ids[code]=DatumGetInt16(id);source.reference_found[code]=true;
    }
    ExecDropSingleTupleTableSlot(slot);table_endscan(scan);table_close(reference,NoLock);
    source.parent_count=count;
    buffer_stage(&source);
    results[0]=Int64GetDatum(load_target(&source,oids[2],5));
    results[1]=Int64GetDatum(load_target(&source,oids[3],6));
    results[2]=Int64GetDatum(load_target(&source,oids[4],2));
    hash_destroy(source.parents);table_close(source.cache,NoLock);
    PG_RETURN_ARRAYTYPE_P(construct_array_builtin(results,3,INT8OID));
}
