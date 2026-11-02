/* Current-snapshot single-pass actual-key FK validation research, index-only raw keys. Catalog completion follows only
 * after full checking of all six relations; unsupported domains change nothing.
 * All keys come from actual parent tuples in this statement snapshot.
 */
#include "postgres.h"
#include "fmgr.h"
#include "access/genam.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/visibilitymap.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/objectaccess.h"
#include "access/tableam.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_index.h"
#include "catalog/namespace.h"
#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "storage/bufmgr.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(challenge_fk_scan);
PG_FUNCTION_INFO_V1(challenge_fk_validate);
PG_FUNCTION_INFO_V1(challenge_fk_identity);
#ifndef CHALLENGE_PROBE_SOURCE_SHA
#define CHALLENGE_PROBE_SOURCE_SHA "unidentified-fk-validation"
#endif

typedef struct KeySet { uint64 first_byte,bytes; unsigned char *bits; } KeySet;
typedef struct ForeignKey
{
    int child, parent;
    const char *column;
    AttrNumber att;
    char *name;
    Oid oid;
} ForeignKey;

static AttrNumber
column(Relation r,const char *name,Oid type)
{
    AttrNumber n=get_attnum(RelationGetRelid(r),name);
    Form_pg_attribute a;
    if(n<=0) return InvalidAttrNumber;
    a=TupleDescAttr(RelationGetDescr(r),n-1);
    if(a->attisdropped || a->atttypid!=type || a->atttypmod!=-1 || a->attgenerated)
        return InvalidAttrNumber;
    return n;
}

static bool
primary(Relation r,AttrNumber att)
{
    Oid oid;
    Bitmapset *attrs=get_primary_key_attnos(RelationGetRelid(r),false,&oid);
    HeapTuple tuple;
    Form_pg_constraint con;
    bool ok;
    if(attrs==NULL) return false;
    /* PostgreSQL represents relation attributes offset by -7 here. */
    ok=bms_num_members(attrs)==1 && bms_is_member(att+7,attrs);
    bms_free(attrs);
    if(!ok) return false;
    tuple=SearchSysCache1(CONSTROID,ObjectIdGetDatum(oid));
    if(!HeapTupleIsValid(tuple)) return false;
    con=(Form_pg_constraint)GETSTRUCT(tuple);
    ok=con->contype==CONSTRAINT_PRIMARY && con->convalidated && !con->condeferrable &&
       !OidIsValid(con->conparentid) && con->conislocal && con->coninhcount==0;
    if(ok)
    {
        HeapTuple index=SearchSysCache1(INDEXRELID,ObjectIdGetDatum(con->conindid));
        if(!HeapTupleIsValid(index)) ok=false;
        else
        {
            Form_pg_index info=(Form_pg_index)GETSTRUCT(index);
            ok=info->indrelid==RelationGetRelid(r) && info->indisvalid && info->indisunique &&
               info->indisprimary && info->indimmediate && info->indisready && info->indislive &&
               info->indnkeyatts==1 && info->indkey.values[0]==att;
            ReleaseSysCache(index);
        }
    }
    ReleaseSysCache(tuple);return ok;
}

static bool
foreign(Relation child,Relation parent,ForeignKey *fk,AttrNumber pk,Oid eq)
{
    List *oids=RelationGetFKeyList(child);
    ListCell *cell;
    int matched=0;
    foreach(cell,oids)
    {
        ForeignKeyCacheInfo *info=lfirst(cell);
        HeapTuple tuple;
        Form_pg_constraint con;
        bool ok;
        int nkeys,ndeletes;
        AttrNumber childkeys[INDEX_MAX_KEYS],parentkeys[INDEX_MAX_KEYS],deletekeys[INDEX_MAX_KEYS];
        Oid pfeq[INDEX_MAX_KEYS],ppeq[INDEX_MAX_KEYS],ffeq[INDEX_MAX_KEYS];
        if(info->confrelid!=RelationGetRelid(parent) || info->nkeys!=1 ||
           info->conkey[0]!=fk->att || info->confkey[0]!=pk) continue;
        tuple=SearchSysCache1(CONSTROID,ObjectIdGetDatum(info->conoid));
        if(!HeapTupleIsValid(tuple)) return false;
        con=(Form_pg_constraint)GETSTRUCT(tuple);
        DeconstructFkConstraintRow(tuple,&nkeys,childkeys,parentkeys,pfeq,ppeq,ffeq,&ndeletes,deletekeys);
        ok=con->contype==CONSTRAINT_FOREIGN && !con->condeferrable && !con->condeferred &&
           con->confupdtype==FKCONSTR_ACTION_CASCADE && con->confdeltype==FKCONSTR_ACTION_CASCADE &&
           con->confmatchtype==FKCONSTR_MATCH_SIMPLE && !OidIsValid(con->conparentid) &&
           con->conislocal && con->coninhcount==0 && nkeys==1 && ndeletes==0 &&
           childkeys[0]==fk->att && parentkeys[0]==pk &&
           get_opcode(pfeq[0])==eq && get_opcode(ppeq[0])==eq && get_opcode(ffeq[0])==eq;
        if(ok) { fk->name=pstrdup(NameStr(con->conname));fk->oid=con->oid; }
        ReleaseSysCache(tuple);
        if(!ok) return false;
        matched++;
    }
    return matched==1;
}

static bool
key_add(KeySet *set,int64 key,uint64 *used,uint64 limit)
{
    uint64 ordinal=((uint64)key)^UINT64CONST(0x8000000000000000),byte=ordinal>>3,offset;
    if(set->bits==NULL)
    {
        uint64 bytes=1024;
        if(bytes>limit-*used) return false;
        set->first_byte=byte&~UINT64CONST(1023);
        set->bytes=bytes;set->bits=palloc0(bytes);*used+=bytes;
    }
    else if(byte<set->first_byte || byte-set->first_byte>=set->bytes)
    {
        uint64 first=Min(byte,set->first_byte);
        uint64 last=Max(byte,set->first_byte+set->bytes-1);
        uint64 span=last-first+1,bytes=set->bytes;
        unsigned char *newbits;
        while(bytes<span && bytes<=MaxAllocSize/2) bytes*=2;
        /* Include old and new allocations in the peak memory bound. */
        if(bytes<span || bytes>MaxAllocSize || bytes>limit-*used) return false;
        newbits=palloc0(bytes);
        memcpy(newbits+(set->first_byte-first),set->bits,set->bytes);
        pfree(set->bits);*used=*used-set->bytes+bytes;
        set->first_byte=first;set->bytes=bytes;set->bits=newbits;
    }
    offset=byte-set->first_byte;
    if(set->bits[offset] & (1U<<(ordinal&7)))
        elog(ERROR,"Validated parent primary key is duplicated");
    set->bits[offset] |= 1U<<(ordinal&7);
    return true;
}

/* Index-only key read with PostgreSQL's visibility rule: an all-visible page
 * needs no heap access, every other entry is confirmed against the heap. */
static bool
index_keys(Relation heap,AttrNumber att,Snapshot snapshot,KeySet *set,uint64 *used,uint64 limit)
{
    Oid oid=RelationGetPrimaryKeyIndex(heap);
    Relation index;
    IndexScanDesc scan;
    Buffer vm=InvalidBuffer;
    ItemPointer tid;
    BlockNumber blocks=RelationGetNumberOfBlocks(heap),visible=0;
    bool ok=true;
    if(!OidIsValid(oid)) return false;
    visibilitymap_count(heap,&visible,NULL);
    if(blocks==0 || (uint64)visible*10<(uint64)blocks*9) return false;
    index=index_open(oid,AccessShareLock);
    if(index->rd_index->indnkeyatts!=1 || index->rd_index->indkey.values[0]!=att)
    { index_close(index,AccessShareLock);return false; }
    scan=index_beginscan(heap,index,snapshot,0,0);
    scan->xs_want_itup=true;
    index_rescan(scan,NULL,0,NULL,0);
    while(ok && (tid=index_getnext_tid(scan,ForwardScanDirection))!=NULL)
    {
        bool isnull,all_dead;
        Datum value;
        CHECK_FOR_INTERRUPTS();
        if(!VM_ALL_VISIBLE(heap,ItemPointerGetBlockNumber(tid),&vm) &&
           !table_index_fetch_tuple_check(heap,tid,snapshot,&all_dead)) continue;
        value=index_getattr(scan->xs_itup,1,scan->xs_itupdesc,&isnull);
        if(isnull) elog(ERROR,"Validated parent primary key is NULL");
        ok=key_add(set,DatumGetInt64(value),used,limit);
    }
    if(BufferIsValid(vm)) ReleaseBuffer(vm);
    index_endscan(scan);index_close(index,AccessShareLock);
    if(!ok && set->bits!=NULL)
    { *used-=set->bytes;pfree(set->bits);set->bits=NULL;set->bytes=0; }
    return ok;
}

static Datum
scan_graph(FunctionCallInfo fcinfo,bool complete)
{
    /* Relation argument order raw_data, table1, table3, table4, table2, table5, table6. */
    const char *names[7]={"raw_data","table1","table3","table4","table2","table5","table6"};
    Relation rels[7]={0};
    AttrNumber pk[4];
    KeySet sets[4]={{0}};
    ForeignKey fks[6]={{1,0,"id_column_3",0,NULL},{1,2,"id_column_4",0,NULL},
        {4,1,"id_column_2",0,NULL},{5,1,"id_column_2",0,NULL},
        {6,1,"id_column_2",0,NULL},{6,3,"id_column_3",0,NULL}};
    Datum counts[6];
    Snapshot validation_snapshot;
    uint64 used=0,limit=(uint64)work_mem*1024;
    Oid public_oid=get_namespace_oid("public",false);
    bool supported=true;
    int i;
    if(complete)
    {
        PreventCommandIfReadOnly("Complete shared-key FK validation");
        PreventCommandIfParallelMode("Complete shared-key FK validation");
    }
    for(i=0;i<7;i++)
    {
        rels[i]=table_open(PG_GETARG_OID(i),i==0?ShareLock:AccessExclusiveLock);
        if(strcmp(RelationGetRelationName(rels[i]),names[i])!=0 ||
           RelationGetNamespace(rels[i])!=public_oid ||
           rels[i]->rd_rel->relkind!=RELKIND_RELATION || rels[i]->rd_rel->relispartition ||
           rels[i]->rd_rel->relrowsecurity || has_subclass(RelationGetRelid(rels[i])) ||
           !object_ownercheck(RelationRelationId,RelationGetRelid(rels[i]),GetUserId()) ||
           pg_class_aclcheck(RelationGetRelid(rels[i]),GetUserId(),ACL_SELECT)!=ACLCHECK_OK)
            supported=false;
    }
    for(i=0;i<4 && supported;i++)
    {
        pk[i]=column(rels[i],i==0?"raw_id":"id_column_1",i==3?INT2OID:INT8OID);
        if(pk[i]==InvalidAttrNumber || !primary(rels[i],pk[i])) supported=false;
    }
    for(i=0;i<6 && supported;i++)
    {
        ForeignKey *f=&fks[i];
        f->att=column(rels[f->child],f->column,i==5?INT2OID:INT8OID);
        if(f->att==InvalidAttrNumber || !foreign(rels[f->child],rels[f->parent],f,pk[f->parent],i==5?F_INT2EQ:F_INT8EQ))
            supported=false;
    }
    /* Match RI_Initial_Check: an old RR statement snapshot must not hide
     * rows committed before the validation locks were acquired. Register one
     * current snapshot after locking the complete graph, reuse it throughout.
     */
    validation_snapshot=RegisterSnapshot(GetLatestSnapshot());
    /* table1's keys are collected during its child scan below. */
    for(i=0;i<4 && supported;i++)
    {
        KeySet *set=&sets[i];
        TableScanDesc scan;
        TupleTableSlot *slot;
        if(i==1) continue;
        if(i==0 && index_keys(rels[0],pk[0],validation_snapshot,set,&used,limit)) continue;
        scan=table_beginscan(rels[i],validation_snapshot,0,NULL);
        slot=table_slot_create(rels[i],NULL);
        while(table_scan_getnextslot(scan,ForwardScanDirection,slot))
        {
            bool isnull;
            Datum value=slot_getattr(slot,pk[i],&isnull);
            CHECK_FOR_INTERRUPTS();
            if(isnull) elog(ERROR,"Validated parent primary key is NULL");
            if(!key_add(set,i==3?(int64)DatumGetInt16(value):DatumGetInt64(value),&used,limit)) { supported=false;break; }
        }
        ExecDropSingleTupleTableSlot(slot);table_endscan(scan);
    }
    for(i=0;i<6 && supported;i++)
    {
        ForeignKey *f=&fks[i];
        Relation child=rels[f->child];
        TableScanDesc scan=table_beginscan(child,validation_snapshot,0,NULL);
        TupleTableSlot *slot=table_slot_create(child,NULL);
        int width=(i==0 || i==4)?2:1,j;
        int64 checked[2]={0,0},badkey=0;
        bool second_bad=false;
        while(table_scan_getnextslot(scan,ForwardScanDirection,slot))
        {
            CHECK_FOR_INTERRUPTS();
            if(i==0)
            {
                bool isnull;
                Datum value=slot_getattr(slot,pk[1],&isnull);
                if(isnull) elog(ERROR,"Validated parent primary key is NULL");
                if(!key_add(&sets[1],DatumGetInt64(value),&used,limit)) { supported=false;break; }
            }
            for(j=0;j<width;j++)
            {
                ForeignKey *current=&fks[i+j];
                bool isnull;
                Datum value=slot_getattr(slot,current->att,&isnull);
                int64 key;
                uint64 ordinal,byte,offset;
                if(isnull) continue;
                key=i+j==5?(int64)DatumGetInt16(value):DatumGetInt64(value);
                ordinal=((uint64)key)^UINT64CONST(0x8000000000000000);byte=ordinal>>3;
                offset=byte-sets[current->parent].first_byte;
                if(byte<sets[current->parent].first_byte || offset>=sets[current->parent].bytes || !(sets[current->parent].bits[offset] & (1U<<(ordinal&7))))
                {
                    if(j==1) { if(!second_bad) badkey=key;second_bad=true;continue; }
                    ereport(ERROR,(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
                        errmsg("insert or update on table \"%s\" violates foreign key constraint \"%s\"",RelationGetRelationName(child),current->name),
                        errdetail("Key (%s)=(" INT64_FORMAT ") is not present in table \"%s\".",current->column,key,RelationGetRelationName(rels[current->parent])),
                        errtableconstraint(child,current->name)));
                }
                checked[j]++;
            }
        }
        ExecDropSingleTupleTableSlot(slot);table_endscan(scan);
        if(supported && second_bad)
        {
            ForeignKey *current=&fks[i+1];
            ereport(ERROR,(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
                errmsg("insert or update on table \"%s\" violates foreign key constraint \"%s\"",RelationGetRelationName(child),current->name),
                errdetail("Key (%s)=(" INT64_FORMAT ") is not present in table \"%s\".",current->column,badkey,RelationGetRelationName(rels[current->parent])),
                errtableconstraint(child,current->name)));
        }
        for(j=0;j<width;j++) counts[i+j]=Int64GetDatum(checked[j]);
        i+=width-1;
    }
    UnregisterSnapshot(validation_snapshot);
    /* No catalog writes until every child key has passed. The relation locks
     * acquired before the snapshot scans remain held until transaction end.
     * Use the same CatalogTupleUpdate/post-alter-hook mechanism as PostgreSQL
     * REL_17_11 ATExecValidateConstraint; errors roll back the whole operation.
     */
    if(supported && complete)
    {
        Relation catalog=table_open(ConstraintRelationId,RowExclusiveLock);
        for(i=0;i<6;i++)
        {
            HeapTuple copy=SearchSysCacheCopy1(CONSTROID,ObjectIdGetDatum(fks[i].oid));
            Form_pg_constraint con;
            if(!HeapTupleIsValid(copy)) elog(ERROR,"Checked FK disappeared");
            con=(Form_pg_constraint)GETSTRUCT(copy);
            if(con->contype!=CONSTRAINT_FOREIGN || con->conrelid!=RelationGetRelid(rels[fks[i].child]) ||
               con->confrelid!=RelationGetRelid(rels[fks[i].parent]) || strcmp(NameStr(con->conname),fks[i].name)!=0)
                elog(ERROR,"Checked FK identity changed");
            if(!con->convalidated)
            {
                con->convalidated=true;
                CatalogTupleUpdate(catalog,&copy->t_self,copy);
                InvokeObjectPostAlterHook(ConstraintRelationId,con->oid,0);
                CommandCounterIncrement();
            }
            heap_freetuple(copy);
        }
        table_close(catalog,RowExclusiveLock);
    }
    for(i=0;i<4;i++) if(sets[i].bits) pfree(sets[i].bits);
    for(i=0;i<6;i++) if(fks[i].name) pfree(fks[i].name);
    for(i=6;i>=0;i--) table_close(rels[i],NoLock);
    if(!supported) PG_RETURN_NULL();
    PG_RETURN_ARRAYTYPE_P(construct_array(counts,6,INT8OID,8,true,TYPALIGN_DOUBLE));
}

Datum
challenge_fk_scan(PG_FUNCTION_ARGS)
{ return scan_graph(fcinfo,false); }

Datum
challenge_fk_validate(PG_FUNCTION_ARGS)
{ return scan_graph(fcinfo,true); }

Datum
challenge_fk_identity(PG_FUNCTION_ARGS)
{ PG_RETURN_TEXT_P(cstring_to_text(CHALLENGE_PROBE_SOURCE_SHA)); }
