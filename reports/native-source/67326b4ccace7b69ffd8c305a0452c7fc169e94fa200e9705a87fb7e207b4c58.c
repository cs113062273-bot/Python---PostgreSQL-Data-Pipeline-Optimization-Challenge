/* Read-only shared-key FK research. Never modifies pg_constraint or data.
 * All keys come from actual parent tuples in this statement snapshot.
 */
#include "postgres.h"
#include "fmgr.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/pg_constraint.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_type_d.h"
#include "executor/executor.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(challenge_fk_scan);

typedef struct Key { int64 value; } Key;
typedef struct ForeignKey
{
    int child, parent;
    const char *column;
    AttrNumber att;
    char *name;
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
        if(info->confrelid!=RelationGetRelid(parent) || info->nkeys!=1 ||
           info->conkey[0]!=fk->att || info->confkey[0]!=pk) continue;
        tuple=SearchSysCache1(CONSTROID,ObjectIdGetDatum(info->conoid));
        if(!HeapTupleIsValid(tuple)) return false;
        con=(Form_pg_constraint)GETSTRUCT(tuple);
        ok=con->contype==CONSTRAINT_FOREIGN && !con->condeferrable && !con->condeferred &&
           con->confupdtype==FKCONSTR_ACTION_CASCADE && con->confdeltype==FKCONSTR_ACTION_CASCADE &&
           con->confmatchtype==FKCONSTR_MATCH_SIMPLE && !OidIsValid(con->conparentid) &&
           con->conislocal && con->coninhcount==0 && get_opcode(info->conpfeqop[0])==eq;
        if(ok) fk->name=pstrdup(NameStr(con->conname));
        ReleaseSysCache(tuple);
        if(!ok) return false;
        matched++;
    }
    return matched==1;
}

Datum
challenge_fk_scan(PG_FUNCTION_ARGS)
{
    /* Relation argument order raw_data, table1, table3, table4, table2, table5, table6. */
    const char *names[7]={"raw_data","table1","table3","table4","table2","table5","table6"};
    Relation rels[7]={0};
    AttrNumber pk[4];
    HTAB *sets[4]={0};
    ForeignKey fks[6]={{1,0,"id_column_3",0,NULL},{1,2,"id_column_4",0,NULL},
        {4,1,"id_column_2",0,NULL},{5,1,"id_column_2",0,NULL},
        {6,1,"id_column_2",0,NULL},{6,3,"id_column_3",0,NULL}};
    Datum counts[6];
    uint64 used=0,limit=(uint64)work_mem*1024/64;
    Oid public_oid=get_namespace_oid("public",false);
    bool supported=true;
    int i;
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
    for(i=0;i<4 && supported;i++)
    {
        HASHCTL ctl;
        TableScanDesc scan;
        TupleTableSlot *slot;
        memset(&ctl,0,sizeof(ctl));ctl.keysize=sizeof(int64);ctl.entrysize=sizeof(Key);
        sets[i]=hash_create("per-call actual FK parent keys",1024,&ctl,HASH_ELEM|HASH_BLOBS);
        scan=table_beginscan(rels[i],GetActiveSnapshot(),0,NULL);
        slot=table_slot_create(rels[i],NULL);
        while(table_scan_getnextslot(scan,ForwardScanDirection,slot))
        {
            bool isnull,found;
            Datum value=slot_getattr(slot,pk[i],&isnull);
            int64 key;
            CHECK_FOR_INTERRUPTS();
            if(isnull) elog(ERROR,"Validated parent primary key is NULL");
            if(++used>limit) { supported=false;break; }
            key=i==3?(int64)DatumGetInt16(value):DatumGetInt64(value);
            hash_search(sets[i],&key,HASH_ENTER,&found);
            if(found) elog(ERROR,"Validated parent primary key is duplicated");
        }
        ExecDropSingleTupleTableSlot(slot);table_endscan(scan);
    }
    for(i=0;i<6 && supported;i++)
    {
        ForeignKey *f=&fks[i];
        Relation child=rels[f->child];
        TableScanDesc scan=table_beginscan(child,GetActiveSnapshot(),0,NULL);
        TupleTableSlot *slot=table_slot_create(child,NULL);
        int64 checked=0;
        while(table_scan_getnextslot(scan,ForwardScanDirection,slot))
        {
            bool isnull;
            Datum value=slot_getattr(slot,f->att,&isnull);
            int64 key;
            CHECK_FOR_INTERRUPTS();
            if(isnull) continue;
            key=i==5?(int64)DatumGetInt16(value):DatumGetInt64(value);
            if(hash_search(sets[f->parent],&key,HASH_FIND,NULL)==NULL)
                ereport(ERROR,(errcode(ERRCODE_FOREIGN_KEY_VIOLATION),
                    errmsg("insert or update on table \"%s\" violates foreign key constraint \"%s\"",RelationGetRelationName(child),f->name),
                    errdetail("Key (%s)=(" INT64_FORMAT ") is not present in table \"%s\".",f->column,key,RelationGetRelationName(rels[f->parent])),
                    errtableconstraint(child,f->name)));
            checked++;
        }
        counts[i]=Int64GetDatum(checked);
        ExecDropSingleTupleTableSlot(slot);table_endscan(scan);
    }
    for(i=0;i<4;i++) if(sets[i]) hash_destroy(sets[i]);
    for(i=0;i<6;i++) if(fks[i].name) pfree(fks[i].name);
    for(i=6;i>=0;i--) table_close(rels[i],NoLock);
    if(!supported) PG_RETURN_NULL();
    PG_RETURN_ARRAYTYPE_P(construct_array(counts,6,INT8OID,8,true,TYPALIGN_DOUBLE));
}
