#ifndef CHALLENGE_FUSED_H
#define CHALLENGE_FUSED_H
#include "postgres.h"
#include "executor/tuptable.h"
typedef struct FusedCollector
{
    void *context;
    void (*consume)(void *context, TupleTableSlot *slot, int64 native_parent_id);
} FusedCollector;
extern uint64 fused_main_load(Oid input_oid, Oid target_oid, Oid dimension_oid, FusedCollector *collector);
#endif
