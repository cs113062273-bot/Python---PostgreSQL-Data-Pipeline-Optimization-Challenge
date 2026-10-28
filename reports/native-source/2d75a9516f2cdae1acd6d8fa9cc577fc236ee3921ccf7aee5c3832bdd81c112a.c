/* Isolated research only. Pure JSONB argument traversal, no SPI/data access. */
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type_d.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/numeric.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(challenge_extract64);
PG_FUNCTION_INFO_V1(challenge_probe_identity);

#ifndef CHALLENGE_PROBE_SOURCE_SHA
#define CHALLENGE_PROBE_SOURCE_SHA "unidentified-research-build"
#endif

#define FIELD_COUNT 64
typedef struct ProbeDescriptor
{
    TupleDesc desc;
    int output_position[FIELD_COUNT];
} ProbeDescriptor;

/* Internal field numbers are derived from the fixed recipe column names. */
static int
field_index(const char *name, int length)
{
    int prefix = 0, count = 0, offset = 0, ordinal = 0, i;
    if (length == 17 && memcmp(name, "datetime_column_1", 17) == 0) return 0;
    if (length == 11 && memcmp(name, "id_column_1", 11) == 0) return 1;
    if (length > 15 && memcmp(name, "integer_column_", 15) == 0)
        { prefix = 15; count = 15; offset = 2; }
    else if (length > 15 && memcmp(name, "numeric_column_", 15) == 0)
        { prefix = 15; count = 30; offset = 17; }
    else if (length > 12 && memcmp(name, "text_column_", 12) == 0)
        { prefix = 12; count = 17; offset = 47; }
    else return -1;
    if (name[prefix] == '0' || length - prefix > 2) return -1;
    for (i = prefix; i < length; i++)
    {
        if (name[i] < '0' || name[i] > '9') return -1;
        ordinal = ordinal * 10 + name[i] - '0';
    }
    return ordinal >= 1 && ordinal <= count ? offset + ordinal - 1 : -1;
}

static bool
bounded_number(const char *s, int length, int integer_digits, int fractional_digits,
               bool identifier)
{
    int i = 0, digits = 0, first;
    if (length == 0) return true;
    if (s[i] == '+' || s[i] == '-') i++;
    first = i;
    while (i < length && s[i] >= '0' && s[i] <= '9') { digits++; i++; }
    if (digits == 0 || digits > integer_digits) return false;
    if (identifier && digits == 10 && s[first] > '1') return false;
    if (i == length) return true;
    if (fractional_digits == 0 || s[i++] != '.') return false;
    digits = 0;
    while (i < length && s[i] >= '0' && s[i] <= '9') { digits++; i++; }
    return i == length && digits > 0 && digits <= fractional_digits;
}

static int
two_digits(const char *s)
{
    if (s[0] < '0' || s[0] > '9' || s[1] < '0' || s[1] > '9') return -1;
    return (s[0] - '0') * 10 + s[1] - '0';
}

static bool
bounded_stamp(const char *s, int length)
{
    static const int days[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    int year = 0, month, day, hour, minute, second, limit, i, fraction;
    if (length == 0) return true;
    if (length < 19) return false;
    for (i = 0; i < 4; i++)
    {
        if (s[i] < '0' || s[i] > '9') return false;
        year = year * 10 + s[i] - '0';
    }
    if (year == 0 || s[4] != '-' || s[7] != '-' ||
        (s[10] != 'T' && s[10] != ' ') || s[13] != ':' || s[16] != ':') return false;
    month = two_digits(s+5); day = two_digits(s+8); hour = two_digits(s+11);
    minute = two_digits(s+14); second = two_digits(s+17);
    if (month < 1 || month > 12 || day < 1 || hour < 0 || hour > 23 ||
        minute < 0 || minute > 59 || second < 0 || second > 59) return false;
    limit = days[month];
    if (month == 2 && year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) limit++;
    if (day > limit) return false;
    i = 19;
    if (i < length && s[i] == '.')
    {
        i++; fraction = 0;
        while (i < length && s[i] >= '0' && s[i] <= '9') { fraction++; i++; }
        if (fraction < 1 || fraction > 6) return false;
    }
    if (i == length) return true;
    if (s[i] == 'Z') return i+1 == length;
    if (s[i] != '+' && s[i] != '-') return false;
    i++;
    if (length-i < 2 || (hour = two_digits(s+i)) < 0 || hour > 12) return false;
    i += 2;
    if (i == length) return true;
    if (s[i] == ':') i++;
    return length-i == 2 && (minute = two_digits(s+i)) >= 0 && minute <= 59;
}

static bool
field_safe(int field, text *value)
{
    const char *s = VARDATA_ANY(value);
    int length = VARSIZE_ANY_EXHDR(value);
    if (field == 0) return bounded_stamp(s, length);
    if (field == 1) return bounded_number(s, length, 10, 0, true);
    if (field >= 2 && field < 17) return bounded_number(s, length, 4, 0, false);
    if (field >= 17 && field < 47) return bounded_number(s, length, 6, 3, false);
    return true;
}

static text *
value_text(JsonbValue *value)
{
    char *temporary;
    text *result;
    switch (value->type)
    {
        case jbvNull: return NULL;
        case jbvString:
            return cstring_to_text_with_len(value->val.string.val,value->val.string.len);
        case jbvBool: return cstring_to_text(value->val.boolean ? "true" : "false");
        case jbvNumeric:
            temporary = DatumGetCString(DirectFunctionCall1(numeric_out,NumericGetDatum(value->val.numeric)));
            break;
        case jbvBinary:
            temporary = JsonbToCString(NULL,value->val.binary.data,value->val.binary.len);
            break;
        default: elog(ERROR,"Unexpected JSONB token in isolated native probe"); return NULL;
    }
    result = cstring_to_text(temporary); pfree(temporary); return result;
}

Datum
challenge_probe_identity(PG_FUNCTION_ARGS)
{
    PG_RETURN_TEXT_P(cstring_to_text(CHALLENGE_PROBE_SOURCE_SHA));
}

Datum
challenge_extract64(PG_FUNCTION_ARGS)
{
    ProbeDescriptor *cache = fcinfo->flinfo->fn_extra;
    Datum values[FIELD_COUNT+1];
    bool nulls[FIELD_COUNT+1];
    bool safe = true;
    Jsonb *input = PG_ARGISNULL(0) ? NULL : PG_GETARG_JSONB_P(0);
    int i;
    if (cache == NULL)
    {
        TupleDesc supplied;
        MemoryContext previous = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
        if (get_call_result_type(fcinfo,NULL,&supplied) != TYPEFUNC_COMPOSITE || supplied->natts != FIELD_COUNT+1)
            ereport(ERROR,(errmsg("Native research probe requires 64 named TEXT columns and one proof BOOLEAN")));
        cache = palloc0(sizeof(*cache));
        cache->desc = BlessTupleDesc(CreateTupleDescCopy(supplied));
        for (i = 0; i < FIELD_COUNT; i++) cache->output_position[i] = -1;
        for (i = 0; i < FIELD_COUNT; i++)
        {
            Form_pg_attribute attribute = TupleDescAttr(cache->desc,i);
            const char *name = NameStr(attribute->attname);
            int field = field_index(name,strlen(name));
            if (attribute->attisdropped || attribute->atttypid != TEXTOID ||
                attribute->atttypmod != -1 || field < 0 || cache->output_position[field] != -1)
                ereport(ERROR,(errmsg("Native research probe forbids non-TEXT, unknown or duplicate output columns")));
            cache->output_position[field] = i;
        }
        if (TupleDescAttr(cache->desc,FIELD_COUNT)->atttypid != BOOLOID ||
            strcmp(NameStr(TupleDescAttr(cache->desc,FIELD_COUNT)->attname),"pipeline_domain_safe") != 0)
            ereport(ERROR,(errmsg("Native research probe proof column is invalid")));
        fcinfo->flinfo->fn_extra = cache;
        MemoryContextSwitchTo(previous);
    }
    memset(values,0,sizeof(values)); memset(nulls,true,sizeof(nulls));
    if (input != NULL && JB_ROOT_IS_OBJECT(input))
    {
        JsonbIterator *iterator = JsonbIteratorInit(&input->root);
        JsonbValue value;
        JsonbIteratorToken token;
        while ((token = JsonbIteratorNext(&iterator,&value,true)) != WJB_DONE)
        {
            if (token == WJB_KEY)
            {
                int field = field_index(value.val.string.val,value.val.string.len);
                token = JsonbIteratorNext(&iterator,&value,true);
                if (token != WJB_VALUE) elog(ERROR,"Unexpected JSONB object traversal token");
                if (field >= 0)
                {
                    text *converted = value_text(&value);
                    if (converted != NULL)
                    {
                        int position = cache->output_position[field];
                        values[position] = PointerGetDatum(converted); nulls[position] = false;
                        if (!field_safe(field,converted)) safe = false;
                    }
                }
            }
        }
    }
    values[FIELD_COUNT] = BoolGetDatum(safe); nulls[FIELD_COUNT] = false;
    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(cache->desc,values,nulls)));
}
