/* Isolated research only. Pure JSONB argument traversal, no SPI/data access. */
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_collation_d.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/numeric.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(challenge_extract64);
PG_FUNCTION_INFO_V1(challenge_probe_identity);
PG_FUNCTION_INFO_V1(challenge_compact12);
PG_FUNCTION_INFO_V1(challenge_axis_rows);

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

/* Compact implementation: exact bounded fixed-point integers, never float. */
static bool
compact_number(JsonbValue *value, bool present, int digits, int fraction,
               bool identifier, int64 *output, bool *isnull)
{
    const char *s;
    char *temporary = NULL;
    int length, i, fractional = 0, places = 0;
    int64 whole = 0;
    bool negative, valid;
    *isnull = true; *output = 0;
    if (!present || value->type == jbvNull) return true;
    if (value->type == jbvString) { s = value->val.string.val; length = value->val.string.len; }
    else if (value->type == jbvNumeric)
    {
        temporary = DatumGetCString(DirectFunctionCall1(numeric_out,NumericGetDatum(value->val.numeric)));
        s = temporary; length = strlen(s);
    }
    else return false;
    valid = bounded_number(s,length,digits,fraction,identifier);
    if (valid && length > 0)
    {
        negative = s[0] == '-'; i = (s[0] == '-' || s[0] == '+') ? 1 : 0;
        while (i < length && s[i] != '.') { whole = whole * 10 + s[i] - '0'; i++; }
        if (fraction > 0)
        {
            if (i < length) i++;
            while (i < length) { fractional = fractional * 10 + s[i] - '0'; places++; i++; }
            while (places < 3) { fractional *= 10; places++; }
            whole = whole * 1000 + fractional;
        }
        *output = negative ? -whole : whole; *isnull = false;
    }
    if (temporary != NULL) pfree(temporary);
    return valid;
}

static text *
compact_dimension(JsonbValue *value, bool present, bool uppercase)
{
    text *raw, *trimmed;
    if (!present || value->type == jbvNull) return NULL;
    raw = value_text(value);
    if (VARSIZE_ANY_EXHDR(raw) == 0) return NULL;
    trimmed = DatumGetTextPP(DirectFunctionCall1(btrim1,PointerGetDatum(raw)));
    return uppercase ? DatumGetTextPP(DirectFunctionCall1Coll(upper,DEFAULT_COLLATION_OID,PointerGetDatum(trimmed))) : trimmed;
}

static int32
compact_kind(JsonbValue *value, bool present)
{
    const char *s;
    int length, i;
    text *normalized;
    if (!present || value->type != jbvString) return 0;
    s = value->val.string.val; length = value->val.string.len;
    while (length > 0 && *s == ' ') { s++; length--; }
    while (length > 0 && s[length-1] == ' ') length--;
    if (length == 1 && (unsigned char)s[0] < 128)
    {
        char kind = s[0];
        if (kind == 'x' || kind == 'y' || kind == 'z') kind -= 'a'-'A';
        return kind == 'X' || kind == 'Y' || kind == 'Z' ? kind : 0;
    }
    for (i = 0; i < length; i++)
        if ((unsigned char)s[i] >= 128)
        {
            normalized = compact_dimension(value,true,true);
            if (normalized != NULL && VARSIZE_ANY_EXHDR(normalized) == 1)
            {
                char kind = VARDATA_ANY(normalized)[0];
                if (kind == 'X' || kind == 'Y' || kind == 'Z') return kind;
            }
            break;
        }
    return 0;
}

Datum
challenge_compact12(PG_FUNCTION_ARGS)
{
    static const char *names[] = {"pipeline_domain_safe","pipeline_identifier","pipeline_stamp","pipeline_axis_max","pipeline_numeric_1","pipeline_numeric_2","pipeline_integer_1","text_column_1","text_column_2","text_column_3","pipeline_axes","pipeline_axis_rows"};
    static const Oid types[] = {BOOLOID,INT4OID,TEXTOID,INT2OID,INT4OID,INT4OID,INT2OID,TEXTOID,TEXTOID,TEXTOID,BYTEAOID,INT2OID};
    TupleDesc desc = fcinfo->flinfo->fn_extra;
    Jsonb *input = PG_ARGISNULL(0) ? NULL : PG_GETARG_JSONB_P(0);
    JsonbValue fields[FIELD_COUNT];
    bool present[FIELD_COUNT];
    Datum result[12];
    unsigned char packed[14*10];
    bool nulls[12], integer_nulls[15], numeric_nulls[30], identifier_null;
    int64 integers[15], numerics[30], identifier, maximum = 0;
    bool safe = true, maximum_null = true;
    int i, count = 0;
    bytea *axes;
    if (desc == NULL)
    {
        TupleDesc supplied;
        MemoryContext previous = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
        if (get_call_result_type(fcinfo,NULL,&supplied) != TYPEFUNC_COMPOSITE || supplied->natts != 12)
            ereport(ERROR,(errmsg("Compact native research probe requires its exact 12-column descriptor")));
        desc = BlessTupleDesc(CreateTupleDescCopy(supplied));
        for (i = 0; i < 12; i++)
        {
            Form_pg_attribute attribute = TupleDescAttr(desc,i);
            if (attribute->attisdropped || attribute->atttypid != types[i] || attribute->atttypmod != -1 ||
                strcmp(NameStr(attribute->attname),names[i]) != 0)
                ereport(ERROR,(errmsg("Compact native research output descriptor is invalid")));
        }
        fcinfo->flinfo->fn_extra = desc; MemoryContextSwitchTo(previous);
    }
    memset(present,false,sizeof(present)); memset(result,0,sizeof(result)); memset(nulls,true,sizeof(nulls));
    if (input != NULL && JB_ROOT_IS_OBJECT(input))
    {
        JsonbIterator *iterator = JsonbIteratorInit(&input->root);
        JsonbValue value;
        JsonbIteratorToken token;
        while ((token = JsonbIteratorNext(&iterator,&value,true)) != WJB_DONE)
            if (token == WJB_KEY)
            {
                int field = field_index(value.val.string.val,value.val.string.len);
                token = JsonbIteratorNext(&iterator,&value,true);
                if (token != WJB_VALUE) elog(ERROR,"Unexpected compact JSONB object token");
                if (field >= 0) { fields[field] = value; present[field] = true; }
            }
    }
    if (!compact_number(&fields[1],present[1],10,0,true,&identifier,&identifier_null)) { safe = false; goto finish; }
    for (i = 0; i < 15; i++)
        if (!compact_number(&fields[2+i],present[2+i],4,0,false,&integers[i],&integer_nulls[i])) { safe = false; goto finish; }
    for (i = 0; i < 30; i++)
        if (!compact_number(&fields[17+i],present[17+i],6,3,false,&numerics[i],&numeric_nulls[i])) { safe = false; goto finish; }
    if (present[0] && fields[0].type != jbvNull)
    {
        if (fields[0].type != jbvString || !bounded_stamp(fields[0].val.string.val,fields[0].val.string.len)) { safe = false; goto finish; }
        if (fields[0].val.string.len > 0) { result[2] = PointerGetDatum(value_text(&fields[0])); nulls[2] = false; }
    }
    if (!identifier_null) { result[1] = Int32GetDatum((int32)identifier); nulls[1] = false; }
    for (i = 1; i < 15; i++)
        if (!integer_nulls[i] && (maximum_null || integers[i] > maximum)) { maximum = integers[i]; maximum_null = false; }
    if (!maximum_null) { result[3] = Int16GetDatum((int16)maximum); nulls[3] = false; }
    for (i = 0; i < 2; i++)
        if (!numeric_nulls[i] && numerics[i] > 0) { result[4+i] = Int32GetDatum((int32)numerics[i]); nulls[4+i] = false; }
    if (!integer_nulls[0]) { result[6] = Int16GetDatum((int16)integers[0]); nulls[6] = false; }
    for (i = 0; i < 3; i++)
    {
        text *dimension = compact_dimension(&fields[47+i],present[47+i],i>0);
        if (dimension != NULL) { result[7+i] = PointerGetDatum(dimension); nulls[7+i] = false; }
    }
    for (i = 1; i <= 14 && i <= maximum; i++)
    {
        int32 kind;
        if (integer_nulls[i] || integers[i] != i || numeric_nulls[2*i] || numerics[2*i] < 0) continue;
        kind = compact_kind(&fields[49+i],present[49+i]);
        if (kind == 0) continue;
        uint32 weight = (uint32)numerics[2*i];
        uint32 gap = !numeric_nulls[2*i+1] && numerics[2*i+1] > 0 ? (uint32)numerics[2*i+1] : 0;
        unsigned char *row = packed+10*count;
        row[0] = (unsigned char)i; row[1] = (unsigned char)kind;
        row[2] = weight >> 24; row[3] = weight >> 16; row[4] = weight >> 8; row[5] = weight;
        row[6] = gap >> 24; row[7] = gap >> 16; row[8] = gap >> 8; row[9] = gap;
        count++;
    }
    axes = palloc(VARHDRSZ+count*10); SET_VARSIZE(axes,VARHDRSZ+count*10);
    memcpy(VARDATA(axes),packed,count*10);
    result[10] = PointerGetDatum(axes); nulls[10] = false;
    result[11] = Int16GetDatum(count); nulls[11] = false;
finish:
    result[0] = BoolGetDatum(safe); nulls[0] = false;
    PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(desc,result,nulls)));
}

typedef struct AxisRows
{
    int count;
    unsigned char rows[14*10];
} AxisRows;

static uint32
axis_u32(const unsigned char *bytes)
{
    return ((uint32)bytes[0] << 24) | ((uint32)bytes[1] << 16) | ((uint32)bytes[2] << 8) | bytes[3];
}

Datum
challenge_axis_rows(PG_FUNCTION_ARGS)
{
    FuncCallContext *context;
    AxisRows *data;
    if (SRF_IS_FIRSTCALL())
    {
        static const char *names[] = {"position_number","weight","axis_kind","gap"};
        static const Oid types[] = {INT2OID,INT4OID,TEXTOID,INT4OID};
        TupleDesc supplied;
        bytea *input = PG_ARGISNULL(0) ? NULL : PG_GETARG_BYTEA_PP(0);
        int length = input == NULL ? 0 : VARSIZE_ANY_EXHDR(input), i;
        MemoryContext previous;
        if (length > 14*10 || length % 10 != 0)
            ereport(ERROR,(errmsg("Native axis frame length is invalid")));
        context = SRF_FIRSTCALL_INIT(); previous = MemoryContextSwitchTo(context->multi_call_memory_ctx);
        if (get_call_result_type(fcinfo,NULL,&supplied) != TYPEFUNC_COMPOSITE || supplied->natts != 4)
            ereport(ERROR,(errmsg("Native axis rows require the exact four-column descriptor")));
        context->tuple_desc = BlessTupleDesc(CreateTupleDescCopy(supplied));
        for (i = 0; i < 4; i++)
            if (TupleDescAttr(context->tuple_desc,i)->atttypid != types[i] ||
                TupleDescAttr(context->tuple_desc,i)->atttypmod != -1 ||
                strcmp(NameStr(TupleDescAttr(context->tuple_desc,i)->attname),names[i]) != 0)
                ereport(ERROR,(errmsg("Native axis row descriptor is invalid")));
        data = palloc0(sizeof(*data)); data->count = length/10;
        if (length > 0) memcpy(data->rows,VARDATA_ANY(input),length);
        for (i = 0; i < data->count; i++)
        {
            const unsigned char *row = data->rows+10*i;
            if (row[0] < 1 || row[0] > 14 || (row[1] != 'X' && row[1] != 'Y' && row[1] != 'Z') ||
                axis_u32(row+2) > 999999999 || axis_u32(row+6) > 999999999 ||
                (i > 0 && row[0] <= data->rows[10*(i-1)]))
                ereport(ERROR,(errmsg("Native axis frame payload is invalid")));
        }
        context->user_fctx = data; context->max_calls = data->count;
        MemoryContextSwitchTo(previous);
    }
    context = SRF_PERCALL_SETUP(); data = context->user_fctx;
    if (context->call_cntr < context->max_calls)
    {
        const unsigned char *row = data->rows+10*context->call_cntr;
        Datum values[4]; bool nulls[4] = {false,false,false,false};
        uint32 gap = axis_u32(row+6);
        values[0] = Int16GetDatum((int16)row[0]); values[1] = Int32GetDatum((int32)axis_u32(row+2));
        values[2] = PointerGetDatum(cstring_to_text_with_len((const char *)row+1,1));
        values[3] = Int32GetDatum((int32)gap); nulls[3] = gap == 0;
        SRF_RETURN_NEXT(context,HeapTupleGetDatum(heap_form_tuple(context->tuple_desc,values,nulls)));
    }
    SRF_RETURN_DONE(context);
}
