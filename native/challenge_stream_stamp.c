/* Research normalizer: fused JSON value walk and single-pass numeric proof.
 * No row values or query results are cached; all current input bytes are checked. */
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "access/htup_details.h"
#include "catalog/pg_type_d.h"
#include "catalog/pg_collation_d.h"
#include "datatype/timestamp.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/numeric.h"
#include "nodes/miscnodes.h"
#include "pgtime.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1(challenge_extract64);
PG_FUNCTION_INFO_V1(challenge_probe_identity);
PG_FUNCTION_INFO_V1(challenge_compact13);
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
    char *temporary=NULL;
    int length,i=0,whole_digits=0,places=0,first;
    int64 whole=0,fractional=0;
    bool negative=false,valid=false;
    *isnull=true;*output=0;
    if(!present || value->type==jbvNull) return true;
    if(value->type==jbvString) { s=value->val.string.val;length=value->val.string.len; }
    else if(value->type==jbvNumeric)
    {
        temporary=DatumGetCString(DirectFunctionCall1(numeric_out,NumericGetDatum(value->val.numeric)));
        s=temporary;length=strlen(s);
    }
    else return false;
    if(length==0) { valid=true;goto done; }
    if(s[i]=='+' || s[i]=='-') { negative=s[i]=='-';i++; }
    first=i;
    while(i<length && s[i]>='0' && s[i]<='9')
    {
        if(++whole_digits>digits) goto done;
        whole=whole*10+s[i]-'0';i++;
    }
    if(whole_digits==0 || (identifier && whole_digits==10 && s[first]>'1')) goto done;
    if(i<length)
    {
        if(fraction==0 || s[i++]!='.') goto done;
        while(i<length && s[i]>='0' && s[i]<='9')
        {
            if(++places>fraction) goto done;
            fractional=fractional*10+s[i]-'0';i++;
        }
        if(places==0 || i!=length) goto done;
    }
    if(fraction>0)
    {
        while(places<3) { fractional*=10;places++; }
        whole=whole*1000+fractional;
    }
    *output=negative?-whole:whole;*isnull=false;valid=true;
done:
    if(temporary!=NULL) pfree(temporary);
    return valid;
}

static text *
compact_dimension(JsonbValue *value, bool present, bool uppercase)
{
    text *raw, *trimmed;
    if (!present || value->type == jbvNull) return NULL;
    if (value->type == jbvString && value->val.string.len > 0)
    {
        const unsigned char *s = (const unsigned char *)value->val.string.val;
        int length = value->val.string.len, i;
        bool plain = s[0] != ' ' && s[length-1] != ' ';
        for (i = 0; plain && i < length; i++)
            plain = s[i] < 128 && !(uppercase && s[i] >= 'a' && s[i] <= 'z');
        if (plain) return cstring_to_text_with_len((const char *)s,length);
    }
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

typedef struct ParsedNumbers
{
    int64 identifier,integers[15],numerics[30];
    bool identifier_null,integer_nulls[15],numeric_nulls[30];
} ParsedNumbers;

static bool
compact_ascii(const char *s,int length,int digits,int fraction,bool identifier,int64 *output,bool *isnull)
{
    int i=0,whole_digits=0,places=0,first;
    int64 whole=0,fractional=0;
    bool negative=false,valid=false;
    *isnull=true;*output=0;
    if(length==0) { valid=true;goto done; }
    if(s[i]=='+' || s[i]=='-') { negative=s[i]=='-';i++; }
    first=i;
    while(i<length && s[i]>='0' && s[i]<='9')
    {
        if(++whole_digits>digits) goto done;
        whole=whole*10+s[i]-'0';i++;
    }
    if(whole_digits==0 || (identifier && whole_digits==10 && s[first]>'1')) goto done;
    if(i<length)
    {
        if(fraction==0 || s[i++]!='.') goto done;
        while(i<length && s[i]>='0' && s[i]<='9')
        {
            if(++places>fraction) goto done;
            fractional=fractional*10+s[i]-'0';i++;
        }
        if(places==0 || i!=length) goto done;
    }
    if(fraction>0)
    {
        while(places<3) { fractional*=10;places++; }
        whole=whole*1000+fractional;
    }
    *output=negative?-whole:whole;*isnull=false;valid=true;
done:
    return valid;
}

/* Every cache hit validates the entire current key metadata and all key bytes.
 * Only key-to-field positions are reused, never values or proof results. */
#define LAYOUT_KEYS 256
#define LAYOUT_BYTES 8192
typedef struct FieldLayout
{
    bool valid;
    uint32 count,bytes;
    JEntry entries[LAYOUT_KEYS];
    int fields[LAYOUT_KEYS];
    char keys[LAYOUT_BYTES];
} FieldLayout;

/* 1: complete numeric proof, 0: unsafe number, -1: generic iterator needed. */
static int
fast_stream(JsonbContainer *container,JsonbValue *fields,bool *present,FieldLayout *layout,ParsedNumbers *numbers)
{
    uint32 count=JsonContainerSize(container),offset=0,i;
    const JEntry *entries=container->children;
    const char *base=(const char *)(entries+count*2);
    int key_field[LAYOUT_KEYS];
    const int *mapping=key_field;
    numbers->identifier_null=true;
    memset(numbers->integer_nulls,true,sizeof(numbers->integer_nulls));
    memset(numbers->numeric_nulls,true,sizeof(numbers->numeric_nulls));
    if(count>LAYOUT_KEYS) return -1;
    if(layout->valid && count==layout->count &&
       memcmp(entries,layout->entries,count*sizeof(JEntry))==0 &&
       memcmp(base,layout->keys,layout->bytes)==0)
    {
        offset=layout->bytes;mapping=layout->fields;
    }
    else
    {
        for(i=0;i<count;i++)
        {
            uint32 end=JBE_HAS_OFF(entries[i])?JBE_OFFLENFLD(entries[i]):offset+JBE_OFFLENFLD(entries[i]);
            key_field[i]=field_index(base+offset,end-offset);offset=end;
        }
        layout->valid=offset<=LAYOUT_BYTES;
        if(layout->valid)
        {
            layout->count=count;layout->bytes=offset;
            memcpy(layout->entries,entries,count*sizeof(JEntry));
            memcpy(layout->fields,key_field,count*sizeof(int));
            memcpy(layout->keys,base,offset);
            mapping=layout->fields;
        }
    }
    for(i=0;i<count;i++)
    {
        JEntry entry=entries[count+i];
        uint32 end=JBE_HAS_OFF(entry)?JBE_OFFLENFLD(entry):offset+JBE_OFFLENFLD(entry);
        int field=mapping[i];
        if(field>=0)
        {
            if(JBE_ISSTRING(entry))
            {
                const char *value=base+offset;
                int length=end-offset;
                if(field>=1 && field<47)
                {
                    bool valid;
                    /* Missing/null/empty numbers share the original NULL
                     * semantics; avoid building 45 temporary JsonbValues. */
                    if(length==0) { offset=end;continue; }
                    if(field==1) valid=compact_ascii(value,length,10,0,true,&numbers->identifier,&numbers->identifier_null);
                    else if(field<17) valid=compact_ascii(value,length,4,0,false,&numbers->integers[field-2],&numbers->integer_nulls[field-2]);
                    else valid=compact_ascii(value,length,6,3,false,&numbers->numerics[field-17],&numbers->numeric_nulls[field-17]);
                    if(!valid) return 0;
                }
                else
                {
                    fields[field].type=jbvString;fields[field].val.string.val=(char *)value;
                    fields[field].val.string.len=length;present[field]=true;
                }
            }
            else if(JBE_ISNULL(entry))
            {
                if(field==0 || field>=47) { fields[field].type=jbvNull;present[field]=true; }
            }
            else return -1;
        }
        offset=end;
    }
    return 1;
}

#define DAY_SLOTS 512
typedef struct DayMemo { int32 key; bool fixed; } DayMemo;
typedef struct CompactCache
{
    TupleDesc desc;
    DayMemo days[DAY_SLOTS];
    FieldLayout layout;
} CompactCache;

static bool
native_local(const char *text_value, Timestamp expected)
{
    ErrorSaveContext errors = {T_ErrorSaveContext};
    Datum parsed;
    if (!DirectInputFunctionCallSafe(timestamptz_in,text_value,InvalidOid,-1,(Node*)&errors,&parsed)) return false;
    return DatumGetTimestamp(DirectFunctionCall2(timestamptz_zone,CStringGetTextDatum("America/Sao_Paulo"),parsed))==expected;
}

/* Native first encounter per day: both day ends prove the -03 offset. */
static bool
day_fixed(CompactCache *cache, int year, int month, int day, Timestamp midnight)
{
    int32 key=year*10000+month*100+day;
    int slot=(uint32)key%DAY_SLOTS,probe;
    char start[32],end[40];
    bool fixed;
    for(probe=0;probe<16;probe++)
    {
        DayMemo *memo=&cache->days[(slot+probe)%DAY_SLOTS];
        if(memo->key==key) return memo->fixed;
        if(memo->key==0)
        {
            snprintf(start,sizeof(start),"%04d-%02d-%02d 00:00:00-03",year,month,day);
            snprintf(end,sizeof(end),"%04d-%02d-%02d 23:59:59.999999-03",year,month,day);
            fixed=native_local(start,midnight) && native_local(end,midnight+USECS_PER_DAY-1);
            memo->key=key;memo->fixed=fixed;return fixed;
        }
    }
    return false;
}

static bool
fast_stamp(CompactCache *cache, const char *s, int length, Timestamp *output)
{
    int year,month,day,hour,minute,second,i,digits=0;
    int64 fraction=0;
    Timestamp midnight;
    if(length<22 || s[10]!=' ' || s[length-3]!='-' || s[length-2]!='0' || s[length-1]!='3') return false;
    year=(s[0]-'0')*1000+(s[1]-'0')*100+(s[2]-'0')*10+s[3]-'0';
    month=two_digits(s+5);day=two_digits(s+8);hour=two_digits(s+11);minute=two_digits(s+14);second=two_digits(s+17);
    if(length>22)
    {
        if(s[19]!='.') return false;
        for(i=20;i<length-3;i++) { fraction=fraction*10+s[i]-'0';digits++; }
        if(digits<1 || digits>6) return false;
        for(;digits<6;digits++) fraction*=10;
    }
    midnight=(Timestamp)(date2j(year,month,day)-POSTGRES_EPOCH_JDATE)*USECS_PER_DAY;
    if(!day_fixed(cache,year,month,day,midnight)) return false;
    *output=midnight+hour*USECS_PER_HOUR+minute*USECS_PER_MINUTE+second*USECS_PER_SEC+fraction;
    return true;
}

Datum
challenge_compact13(PG_FUNCTION_ARGS)
{
    static const char *names[] = {"pipeline_domain_safe","pipeline_identifier","pipeline_native_flags","pipeline_axis_max","pipeline_numeric_1","pipeline_numeric_2","pipeline_integer_1","text_column_1","text_column_2","text_column_3","pipeline_axes","pipeline_axis_rows","pipeline_local_stamp"};
    static const Oid types[] = {BOOLOID,INT4OID,INT2OID,INT2OID,INT4OID,INT4OID,INT2OID,TEXTOID,TEXTOID,TEXTOID,BYTEAOID,INT2OID,TIMESTAMPOID};
    CompactCache *cache = fcinfo->flinfo->fn_extra;
    TupleDesc desc = cache == NULL ? NULL : cache->desc;
    Jsonb *input = PG_ARGISNULL(0) ? NULL : PG_GETARG_JSONB_P(0);
    const char *stamp_text = NULL;
    int stamp_length = 0;
    JsonbValue fields[FIELD_COUNT];
    bool present[FIELD_COUNT];
    Datum result[13];
    unsigned char packed[14*10];
    bool nulls[13];
    ParsedNumbers numbers;
    int parsed_status=-1;
    int64 maximum=0;
    bool safe = true, maximum_null = true;
    int i, count = 0;
    int16 flags = 3;
    bytea *axes;
    if (desc == NULL)
    {
        TupleDesc supplied;
        MemoryContext previous = MemoryContextSwitchTo(fcinfo->flinfo->fn_mcxt);
        if (get_call_result_type(fcinfo,NULL,&supplied) != TYPEFUNC_COMPOSITE || supplied->natts != 13)
            ereport(ERROR,(errmsg("Compact native research probe requires its exact 13-column descriptor")));
        desc = BlessTupleDesc(CreateTupleDescCopy(supplied));
        for (i = 0; i < 13; i++)
        {
            Form_pg_attribute attribute = TupleDescAttr(desc,i);
            if (attribute->attisdropped || attribute->atttypid != types[i] || attribute->atttypmod != -1 ||
                strcmp(NameStr(attribute->attname),names[i]) != 0)
                ereport(ERROR,(errmsg("Compact native research output descriptor is invalid")));
        }
        cache = palloc0(sizeof(*cache)); cache->desc = desc;
        fcinfo->flinfo->fn_extra = cache; MemoryContextSwitchTo(previous);
    }
    memset(present,false,sizeof(present)); memset(result,0,sizeof(result)); memset(nulls,true,sizeof(nulls));
    if(input!=NULL && JB_ROOT_IS_OBJECT(input))
    {
        parsed_status=fast_stream(&input->root,fields,present,&cache->layout,&numbers);
        if(parsed_status==0) { safe=false;goto finish; }
    }
    if(input!=NULL && JB_ROOT_IS_OBJECT(input) && parsed_status<0)
    {
        JsonbIterator *iterator = JsonbIteratorInit(&input->root);
        JsonbValue value;
        JsonbIteratorToken token;
        memset(present,false,sizeof(present));
        while ((token = JsonbIteratorNext(&iterator,&value,true)) != WJB_DONE)
            if (token == WJB_KEY)
            {
                int field = field_index(value.val.string.val,value.val.string.len);
                token = JsonbIteratorNext(&iterator,&value,true);
                if (token != WJB_VALUE) elog(ERROR,"Unexpected compact JSONB object token");
                if (field >= 0) { fields[field] = value; present[field] = true; }
            }
    }
    if(parsed_status<0)
    {
    if (!compact_number(&fields[1],present[1],10,0,true,&numbers.identifier,&numbers.identifier_null)) { safe = false; goto finish; }
    for (i = 0; i < 15; i++)
        if (!compact_number(&fields[2+i],present[2+i],4,0,false,&numbers.integers[i],&numbers.integer_nulls[i])) { safe = false; goto finish; }
    for (i = 0; i < 30; i++)
        if (!compact_number(&fields[17+i],present[17+i],6,3,false,&numbers.numerics[i],&numbers.numeric_nulls[i])) { safe = false; goto finish; }
    }
    if (present[0] && fields[0].type != jbvNull)
    {
        if (fields[0].type != jbvString || !bounded_stamp(fields[0].val.string.val,fields[0].val.string.len)) { safe = false; goto finish; }
        if (fields[0].val.string.len > 0) { stamp_text = fields[0].val.string.val; stamp_length = fields[0].val.string.len; }
    }
    if (!numbers.identifier_null) { result[1] = Int32GetDatum((int32)numbers.identifier); nulls[1] = false; }
    for (i = 1; i < 15; i++)
        if (!numbers.integer_nulls[i] && (maximum_null || numbers.integers[i] > maximum)) { maximum = numbers.integers[i]; maximum_null = false; }
    if (!maximum_null) { result[3] = Int16GetDatum((int16)maximum); nulls[3] = false; }
    for (i = 0; i < 2; i++)
        if (!numbers.numeric_nulls[i] && numbers.numerics[i] > 0) { result[4+i] = Int32GetDatum((int32)numbers.numerics[i]); nulls[4+i] = false; }
    if (!numbers.integer_nulls[0]) { result[6] = Int16GetDatum((int16)numbers.integers[0]); nulls[6] = false; }
    for (i = 0; i < 3; i++)
    {
        text *dimension = compact_dimension(&fields[47+i],present[47+i],i>0);
        if (dimension != NULL) { result[7+i] = PointerGetDatum(dimension); nulls[7+i] = false; }
    }
    /* Bit 0: main name over its byte bound. Bit 1: details code outside ''/^[A-Z]{2}$. */
    flags = 0;
    if (!nulls[7] && VARSIZE_ANY_EXHDR(DatumGetTextPP(result[7])) > 128) flags |= 1;
    if (!nulls[9])
    {
        text *code = DatumGetTextPP(result[9]);
        const char *s = VARDATA_ANY(code);
        int length = VARSIZE_ANY_EXHDR(code);
        if (length != 0 && (length != 2 || s[0] < 'A' || s[0] > 'Z' || s[1] < 'A' || s[1] > 'Z')) flags |= 2;
    }
    for (i = 1; i <= 14 && i <= maximum; i++)
    {
        int32 kind;
        if (numbers.integer_nulls[i] || numbers.integers[i] != i || numbers.numeric_nulls[2*i] || numbers.numerics[2*i] < 0) continue;
        kind = compact_kind(&fields[49+i],present[49+i]);
        if (kind == 0) continue;
        uint32 weight = (uint32)numbers.numerics[2*i];
        uint32 gap = !numbers.numeric_nulls[2*i+1] && numbers.numerics[2*i+1] > 0 ? (uint32)numbers.numerics[2*i+1] : 0;
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
    if (stamp_text != NULL)
    {
        ErrorSaveContext errors = {T_ErrorSaveContext};
        Datum parsed;
        Timestamp local;
        char *stamp;
        if (fast_stamp(cache,stamp_text,stamp_length,&local))
        {
            result[12]=TimestampGetDatum(local);nulls[12]=false;goto finish;
        }
        stamp=pnstrdup(stamp_text,stamp_length);
        if (pg_tzset("America/Sao_Paulo")==NULL ||
            !DirectInputFunctionCallSafe(timestamptz_in,stamp,InvalidOid,-1,(Node*)&errors,&parsed))
        { pfree(stamp);safe=false;goto finish; }
        result[12]=DirectFunctionCall2(timestamptz_zone,CStringGetTextDatum("America/Sao_Paulo"),parsed);
        nulls[12]=false;pfree(stamp);
    }
finish:
    result[0] = BoolGetDatum(safe); nulls[0] = false;
    result[2] = Int16GetDatum(flags); nulls[2] = false;
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
