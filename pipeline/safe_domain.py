"""A sufficient, data-independent domain in which all recipe casts are safe.

This is not a validator that rejects inputs: outside this conservative domain,
the caller executes the exact original pipeline with its original constraints.
Bounds follow PostgreSQL integer ranges, not generator values or its seed.
"""
INTEGER=r"[+-]?[0-9]{1,4}"
NUMERIC=r"[+-]?[0-9]{1,6}(?:[.][0-9]{1,3})?"
IDENTIFIER=r"[+-]?(?:[0-9]{1,9}|[01][0-9]{9})"
YEAR=r"(000[1-9]|00[1-9][0-9]|0[1-9][0-9]{2}|[1-9][0-9]{3})"
LEAP=r"([0-9]{2}(0[48]|[2468][048]|[13579][26])|(0[48]|[2468][048]|[13579][26])00)"
DATE="("+YEAR+r"-((0[1-9]|1[0-2])-(0[1-9]|1[0-9]|2[0-8])|(0[13-9]|1[0-2])-(29|30)|(0[13578]|1[02])-31)|"+LEAP+r"-02-29)"
STAMP=DATE+r"[ T]([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]([.][0-9]{1,6})?(Z|[+-](0[0-9]|1[0-2])(:?[0-5][0-9])?)?"
STAMP_SHAPE=r"\A[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])[ T](?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:[.][0-9]{1,6})?(?:Z|[+-](?:0[0-9]|1[0-2])(?::?[0-5][0-9])?)?\Z"


def literal(pattern):return "E'"+pattern.replace("\\","\\\\").replace("'","''")+"'"


def _bundle(prefix,count):
    # Immutable TEXT concatenation; polymorphic concat is STABLE. Balance the
    # tree so temporary allocations grow logarithmically rather than linearly.
    parts=[f"coalesce(c.{prefix}_{n},'')" for n in range(1,count+1)]
    def balanced(items):
        if len(items)==1:return items[0]
        middle=len(items)//2
        return "("+balanced(items[:middle])+"||'|'||"+balanced(items[middle:])+")"
    return balanced(parts)


def condition_parts():
    def bundle(name,count,unit):
        pattern=r"\A(?:"+unit+r")?(?:[|](?:"+unit+r")?)*\Z"
        # The cyclic RE has a small state machine. Count separators separately
        # so a literal '|' inside an input can never masquerade as two numbers.
        delimiters=f"pg_catalog.octet_length(c.{name})-pg_catalog.octet_length(pg_catalog.replace(c.{name},'|',''))={count-1}"
        return "("+delimiters+f" AND (c.{name} COLLATE \"C\") ~ {literal(pattern)})"
    year="pg_catalog.substr(c.stamp,1,4)::integer"
    month="pg_catalog.substr(c.stamp,6,2)"
    day="pg_catalog.substr(c.stamp,9,2)::integer"
    # CASE guards the short bounded integer casts. Calendar arithmetic covers
    # every Gregorian month/leap year without a large alternation automaton.
    calendar=f"""CASE WHEN c.stamp='' THEN true WHEN (c.stamp COLLATE "C") ~ {literal(STAMP_SHAPE)} THEN
      {year}>0 AND {day}<=CASE WHEN {month}='02' THEN 28+CASE WHEN {year}%4=0 AND ({year}%100<>0 OR {year}%400=0) THEN 1 ELSE 0 END
      WHEN {month} IN ('04','06','09','11') THEN 30 ELSE 31 END ELSE false END"""
    return [
        bundle("integers",15,INTEGER),bundle("numerics",30,NUMERIC),
        '(c.identifier COLLATE "C") ~ '+literal(r"\A(?:"+IDENTIFIER+r")?\Z"),
        "("+calendar+")",
    ]


def condition_sql():return " AND ".join(condition_parts())


def projection_sql():
    projection=_bundle('integer_column',15)+" AS integers,"+_bundle('numeric_column',30)+" AS numerics,coalesce(c.id_column_1,'') AS identifier,coalesce(c.datetime_column_1,'') AS stamp"
    return "SELECT "+projection+" FROM pg_temp.pipeline_normalized_input c OFFSET 0"


def gate_sql():
    return "SELECT coalesce(pg_catalog.bool_and("+condition_sql()+"),true) AS safe FROM ("+projection_sql()+") c"
