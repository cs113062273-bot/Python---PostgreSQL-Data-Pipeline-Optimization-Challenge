"""Native per-value trim/case reuse, rollback-only projection research."""
import hashlib
from pathlib import Path
from bench import calendar_proof_probe as calendar,projection_pair
from pipeline.recipes import EXTRACT,destinations


def source_identity():
    return hashlib.sha256((calendar.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def projection(scopes,*,logged=False,group_size=1):
    if group_size not in (1,2,4):raise ValueError(group_size)
    body=calendar.projection(scopes,logged=logged)
    fields=sorted({key for d in destinations() for key in EXTRACT.findall(d['sql'])})
    texts=[field for field in fields if field.startswith('text_column_')]
    aliases={};joins=[]
    for offset in range(0,len(texts),group_size):
        group=texts[offset:offset+group_size];expressions=[];alias='text_memo_'+str(offset)
        for field in group:
            original="p.payload->>'"+field+"'";normalized='pg_catalog.btrim('+original+')'
            expression='pg_catalog.btrim(n.'+field+')'
            if field!='text_column_1':normalized='pg_catalog.upper('+normalized+')';expression='pg_catalog.upper('+expression+')'
            assert body.count(normalized)==1
            body=body.replace(normalized,original,1);expressions.append(expression);aliases[field]=alias+'.'+field
        if len(group)==1:
            joins.append('CROSS JOIN LATERAL pg_catalog.unnest(pg_catalog.array_append(ARRAY[]::text[],'+expressions[0]+')) '+alias+'('+group[0]+')')
        else:
            joins.append('CROSS JOIN LATERAL pg_catalog.unnest(pg_catalog.array_append(ARRAY[]::record[],ROW('+','.join(expressions)+'))) '+alias+'('+','.join(field+' text' for field in group)+')')
    assert body.count('SELECT s.raw_id,n.*,')==1
    selected=','.join(aliases.get(field,'n.'+field)+' AS '+field for field in fields)
    body=body.replace('SELECT s.raw_id,n.*,','SELECT s.raw_id,'+selected+',',1)
    assert body.endswith(';')
    return '-- lab native normalized text Memoize group='+str(group_size)+' source_sha256='+source_identity()+'\n'+body[:-1]+'\n'+'\n'.join(joins)+';'


def main():
    projection_pair.component({'original':calendar.projection,'scalar':lambda scopes,logged=False:projection(scopes,logged=logged,group_size=1),'pairs':lambda scopes,logged=False:projection(scopes,logged=logged,group_size=2),'quartets':lambda scopes,logged=False:projection(scopes,logged=logged,group_size=4)},source_identity,'reports/text-memo-projection-100k.json')


if __name__=='__main__':main()
