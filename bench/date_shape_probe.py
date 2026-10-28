"""Equivalent ASCII timestamp shape fast path with original-regex fallback."""
import hashlib
from pathlib import Path
import random
import sys
from bench import calendar_proof_probe as calendar,t1_inline_probe as inline,projection_pair
from pipeline.safe_domain import STAMP_SHAPE,literal


def source_identity():
    return hashlib.sha256((inline.source_identity()+Path(__file__).read_text(encoding='utf8')+Path(projection_pair.__file__).read_text(encoding='utf8')).encode()).hexdigest()


def shape():
    old='(c.stamp COLLATE "C") ~ '+literal(STAMP_SHAPE)
    masks=['0000-00-00 00:00:00'+fraction+'-00' for fraction in ('','.0','.00','.000')]
    def part(offset,length):return '(pg_catalog.substr(c.stamp,'+str(offset)+','+str(length)+') COLLATE "C")'
    # Global '+' translation also maps date separators, so raw positions 5/8
    # must still be '-' explicitly. T is allowed only where the mask has space.
    normalized="(pg_catalog.translate(c.stamp,'0123456789T+','0000000000 -') COLLATE \"C\")"
    conditions=[part(5,1)+"='-'",part(8,1)+"='-'",part(6,2)+" BETWEEN '01' AND '12'",part(9,2)+" BETWEEN '01' AND '31'",part(12,2)+"<='23'",part(15,2)+"<='59'",part(18,2)+"<='59'",'(pg_catalog.right(c.stamp,2) COLLATE "C")<=\'12\'']
    return '(CASE WHEN '+normalized+' IN ('+','.join(literal(mask) for mask in masks)+') THEN '+' AND '.join(conditions)+' ELSE '+old+' END)'


def projection(scopes,*,logged=False):
    body=calendar.projection(scopes,logged=logged)
    old='(c.stamp COLLATE "C") ~ '+literal(STAMP_SHAPE);assert body.count(old)==1
    return '-- lab equivalent date shape source_sha256='+source_identity()+'\n'+body.replace(old,shape(),1)


def differential(c):
    rng=random.Random(719377)
    values=['','0000-01-01 00:00:00-00','9999-12-31T23:59:59.999+12','2000+01-01 00:00:00-03','2000-01+01 00:00:00-03','2000-01-01T00:00:00+03','2000-01-01 00:00:00.123456-03:00','bad','中文',None]
    for _ in range(30000):
        values.append(str(rng.randrange(10000)).zfill(4)+rng.choice(('-','+','T',' '))+str(rng.randrange(20)).zfill(2)+rng.choice(('-','+'))+str(rng.randrange(40)).zfill(2)+rng.choice((' ','T','\t'))+str(rng.randrange(30)).zfill(2)+':'+str(rng.randrange(70)).zfill(2)+':'+str(rng.randrange(70)).zfill(2)+rng.choice(('','.0','.00','.000','.999','.0001','.',',1'))+rng.choice(('-','+'))+str(rng.randrange(20)).zfill(2))
    old='(c.stamp COLLATE "C") ~ '+literal(STAMP_SHAPE);different=0
    for offset in range(0,len(values),5000):
        different+=c.execute('SELECT count(*) FILTER(WHERE ('+old+') IS DISTINCT FROM ('+shape()+')) FROM unnest(%s::text[]) c(stamp)',(values[offset:offset+5000],)).fetchone()[0]
    assert different==0
    query="SELECT count(*),count(*) FILTER(WHERE ("+old+') IS DISTINCT FROM ('+shape()+")) FROM (SELECT '2024-04-01 '||lpad((n/3600)::text,2,'0')||':'||lpad(((n/60)%60)::text,2,'0')||':'||lpad((n%60)::text,2,'0')||'.123-03' AS stamp FROM generate_series(0,86399) n) c"
    total,diff=c.execute(query).fetchone();assert diff==0
    return {'random_malformed_and_boundary_vectors':len(values),'every_second_of_one_day':total,'shape_differences':different+diff,'scope':'Same shape predicate; original calendar validation remains. Fractions/offset layouts outside the fast mask execute the original regular expression.'}


def install():
    inline.install()
    import pipeline.parallel_normalization as parallel
    import pipeline.guarded_normalization as guarded
    parallel.prepare_input_sql=projection
    validation=guarded.validation_destinations
    guarded.validation_destinations=lambda destinations:validation(destinations)+[{'statements':[],'function_references':[('pg_catalog','translate')]}]


def main():
    if len(sys.argv)>1 and sys.argv[1]=='component':
        sys.argv.pop(1);projection_pair.component({'calendar':calendar.projection,'ascii_shape':projection},source_identity,'reports/date-shape-projection-1m.json',differential=differential);return
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
