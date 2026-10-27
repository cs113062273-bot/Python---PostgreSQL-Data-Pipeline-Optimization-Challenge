"""Independent original-vs-native batches at compact encoding boundaries."""
from bench.compact_axis_probe import install


def boundary_cases(seed,count):
    for position in range(1,15):
        for kind in ('X','Y','Z'):
            yield {'name':'axis-position-type-and-maximum-safe-numeric','edits':[
                (f'integer_column_{position+1}',str(position)),
                (f'text_column_{position+3}',kind),
                (f'numeric_column_{2*position+1}','999999.999'),
                (f'numeric_column_{2*position+2}','999999.999')
            ]}
    for weight,gap in [('0','0'),('-0','-0'),('0.001','0.001'),('999999.998','-999999.999'),('999999.999',None)]:
        yield {'name':'axis-zero-minimum-and-null-gap','edits':[
            ('integer_column_15','14'),('text_column_17',' z '),('numeric_column_29',weight),('numeric_column_30',gap)
        ]}
    edits=[]
    for position in range(1,15):
        edits.extend([(f'integer_column_{position+1}',str(position)),(f'text_column_{position+3}','XYZ'[(position-1)%3]),(f'numeric_column_{2*position+1}','999999.999'),(f'numeric_column_{2*position+2}','0.001')])
    yield {'name':'all-fourteen-axes-maximum-weight','edits':edits}


def main():
    install()
    import tests.semantics.native_copy as driver
    driver.cases=boundary_cases
    driver.main()


if __name__=='__main__':main()
