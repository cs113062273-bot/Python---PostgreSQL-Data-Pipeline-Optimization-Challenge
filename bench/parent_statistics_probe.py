"""Analyze the freshly loaded parent's axis-count column before child queries.

Completed-database component plans can use parent statistics which do not yet
exist in a fresh native load. This experiment collects only the relevant native
statistics inside the safe branch, with its complete cost inside processing.
"""
import hashlib
from pathlib import Path
from bench import combined_metadata_probe as combined


def source_identity():
    return hashlib.sha256((combined.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def install():
    combined.install()
    import pipeline.bulk_indexes as indexes
    import pipeline.parallel_normalization as parallel
    restore=indexes.restore_sql
    def restored(table,*,mode=None):
        body=restore(table,mode=mode)
        if mode=='guarded_copy' and table=='table1':body+='\nANALYZE public.table1(integer_column_1);'
        return body
    indexes.restore_sql=restored
    projection=parallel.prepare_input_sql
    parallel.prepare_input_sql=lambda scopes,logged=False:'-- lab parent axis-count statistics source_sha256='+source_identity()+'\n'+projection(scopes,logged=logged)


def main():
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
