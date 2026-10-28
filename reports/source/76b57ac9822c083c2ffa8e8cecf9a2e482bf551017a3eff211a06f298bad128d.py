"""Slim native COPY/NULL cache atop the previously qualified inline worker.

This comparison excludes the later date-shape, combined-metadata, fresh-parent
ANALYZE and scratch-hint experiments, rather than presuming they all compose
into an improvement. Explicit source identity is stable before/after install.
"""
import hashlib
from pathlib import Path
from bench import t1_inline_probe as inline,null_cache_probe as null_cache,slim_copy_probe as slim


def source_identity():
    return hashlib.sha256((inline.source_identity()+Path(null_cache.__file__).read_text(encoding='utf8')+Path(slim.__file__).read_text(encoding='utf8')+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def install():
    # Choose one base installer; do not install two compiler/transport chains.
    null_cache.base=inline
    slim.install()
    import pipeline.parallel_normalization as parallel
    projection=parallel.prepare_input_sql
    parallel.prepare_input_sql=lambda scopes,logged=False:'-- lab slim COPY on inline base source_sha256='+source_identity()+'\n'+projection(scopes,logged=logged)


def main():
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
