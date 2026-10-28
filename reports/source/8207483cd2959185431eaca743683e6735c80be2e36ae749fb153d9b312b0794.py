"""Bounded reuse of pure canonical SQL parsing, never database validation.

The actual original service still reexecutes source/destination/function/count
and version checks. Only the compiler's six identical baseline-template parses
share a per-process, bounded cache keyed by the complete scope tuple.
"""
from functools import lru_cache
import hashlib
from pathlib import Path
from bench import native_details_probe as base,native_compact_sql as compiler


def source_identity():
    return hashlib.sha256((base.source_identity()+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def install_cache():
    original=compiler.prepared
    @lru_cache(maxsize=8)
    def templates(scopes):return original('baseline',list(scopes))
    compiler.prepared=lambda mode,scopes:templates(tuple(scopes)) if mode=='baseline' else original(mode,scopes)
    return templates


def install():
    base.install();install_cache()


def main():
    frozen=source_identity();install()
    from bench import service_run as driver
    original=driver.write_report
    def recorded(path,value):
        assert source_identity()==frozen
        original(path,{**value,'native_compile_cache_hook_sha256':frozen,'official_eligibility_unproven':True})
    driver.write_report=recorded;driver.main()


if __name__=='__main__':main()
