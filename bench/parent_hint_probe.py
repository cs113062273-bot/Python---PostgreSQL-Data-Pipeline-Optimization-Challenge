"""Native fresh-parent statistics plus one-batch disposable-file hints."""
import hashlib
from pathlib import Path
import sys
import types
from bench import parent_statistics_probe as parent,temporary_hint_probe as hint,early_copy_probe as early


def source_identity():
    return hashlib.sha256((parent.source_identity()+early.source_identity()+Path(hint.__file__).read_text(encoding='utf8')+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def install():
    # Change only this research module's base-installer reference; do not run
    # two layered data/compiler installers over the same trusted statements.
    hint.combined=parent
    hint.install()
    import pipeline.parallel_normalization as parallel
    projection=parallel.prepare_input_sql
    parallel.prepare_input_sql=lambda scopes,logged=False:'-- lab parent statistics and scratch hint source_sha256='+source_identity()+'\n'+projection(scopes,logged=logged)


def main():
    if len(sys.argv)>1 and sys.argv[1]=='profile':
        sys.argv.pop(1)
        from bench import nested_plan_probe as driver
        driver.candidate=types.SimpleNamespace(install=install,source_identity=source_identity)
        driver.main();return
    install()
    from bench.service_run import main as driver
    driver()


if __name__=='__main__':main()
