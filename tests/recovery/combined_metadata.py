"""Both native -c and stdin isolation suites over one-phase metadata framing."""
import types
from pathlib import Path
from bench import combined_metadata_probe as combined,script_transport_probe as script
from bench.common import write_report
from tests.recovery import metadata_stdin as driver


def main():
    frozen=combined.source_identity()
    driver.script=types.SimpleNamespace(install=combined.install,complete_program=script.complete_program,source_identity=combined.source_identity)
    def recorded(path,value):
        assert combined.source_identity()==frozen
        write_report(Path(path).with_name('combined-'+Path(path).name),{**value,'combined_metadata_hook_sha256':frozen})
    driver.write_report=recorded;driver.main()


if __name__=='__main__':main()
