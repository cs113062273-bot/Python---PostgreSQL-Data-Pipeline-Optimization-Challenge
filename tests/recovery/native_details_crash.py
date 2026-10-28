"""Actual pre-commit server crash of the shared table5/6/2 native pipeline."""
import os
from bench import native_details_probe as candidate
from bench.common import ROOT,write_report
from tests.recovery import native_copy_crash as driver


def main():
    os.environ.update({'PGHOST':'127.0.0.1','PGPORT':'55433','PGUSER':'native_probe','CHALLENGE_DSN':'host=127.0.0.1 port=55433 user=native_probe','CHALLENGE_PSQL':str(ROOT/'.runtime/postgresql/pgsql/bin/psql.exe')})
    frozen=candidate.source_identity();candidate.install()
    def recorded(ignored,value):
        assert candidate.source_identity()==frozen and value['execution_architecture']['safe_domain'] is True
        assert value['local_backend_diagnostics']['actual_binary_files']==3
        write_report('reports/recovery-native-details-server-crash.json',{**value,'candidate_hook_sha256':frozen,'explicit_wal_level':'minimal','official_eligibility_unproven':True})
    driver.write_report=recorded;driver.main()


if __name__=='__main__':main()
