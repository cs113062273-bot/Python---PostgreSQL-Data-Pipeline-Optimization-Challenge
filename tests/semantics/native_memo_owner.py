"""Non-superuser execution and code protection of the memoized COPY loader."""
import types
from bench import native_main_compact_probe as packages,native_details_probe as details
from bench.common import write_report
from tests.semantics import native_details_owner as driver


def main():
    details.package=lambda:packages.package('memo')
    driver.candidate=types.SimpleNamespace(package=details.package,member_guard=details.member_guard)
    driver.write_report=lambda path,value:write_report(str(path).replace('native-details','native-memo'),value)
    driver.main()


if __name__=='__main__':main()
