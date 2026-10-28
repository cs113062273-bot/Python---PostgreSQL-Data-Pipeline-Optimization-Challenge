"""Native differential test of the per-value numeric parsing worker."""
from bench import memo_axes_probe as memo
from bench.common import write_report
from tests.semantics import native_copy as driver


def main():
    identity=memo.source_identity();memo.install('function')
    def recorded(path,value):
        assert memo.source_identity()==identity,'Memo worker changed during differential run'
        write_report(path,{**value,'memo_hook_sha256':identity})
    driver.write_report=recorded
    driver.main()


if __name__=='__main__':main()
