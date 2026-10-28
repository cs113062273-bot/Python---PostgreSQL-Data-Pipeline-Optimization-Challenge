"""Replay native FK qualifications against the single-pass bitmap package."""
import sys
from bench import native_fk_stream_probe as candidate
from bench.common import write_report


def main():
    if len(sys.argv)>1 and sys.argv[1] in ('validator','owner'):
        mode=sys.argv.pop(1)
        if mode=='validator':
            from tests.semantics import native_fk_validate as driver
            output='reports/native-fk-stream-validator-semantics.json'
        else:
            from tests.semantics import native_fk_owner as driver
            output='reports/native-fk-stream-owner-permissions.json'
        driver.candidate=candidate
        driver.main(output=output,**({'extended':True} if mode=='validator' else {}));return
    from tests.semantics import native_fk_copy as driver
    driver.candidate=candidate
    driver.write_report=lambda path,value:write_report(str(path).replace('native-fk','native-fk-stream'),value)
    driver.main()


if __name__=='__main__':main()
