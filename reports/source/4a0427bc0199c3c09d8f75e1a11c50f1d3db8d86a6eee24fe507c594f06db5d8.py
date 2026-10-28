"""Single-pass, bounded actual-parent bitmap variant of full FK validation."""
import hashlib
import json
from pathlib import Path
from bench import native_fk_probe as base
from bench.common import ROOT


def package():
    info=json.loads((ROOT/'reports/native-fk-stream-trusted-package.json').read_text())
    source=(ROOT/info['source']).resolve();assert source.is_relative_to(ROOT/'native')
    assert hashlib.sha256(source.read_bytes()).hexdigest()==info['source_sha256']
    for item in info['files']:
        path=(ROOT/item['path']).resolve();assert path.is_relative_to(ROOT/'.runtime/postgresql/pgsql')
        assert hashlib.sha256(path.read_bytes()).hexdigest()==item['sha256']
    return info


def source_identity():
    # Stable both before and after installing the package override. Keep the
    # original 89.77% implementation and its byte identity independently usable.
    body=base.base.source_identity()+Path(base.__file__).read_text(encoding='utf8')+Path(base.cache.__file__).read_text(encoding='utf8')+json.dumps(package(),sort_keys=True)
    return hashlib.sha256((body+Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def member_guard():
    original=base.package
    try:
        base.package=package
        return base.member_guard()
    finally:base.package=original


def install():
    base.package=package
    base.install()


def main():
    frozen=source_identity();install()
    from bench import service_run as driver
    original=driver.write_report
    def recorded(path,value):
        assert source_identity()==frozen
        original(path,{**value,'native_fk_stream_hook_sha256':frozen,'trusted_fk_package':package(),'official_eligibility_unproven':True})
    driver.write_report=recorded;driver.main()


if __name__=='__main__':main()
