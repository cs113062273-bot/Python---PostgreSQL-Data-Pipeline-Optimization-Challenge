"""Fused COPY plus one JSON value walk with integrated numeric proof."""
import hashlib
import json
from pathlib import Path
from bench import native_fused_probe as fused
from bench.common import ROOT

PREVIOUS_PACKAGE = fused.ORIGINAL_PACKAGE


def stamp_package(kind='main'):
    if kind != 'stamp':
        return PREVIOUS_PACKAGE(kind)
    info = json.loads((ROOT / 'reports/native-stream-stamp-trusted-package.json').read_text())
    for item in [dict(path=info['source'], sha256=info['source_sha256']), *info['files'], *info['vendor_sources']['files']]:
        path = (ROOT / item['path']).resolve()
        assert path.is_relative_to(ROOT) and hashlib.sha256(path.read_bytes()).hexdigest() == item['sha256']
    return info


fused.ORIGINAL_PACKAGE = stamp_package
package = fused.package
install = fused.install


def source_identity():
    return hashlib.sha256((fused.source_identity() + Path(__file__).read_text(encoding='utf8')
                           + json.dumps(stamp_package('stamp'), sort_keys=True)).encode()).hexdigest()


def main():
    frozen = source_identity()
    install()
    from bench import service_run as driver
    original = driver.write_report

    def recorded(path, value):
        assert source_identity() == frozen
        original(path, {**value, 'native_main_hook_sha256': frozen, 'trusted_main_package': package(),
                        'trusted_stamp_package': package('stamp'), 'trusted_details_package': package('memo'),
                        'trusted_fk_package': package('fk'), 'official_eligibility_unproven': True})

    driver.write_report = recorded
    driver.main()


if __name__ == '__main__':
    main()
