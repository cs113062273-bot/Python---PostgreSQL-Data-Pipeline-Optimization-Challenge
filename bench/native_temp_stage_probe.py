"""Research fused stream loader with session-local normalized input storage.

Only the disposable normalized input changes persistence. Permanent output
tables, native constraints, full FK checks, commit and versioning are retained.
The final transformed SQL is archived by the existing server COPY transport.
"""
import hashlib
from pathlib import Path

from bench import native_stream_probe as base

package = base.package


def source_identity():
    return hashlib.sha256((base.source_identity() + Path(__file__).read_text(encoding='utf8')).encode()).hexdigest()


def rewrite(text):
    original = 'pgdm_pipeline_work.pipeline_normalized_input'
    temporary = 'pg_temp.pipeline_native_temp_input_v1'
    create = "CREATE UNLOGGED TABLE '||target||' AS '"
    assert text.count(create) == 2
    assert text.count("target text:='" + original + "'") == 1
    assert temporary not in text
    text = text.replace(create, "CREATE TEMP TABLE '||target||' ON COMMIT DROP AS '")
    text = text.replace(original, temporary)
    begin = 'BEGIN ISOLATION LEVEL REPEATABLE READ;'
    assert text.count(begin) == 1
    # The timing protocol creates a temporary relation before BEGIN, so the
    # buffer allocation must be selected before the first batch statement.
    return ("SET temp_buffers='256MB';\n-- temporary stage source_sha256="
            + source_identity() + '\n' + text)


def install():
    base.install()
    from bench import server_copy_probe as server
    previous = server.rewrite
    server.rewrite = lambda text, cwd: rewrite(previous(text, cwd))


def main():
    frozen = source_identity()
    install()
    from bench import service_run as driver
    previous = driver.write_report

    def recorded(path, value):
        assert source_identity() == frozen
        previous(path, {**value, 'native_main_hook_sha256': frozen,
                        'trusted_main_package': package(), 'trusted_stamp_package': package('stamp'),
                        'trusted_details_package': package('memo'), 'trusted_fk_package': package('fk'),
                        'temporary_normalized_input': True, 'stage_temp_buffers': '256MB',
                        'official_eligibility_unproven': True})

    driver.write_report = recorded
    driver.main()


if __name__ == '__main__':
    main()
