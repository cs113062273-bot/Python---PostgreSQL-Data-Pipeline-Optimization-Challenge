"""Exercise the independent turbo main proof when the details package is absent."""
from bench import native_turbo_probe as candidate
from bench.common import write_report
from tests.semantics import native_main_shared_fallback as driver


def main():
    driver.candidate = candidate
    driver.write_report = lambda path, value: write_report(str(path).replace('native-main-shared', 'native-turbo'), value)
    driver.main()


if __name__ == '__main__':
    main()
