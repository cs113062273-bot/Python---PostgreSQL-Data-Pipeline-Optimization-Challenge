"""Full native differential/layout/boundary replay for the turbo adapter."""
from bench import native_turbo_probe as candidate
from bench.common import write_report
from tests.semantics import native_main_copy as driver


def main():
    driver.candidate = candidate
    driver.write_report = lambda path, value: write_report(str(path).replace('native-main', 'native-turbo'), value)
    driver.main()


if __name__ == '__main__':
    main()
