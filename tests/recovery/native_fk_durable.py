"""Acknowledged full shared-FK pipeline survives immediate server stop."""
from bench import native_fk_probe as candidate
from tests.recovery import native_minimal_durable as driver


def main():
    driver.candidate=candidate
    driver.main(expected_files=3)


if __name__=='__main__':main()
