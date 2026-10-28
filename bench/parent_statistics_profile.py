"""Unscored nested native plan observation with fresh parent statistics."""
from bench import parent_statistics_probe as parent,nested_plan_probe as driver


def main():
    driver.candidate=parent
    driver.main()


if __name__=='__main__':main()
