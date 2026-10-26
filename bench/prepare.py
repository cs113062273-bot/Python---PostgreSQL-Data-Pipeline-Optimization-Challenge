"""Clone official generated input; refuse to replace any existing database."""
import argparse
from psycopg import sql
from bench.common import connect


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="lab_100k")
    parser.add_argument("--template", default="kaggle_challenge")
    args = parser.parse_args()
    if not args.database.startswith("lab_"):
        parser.error("Destination must start with lab_")
    with connect("postgres", autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE {}").format(sql.Identifier(args.database), sql.Identifier(args.template)))
    with connect(args.database) as conn:
        conn.execute("CREATE SCHEMA challenge_lab; CREATE TABLE challenge_lab.disposable (created_at timestamptz DEFAULT now())")
        conn.execute("INSERT INTO challenge_lab.disposable DEFAULT VALUES")
    print(f"Created disposable diagnostic database {args.database}")


if __name__ == "__main__":
    main()
