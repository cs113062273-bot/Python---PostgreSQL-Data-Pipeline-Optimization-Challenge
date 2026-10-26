import argparse
import json
import sys
from bench.common import connect
from pipeline.checkpoints import ChunkRunner
from pipeline.local_service import LocalService


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("database")
    parser.add_argument("job")
    parser.add_argument("event")
    args=parser.parse_args()
    def failpoint(event,detail):
        if event==args.event:
            print(json.dumps({"ready":True,"event":event,"detail":detail}),flush=True)
            sys.stdin.readline()  # Parent kills the client at this exact boundary.
    ChunkRunner(LocalService(),connect,failpoint).run(args.database,["group001"],args.job,chunk_rows=16)


if __name__=="__main__": main()
