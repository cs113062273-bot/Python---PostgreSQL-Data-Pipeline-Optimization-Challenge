"""Summarize successful runs while retaining explicit experiment failures."""
import argparse
import json
import statistics
from bench.common import write_report


def summarize(report):
    modes=list(dict.fromkeys(r["mode"] for r in report["runs"]))
    baseline=statistics.median(r["sql_seconds"] for r in report["runs"] if r["mode"]=="baseline") if "baseline" in modes else None
    report["summary"]={}
    for mode in modes:
        runs=[r for r in report["runs"] if r["mode"]==mode]
        values=[r["sql_seconds"] for r in runs]
        median=statistics.median(values)
        report["summary"][mode]={"individual_seconds":values,"median":median,"min":min(values),"max":max(values),
            "reduction_percent":100*(1-median/baseline) if baseline else None,
            "stage_medians":{s["name"]:statistics.median(next(t["seconds"] for t in r["stages"] if t["name"]==s["name"]) for r in runs) for s in runs[0]["stages"]}}
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("report");args=parser.parse_args()
    report=json.load(open(args.report,encoding="utf-8"))
    write_report(args.report,summarize(report))
    print(report["summary"])


if __name__=="__main__":main()
