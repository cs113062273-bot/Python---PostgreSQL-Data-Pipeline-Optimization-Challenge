"""Check boundaries of SQL rewriting and preservation of organizer sources."""
import json
import hashlib
import pytest
from pipeline.recipes import ROOT, MODES, ORDER, destinations, rewrite
from pipeline.upstream import PostgresAdminService


def test_upstream_bytes_unchanged():
    manifest=json.loads((ROOT/"upstream.json").read_text(encoding="utf-8"))
    for name,expected in manifest["files_sha256"].items():
        assert hashlib.sha256((ROOT/"app"/name).read_bytes()).hexdigest()==expected,name


@pytest.mark.parametrize("mode",MODES)
def test_candidates_preserve_protected_source_and_destination_order(mode):
    result=PostgresAdminService._prepare_cross_table_expansion("public.raw_data",["arbitrary_scope"],destinations(mode))
    assert tuple(d["pure_table_name"] for d in result["destinations"])==ORDER
    for d in result["destinations"]:
        assert d["statements"]
        assert 'WHERE "raw_schema" IN (\'arbitrary_scope\')' in d["statements"][0]
        assert d["destination_columns"]


def test_rewriter_refuses_unknown_shapes():
    with pytest.raises(ValueError):
        rewrite("INSERT INTO public.table1 SELECT 1","record_once")
    with pytest.raises(ValueError):
        rewrite(destinations()[0]["sql"],"unknown")


def test_array_experiment_is_table2_only():
    for baseline,candidate in zip(destinations(),destinations("array_values")):
        if baseline["table_name"]!="public.table2":
            assert baseline["sql"]==candidate["sql"]
