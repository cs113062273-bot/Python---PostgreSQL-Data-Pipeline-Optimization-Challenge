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


def test_direct_queryfoundry_control_uses_pinned_original():
    from vendor.queryfoundry_recipes import rewrite as original
    for baseline,lazy,payload in zip(destinations(),destinations("qf_lazy"),destinations("qf_payload")):
        assert lazy["sql"]==original(baseline["sql"],"lazy_keys")
        assert payload["sql"]==original(baseline["sql"],"payload_once")


def test_planner_override_preserves_counter_diagnostics_and_class_helpers():
    from pipeline.planner import Table1PlannerMixin
    class Tuned(Table1PlannerMixin,PostgresAdminService):pass
    original=PostgresAdminService._build_managed_dml_sql("INSERT INTO public.table1 SELECT source_row.raw_id FROM x source_row","public","table1","add")
    tuned=Tuned._build_managed_dml_sql("INSERT INTO public.table1 SELECT source_row.raw_id FROM x source_row","public","table1","add")
    assert "GET DIAGNOSTICS pgdm_affected_rows = ROW_COUNT;\n    PERFORM set_config" in tuned
    assert "UPDATE" in tuned and "row_count = row_count + pgdm_affected_rows" in tuned
    assert tuned!=original
    prepared=Tuned._prepare_managed_sql_script("INSERT INTO public.table1 (id_column_2) SELECT 1","public.table1")
    assert prepared["wrapped_sql"]


def test_dump_check_normalization_is_narrow():
    from pipeline.checkpoints import normalize_catalog
    original="CHECK (((text_column_1 IS NULL) OR ((text_column_1)::text = ANY ((ARRAY['X'::character varying, 'Y'::character varying, 'Z'::character varying])::text[]))))"
    restored="CHECK (((text_column_1 IS NULL) OR ((text_column_1)::text = ANY (ARRAY[('X'::character varying)::text, ('Y'::character varying)::text, ('Z'::character varying)::text]))))"
    shape=lambda definition:{"constraints":[["table2","table2_text_column_1_check",definition,True]]}
    assert normalize_catalog(shape(original))==normalize_catalog(shape(restored))
    assert normalize_catalog(shape(original))!=normalize_catalog(shape(restored.replace("'Z'","'R'")))


def test_bulk_strategy_binds_exact_recipes_and_resets_thread_context():
    import threading
    from pipeline.bulk_fks import BulkFKMixin
    class Base:
        def __init__(self):self._expansion_timing_local=threading.local()
        def execute_cross_table_expansion(self,*args,**kwargs):
            assert self._expansion_timing_local.bulk_fk_active
            assert self._expansion_timing_local.bulk_selection_context[0]=="public.raw_data"
            raise RuntimeError("injected base failure")
    class Service(BulkFKMixin,Base):
        bulk_foreign_keys=True
        bulk_recipe_mode="bulk_array"
    service=Service()
    with pytest.raises(RuntimeError,match="injected base failure"):
        service.execute_cross_table_expansion("lab_unit","public.raw_data",["scope"],destinations("bulk_array"))
    assert service._expansion_timing_local.bulk_fk_active is False
    assert service._expansion_timing_local.bulk_selection_context is None
    for source,selected in [("public.other",destinations("bulk_array")),("public.raw_data",destinations("qf_lazy")),
                            ("public.raw_data",destinations("bulk_array")[:-1])]:
        with pytest.raises(RuntimeError,match="exact organizer"):
            service.execute_cross_table_expansion("lab_unit",source,["scope"],selected)


def test_guarded_mode_versions_carry_compiler_provenance():
    from pipeline.recipes import compiler_sha256
    guarded=destinations('guarded_normalize')
    original=PostgresAdminService._prepare_cross_table_expansion('public.raw_data',['scope'],destinations())
    compiled=PostgresAdminService._prepare_cross_table_expansion('public.raw_data',['scope'],guarded)
    for d,o,g in zip(guarded,original['destinations'],compiled['destinations']):
        assert 'compiler_sha256='+compiler_sha256() in d['sql']
        assert o['statements']==g['statements']
