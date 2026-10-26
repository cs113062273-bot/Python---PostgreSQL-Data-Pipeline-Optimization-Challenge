"""Run the original Windows GUI with explicitly selected recipe experiments."""
import argparse
import sys
from pipeline.recipes import destinations,rewrite
from pipeline.upstream import APP,PostgresAdminService
from pipeline.planner import Table1PlannerMixin
from pipeline.bulk_fks import BulkFKMixin
from pipeline.bulk_indexes import BulkIndexMixin
from pipeline.normalization import NormalizationMixin
from pipeline.guarded_normalization import GuardedNormalizationMixin, MODES as GUARDED_MODES
from pipeline.recipes import BULK_MODES, bulk_unique_tables


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode",choices=["baseline","bulk_fks","guarded_normalize","guarded_prefix"],default="baseline")
    args=parser.parse_args()
    mode=args.mode
    originals={d["table_name"]:d["sql"].strip() for d in destinations()}
    class ExperimentalService(BulkFKMixin,BulkIndexMixin,GuardedNormalizationMixin,NormalizationMixin,Table1PlannerMixin,PostgresAdminService):
        capture_batch_sql=True
        bulk_foreign_keys=mode in BULK_MODES
        guarded_normalization=mode in GUARDED_MODES
        normalize_input=mode in ("bulk_normalize","bulk_normalize64")
        bulk_recipe_mode=mode
        bulk_unique_tables=bulk_unique_tables(mode)
        table1_hash_join=mode in ("hash_table1","hash_array","qf_hash1","hash_array64","hash_array64_nomemo")
        table2_memoize=mode!="hash_array64_nomemo"
        expansion_work_mem="64MB" if mode in ("hash_array64","hash_array64_nomemo","bulk_full64","bulk_normalize64") else None
        def execute_cross_table_expansion(self,database_name,source_full_table_name,raw_schemas,destinations,**kwargs):
            if self.bulk_foreign_keys and (source_full_table_name!="public.raw_data" or tuple(d.get('table_name') for d in destinations)!=tuple(originals)):
                raise RuntimeError("Bulk FK experiment requires the exact organizer source and six destinations")
            selected=[]
            for destination in destinations:
                d=dict(destination)
                if mode!="baseline":
                    if d.get("sql","").strip()!=originals.get(d.get("table_name")):
                        raise RuntimeError("Experiment accepts only the exact original six recipes; select baseline for custom recipes")
                    d["sql"]=rewrite(d["sql"],mode)
                selected.append(d)
            return super().execute_cross_table_expansion(database_name,source_full_table_name,raw_schemas,selected,**kwargs)
    import services.postgres_service as module
    module.PostgresAdminService=ExperimentalService
    import importlib.util
    spec=importlib.util.spec_from_file_location("organizer_main",APP/"main.py")
    gui=importlib.util.module_from_spec(spec)
    sys.modules[spec.name]=gui
    spec.loader.exec_module(gui)
    gui.App().mainloop()


if __name__=="__main__":main()
