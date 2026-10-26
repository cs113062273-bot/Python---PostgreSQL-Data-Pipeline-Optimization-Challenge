"""Run the original Windows GUI with explicitly selected recipe experiments."""
import argparse
import sys
from pipeline.recipes import destinations,rewrite
from pipeline.upstream import APP,PostgresAdminService


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode",choices=["baseline","path_table1","path_array","qf_lazy","qf_lazy_path1"],default="baseline")
    args=parser.parse_args()
    mode=args.mode
    originals={d["table_name"]:d["sql"].strip() for d in destinations()}
    class ExperimentalService(PostgresAdminService):
        def execute_cross_table_expansion(self,database_name,source_full_table_name,raw_schemas,destinations,**kwargs):
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
