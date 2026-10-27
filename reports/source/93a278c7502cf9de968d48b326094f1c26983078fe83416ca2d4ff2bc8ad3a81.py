"""Read the uncommitted stage size inside its owning native data transaction.

Used only by the supplementary resource probe. The actual instrumented SQL is
archived with its own SHA and that SHA is retained in native version provenance.
No ranked benchmark imports this hook.
"""
import hashlib
import re

from bench.common import ROOT
from pipeline.local_service import LocalService


def main():
    original=LocalService.run_remote_command
    def observed(self,command,stdin_text=None,**kwargs):
        measured=False
        if isinstance(stdin_text,str) and 'BEGIN ISOLATION LEVEL REPEATABLE READ;' in stdin_text and 'pgdm_cross_table_hash_manifest' in stdin_text and getattr(self,'bulk_recipe_mode',None) in ('guarded_ctas','guarded_copy'):
            marker='UPDATE pg_temp.pgdm_expansion_timings SET finished_at=clock_timestamp() WHERE sequence_number=5;'
            if stdin_text.count(marker)!=1:raise RuntimeError('Unknown native stage observation boundary')
            observation="SELECT 'PGDM_RESOURCE_STAGE_BYTES='||coalesce(pg_total_relation_size(to_regclass('pgdm_pipeline_work.pipeline_normalized_input')),0)::text;"
            stdin_text=stdin_text.replace(marker,marker+'\n'+observation,1)
            digest=hashlib.sha256(stdin_text.encode()).hexdigest()
            self._expansion_timing_local.guarded_batch_sha256=digest
            directory=ROOT/'reports/sql';directory.mkdir(parents=True,exist_ok=True)
            with (directory/(digest+'.sql')).open('w',encoding='utf-8',newline='\n') as f:f.write(stdin_text)
            measured=True
        output=original(self,command,stdin_text=stdin_text,**kwargs)
        if measured:
            matches=re.findall(r'^PGDM_RESOURCE_STAGE_BYTES=(\d+)\s*$',output,re.M)
            if len(matches)!=1:raise RuntimeError('Native stage observation missing or ambiguous')
            print('Native resource stage bytes: '+matches[0],flush=True)
            output=re.sub(r'^PGDM_RESOURCE_STAGE_BYTES=\d+\s*$', '',output,flags=re.M)
        return output
    LocalService.run_remote_command=observed
    from bench.service_run import main as service_main
    service_main()


if __name__=='__main__':main()
