"""An interrupted replacement must preserve the previous experiment receipt."""
import json
import os
from pathlib import Path
import tempfile
from unittest.mock import patch
import pytest
from bench.common import ROOT,write_report


def test_replacement_failure_preserves_previous_receipt():
    with tempfile.TemporaryDirectory(prefix='report_receipt_',dir=ROOT/'.runtime') as directory:
        resolved=Path(directory).resolve()
        assert resolved.is_relative_to(ROOT/'.runtime') and resolved.name.startswith('report_receipt_')
        path=resolved/'evidence.json';write_report(path,{'generation':1,'text':'完整\n紀錄'})
        before=path.read_bytes()
        with patch.object(Path,'replace',side_effect=PermissionError('injected replacement failure')),patch('bench.common.os.fsync',wraps=os.fsync) as sync:
            with pytest.raises(PermissionError):write_report(path,{'generation':2})
            assert sync.call_count==1
        assert path.read_bytes()==before and json.loads(path.read_text(encoding='utf8'))['generation']==1
        assert list(resolved.glob('*.tmp'))==[]
        write_report(path,{'generation':3});assert json.loads(path.read_text())['generation']==3
