from __future__ import annotations

import os

import pytest

from tests.support.retained_resource import fault_native as fault_native

pytestmark = pytest.mark.skipif(os.name != "posix", reason="actual POSIX retirement receiving")


@pytest.mark.parametrize("fault", ["unknown_close_open", "unknown_close_closed"])
def test_unknown_original_stays_charged_while_independent_work_receives(
    tmp_path, fault_native, fault
):
    import json
    import subprocess
    import sys

    _, extension = fault_native
    code = (
        "import sys,json;sys.path[:]=json.loads(sys.argv[1]);"
        "from tests.support.journal_retirement_unknown import run;"
        "run(sys.argv[2],sys.argv[3],sys.argv[4])"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, json.dumps(sys.path), str(extension), str(tmp_path), fault],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    receipt = json.loads(result.stdout)
    assert receipt["ordinary_charge"] == 1 and receipt["physical_workers"] == 0
    assert receipt["independent_received"] and receipt["original_retained"]
    assert receipt["repeat_close_dispatches"] == 0 and not receipt["forced_receiving"]
