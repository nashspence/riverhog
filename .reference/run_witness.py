"""Execute the explicit reference overlay and retain phase evidence."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys
import time
from collections import Counter

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import pytest

TESTS=(
 "tests/unit/test_async_retirement_phase_edges.py",
 "tests/unit/test_upload_journal_validation_retirement_loop_growth.py",
 "tests/unit/test_journal_retirement_registry.py",
 "tests/unit/test_upload_journal_validation_retirement_growth.py",
 "tests/unit/test_upload_journal_validation_retirement_catalog_growth.py",
 "tests/unit/test_upload_journal_service_scheduler_growth.py",
 "tests/unit/test_discovery_retirement_registry.py",
)
ADDED=("riverhog/src/riverhog_core/services/journal_retirement_growth.py",
       "tests/support/async_retirement_growth.py","tests/unit/test_async_retirement_phase_edges.py")
class Evidence:
    def __init__(self):
        self.outcomes=[];self.samples=Counter();self.metrics={};self.stream=hashlib.sha256()
        self.file=(ROOT/"phase-samples.jsonl").open("wb")
    def pytest_runtest_logreport(self,report):
        if report.when=="call" or report.failed or report.skipped:
            self.outcomes.append(dict(nodeid=report.nodeid,when=report.when,
                outcome=report.outcome,duration=report.duration))
        if report.when!="call":return
        for name,values in report.user_properties:
            if name=="retirement_transcripts":
                for row in values:
                    encoded=json.dumps(dict(nodeid=report.nodeid,scope=name,transcript=row),sort_keys=True,separators=(",",":")).encode()+b"\n"
                    self.file.write(encoded);self.stream.update(encoded)
                continue
            if name not in {"growth_samples","separate_original_worker_samples"}:continue
            for row in values:
                encoded=json.dumps(dict(nodeid=report.nodeid,scope=name,sample=row),sort_keys=True,separators=(",",":")).encode()+b"\n"
                self.file.write(encoded);self.stream.update(encoded)
                self.samples[row["declaration"]]+=1
                metrics=self.metrics.setdefault(row["declaration"],{})
                for key,value in row["metrics"].items():metrics[key]=max(metrics.get(key,0),value)
    def finish(self,code,elapsed):
        self.file.close()
        source_files=set(subprocess.check_output(["git","diff","--name-only"],cwd=ROOT,text=True).splitlines())|set(ADDED)
        result=dict(reference_sha=subprocess.check_output(["git","rev-parse","HEAD"],cwd=ROOT,text=True).strip(),
            base="dd09f4ab5e824935e680f26fba3d73d2484cb426",scope="explicit reference overlay / actual maintained retirement owners",
            python=sys.version,platform=platform.platform(),pytest=pytest.__version__,returncode=code,
            elapsed_seconds=elapsed,outcomes=self.outcomes,sample_counts=dict(self.samples),maxima=self.metrics,
            full_sample_stream_sha256=self.stream.hexdigest(),
            changed_source_sha256={p:hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in sorted(source_files)},
            reference_only=True,not_product_qualification=True)
        (ROOT/"reference-result.json").write_text(json.dumps(result,sort_keys=True,indent=2)+"\n")
        print(json.dumps({k:v for k,v in result.items() if k not in {"maxima","outcomes","changed_source_sha256"}},sort_keys=True))
if __name__=="__main__":
    evidence=Evidence();started=time.monotonic()
    code=int(pytest.main(["-q","--tb=short",*TESTS],plugins=[evidence]))
    evidence.finish(code,time.monotonic()-started)
    raise SystemExit(code)
