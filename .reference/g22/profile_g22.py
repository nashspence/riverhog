#!/usr/bin/env python3
"""Source-bound native work counters, not an end-to-end performance verdict."""
from __future__ import annotations
import argparse, collections, importlib.util, json, platform, sqlite3, sys, time
from pathlib import Path

def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output exists; never replace retained evidence")
    tests = args.repo / "packages/riverhog-provenance/tests"
    jt = load(tests / "test_journal_epochs.py", "g22_journal_helpers")
    ot = load(tests / "test_online_dag.py", "g22_online_helpers")
    from riverhog_provenance import BoundedSourceObserver, BytesSource, create_journal
    from riverhog_provenance_contracts import ContractCatalog
    from riverhog_provenance._online_dag import OnlineDag
    catalog = ContractCatalog()
    observation = BoundedSourceObserver(catalog=catalog).observe(BytesSource(b"fixed probe"))
    who = observation.observer_agent_id
    journal = create_journal(observation.graph_fragment(), recorded_by_agent_id=who, catalog=catalog)
    records = []
    for n in (16, 32, 64, 128, 256):
        frames, states = jt.seed_graph(journal, who, catalog, n+1)
        for i in reversed(range(n)):
            frames = jt.append_frame(frames, who, catalog, "assertion", {
                "assertions": {"relations": [jt.edge(who, states[i], states[i+1])]}})
        raw = jt.encoded(frames)
        start = time.perf_counter()
        result, counts = jt.validate_counted(raw, catalog)
        elapsed = time.perf_counter()-start
        assert counts["rank_edges"] == n and counts["certificate_checks"] == n
        assert len(result.graph["relations"]) == n
        records.append({"profile": "complete-append-only-reverse-chain", "edges": n,
                        "bytes": len(raw), "counters": counts,
                        "native_seconds": elapsed, "result": "complete"})
        for direction in ("forward", "reverse"):
            counter = collections.Counter()
            db = ot.database()
            try:
                dag = OnlineDag(db, "derivation", lambda key, n=1: counter.update({key:n}))
                positions = range(n) if direction == "forward" else reversed(range(n))
                for i in positions:
                    dag.add(f"n{i:06d}", f"n{i+1:06d}")
                ot.check_invariants(db,"derivation")
                records.append({"profile": "true-online-"+direction+"-chain", "edges": n,
                                "counters": dict(counter), "result": "complete"})
            finally:
                db.close()
    output = {"kind":"native-prototype-not-product-qualification", "source":"152d095038df0c0d0ed642cf265d47b11c65a754",
              "python":platform.python_version(), "sqlite":sqlite3.sqlite_version, "records":records}
    args.output.write_text(json.dumps(output, indent=2)+"\n")
    print(json.dumps(output,indent=2))
if __name__ == "__main__":
    main()
