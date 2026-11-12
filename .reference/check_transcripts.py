"""Finite checker for this witness's actual causal transcripts, not a product registry."""
from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy
import hashlib
import json
from pathlib import Path
import sys

DISPATCH = "riverhog-server.journal-retirement-dispatch.growth/v1"
SCHEDULE = "riverhog-server.journal-retirement-schedule.growth/v1"


def canonical(value):
    # Recorded execution samples contain only ASCII strings and safe integers.
    # This explicitly restricted encoding is not a general JCS replacement.
    def check(v):
        if isinstance(v, str):
            assert v.isascii()
        elif type(v) is int:
            assert -(2**53) < v < 2**53
        elif v is None or type(v) is bool:
            pass
        elif isinstance(v, list):
            for item in v:
                check(item)
        elif isinstance(v, dict):
            for key, item in v.items():
                check(key)
                check(item)
        else:
            raise AssertionError("unsupported transcript sample scalar")
    check(value)
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()


def verify(rows):
    samples = defaultdict(dict)
    declared_workers = Counter()
    observed_workers = Counter()
    transcripts = 0
    attempts = 0

    def visit(node, sample):
        digest = hashlib.sha256(canonical(sample)).hexdigest()
        samples[node][digest] = sample
        for values in sample.get("delegates", {}).values():
            for value in values:
                visit(node, value)

    for row in rows:
        if "sample" in row:
            visit(row["nodeid"], row["sample"])
            if row["scope"] == "separate_original_worker_samples":
                assert row["sample"]["declaration"] == DISPATCH
                declared_workers[row["nodeid"]] += 1

    for row in rows:
        if "transcript" not in row:
            continue
        transcripts += 1
        node, transcript = row["nodeid"], row["transcript"]
        roots = {}
        for root in transcript["root_samples"]:
            assert root["index"] not in roots, "duplicate root identity"
            sample = samples[node][root["sha256"]]
            assert sample["declaration"] == root["declaration"]
            roots[root["index"]] = sample
        scopes, carry, submitted, entered, worker_roots, joined = {}, set(), {}, {}, {}, {}
        physical, withdrawn = set(), set()
        for event in transcript["events"]:
            kind = event["event"]
            original, attempt = event.get("original"), event.get("attempt")
            if original is not None:
                scope = event["scope_sha256"]
                assert len(scope) == 64
                assert scopes.setdefault(original, scope) == scope, "original scope changed"
            if kind == "carry-in":
                assert original not in carry
                assert event["ordinary_charged"] is True
                carry.add(original)
            elif kind == "carry-in-joined":
                assert original in carry and attempt not in joined
                assert event["ordinary_charged"] is True
                joined[attempt] = event
            elif kind == "submitted":
                assert attempt not in submitted and original not in withdrawn
                assert event["close_only"] is False or original in carry or any(
                    prior["original"] == original for prior in submitted.values())
                submitted[attempt] = event
            elif kind == "worker-entered":
                assert attempt not in entered
                entered[attempt] = event
            elif kind == "worker-root":
                assert attempt in submitted and attempt not in worker_roots
                sample = roots[event["sample"]]
                assert sample["declaration"] == DISPATCH
                if submitted[attempt]["close_only"]:
                    assert not sample["delegates"]["work"], "close retry repeated file work"
                    assert len(sample["delegates"]["close"]) == 1
                worker_roots[attempt] = event
                observed_workers[node] += 1
            elif kind == "original-joined":
                assert attempt in worker_roots and attempt not in joined, "join missing or repeated"
                assert submitted[attempt]["original"] == original
                assert event["ordinary_charged"] is True, "join is not ordinary withdrawal"
                joined[attempt] = event
            elif kind == "physical-received":
                assert attempt in joined and attempt not in physical
                assert joined[attempt]["original"] == original, "cross-original receiving"
                physical.add(attempt)
            elif kind == "ordinary-withdrawn":
                assert original not in withdrawn
                relevant = [a for a in physical if joined[a]["original"] == original]
                assert relevant, "withdrawal has no joined original outcome"
                assert any(joined[a]["actor_received"] for a in relevant)
                assert event["actor_received"] and event["close_confirmed"]
                assert event["ordinary_charged"] is False
                withdrawn.add(original)
            elif kind == "schedule-return":
                sample = roots[event["sample"]]
                assert sample["declaration"] == SCHEDULE
                assert event["bookkeeping"] == sample["metrics"]["bookkeeping_events"]
                assert event["bookkeeping"] <= 4 * sample["dimensions"]["unit_limit"]
            elif kind == "phase-root":
                assert event["sample"] in roots
            else:
                raise AssertionError("unknown event: " + kind)
        assert submitted.keys() == entered.keys() == worker_roots.keys(), "missing worker attribution"
        assert submitted.keys() <= joined.keys(), "unfinished original join"
        for attempt, event in submitted.items():
            assert entered[attempt]["original"] == event["original"], "cross-original worker"
        attempts += len(submitted)
    assert declared_workers == observed_workers, "worker roots omitted or counted twice"
    return {"transcripts": transcripts, "observed_attempts": attempts,
            "worker_samples": sum(declared_workers.values()), "complete_observed_attempts": True}


def negative_controls(rows):
    index = next(i for i, r in enumerate(rows) if "transcript" in r and any(
        e["event"] == "original-joined" for e in r["transcript"]["events"]))
    functions = {
        "missing-original-join": lambda es: es.pop(next(i for i,e in enumerate(es) if e["event"] == "original-joined")),
        "duplicate-worker-root": lambda es: es.insert(len(es), deepcopy(next(e for e in es if e["event"] == "worker-root"))),
        "cross-original-join": lambda es: next(e for e in es if e["event"] == "original-joined").update(original=999999),
        "false-ordinary-retention": lambda es: next(e for e in es if e["event"] == "original-joined").update(ordinary_charged=False),
    }
    rejected = []
    for name, mutate in functions.items():
        changed = list(rows)
        changed[index] = deepcopy(rows[index])
        mutate(changed[index]["transcript"]["events"])
        try:
            verify(changed)
        except (AssertionError, KeyError):
            rejected.append(name)
        else:
            raise AssertionError("invalid evidence admitted: " + name)
    changed = list(rows)
    changed.pop(next(i for i,r in enumerate(changed) if r["scope"] == "separate_original_worker_samples"))
    try:
        verify(changed)
    except (AssertionError, KeyError):
        rejected.append("missing-worker-sample")
    else:
        raise AssertionError("missing worker sample admitted")
    return rejected


if __name__ == "__main__":
    path = Path(sys.argv[1])
    content = path.read_bytes()
    rows = [json.loads(line) for line in content.splitlines()]
    result = verify(rows)
    result["negative_controls_rejected"] = negative_controls(rows)
    result["sample_stream_sha256"] = hashlib.sha256(content).hexdigest()
    result["scope"] = "finite reference transcript checker; roots validated separately by maintained framework"
    print(json.dumps(result, sort_keys=True))
