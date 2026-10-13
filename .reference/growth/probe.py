# SPDX-License-Identifier: Apache-2.0
"""Non-authoritative growth diagnostics, not a Riverhog format or validator.

The radix probe executes an exact, hash-pinned AST slice of the native binary
implementation, without replacing any serializer. Other probes are explicit
algorithm counterexamples. Primitive counts are not total CPU/I/O guarantees.
"""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import platform
import sqlite3
import subprocess
import sys
import tempfile
import types
from collections import Counter
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[2]
TAG_PATH = "packages/riverhog-protocol/src/riverhog_protocol/collection_tags.py"
TAG_SHA256 = "84fd0abe870f22d88e83a9f58ca2d8ee11ef078baf9dad5165bdd93f3e8b0ec5"
G22_REF = "e35bddf8f1bb72ca3f0046b597d5966cd5a08312"
G22_PATH = ".reference/g22_g26/sha256_state.py"
G22_SHA256 = "e8326f2eff2fb5094e037c04bdf66e499ed86888b7c543f3ebf81b7f013f1380"


def frames(n: int, width: int = 64) -> list[bytes]:
    if not 0 <= n < 10**8 or width < 8:
        raise ValueError("outside this fixed-width diagnostic workload")
    return [f"{i:08d}".encode() + b"x" * (width - 8) for i in range(n)]


def prefix_hashes(data: Iterable[bytes], *, incremental: bool) -> tuple[list[str], int]:
    result: list[str] = []
    count = 0
    prefix = bytearray()
    state = hashlib.sha256()
    for raw in data:
        if incremental:
            state.update(raw)
            count += len(raw)
            result.append(state.copy().hexdigest())
        else:
            prefix.extend(raw)
            count += len(prefix)
            result.append(hashlib.sha256(prefix).hexdigest())
    return result, count


def hash_trial(n: int, width: int = 64) -> dict[str, int]:
    data = frames(n, width)
    slow, naive = prefix_hashes(data, incremental=False)
    fast, streamed = prefix_hashes(data, incremental=True)
    if slow != fast:
        raise AssertionError("prefix identity changed")
    # Deliberately repeat the bad recovery pattern once per growing prefix.
    # This is NOT instrumentation of all calls in annotation_reference.Copy.
    lifetime = sum(prefix_hashes(data[:k], incremental=False)[1] for k in range(1, n + 1))
    return {"entries": n, "input_octets": n * width,
            "naive_recovery_hash_octets": naive, "streamed_hash_octets": streamed,
            "repeated_bad_recovery_hash_octets": lifetime}


def load_tag_kernel(root: Path = ROOT) -> types.ModuleType:
    raw = (root / TAG_PATH).read_bytes()
    if hashlib.sha256(raw).hexdigest() != TAG_SHA256:
        raise ValueError("native tag source changed: reconcile the audit before running")
    source = ast.parse(raw, filename=TAG_PATH)
    selected: list[ast.stmt] = []
    for node in source.body:
        if isinstance(node, ast.FunctionDef) and node.name == "collection_tag_head_identity":
            break
        if isinstance(node, ast.ImportFrom) and node.module in {"pydantic", "riverhog_canonical_json"}:
            continue
        if isinstance(node, ast.TypeAlias) and node.name.id == "CollectionTag":
            continue
        selected.append(node)
    else:
        raise ValueError("native slice boundary missing")
    module = types.ModuleType("riverhog_native_tag_growth_slice")
    module.__file__ = str(root / TAG_PATH)
    sys.modules[module.__name__] = module
    exec(compile(ast.Module(body=selected, type_ignores=[]), TAG_PATH, "exec"), module.__dict__)
    return module


def counting_store(kernel: types.ModuleType, *, nodes: dict[str, bytes] | None = None,
                   verify_only: bool = False):
    class Store(kernel.MemoryCollectionTagNodeStore):
        def __init__(self):
            super().__init__()
            self.counts = Counter()
            if nodes is not None:
                self.nodes = nodes

        def get(self, digest):
            raw = super().get(digest)
            self.counts["node_gets"] += 1
            self.counts["node_read_octets"] += len(raw)
            return raw

        def put(self, digest, raw):
            self.counts["node_puts"] += 1
            self.counts["node_put_octets"] += len(raw)
            if verify_only:
                if self.nodes.get(digest) != raw:
                    raise ValueError("required historical node is absent or differs")
                return
            if digest not in self.nodes:
                self.counts["new_nodes"] += 1
                self.counts["new_node_octets"] += len(raw)
            super().put(digest, raw)
    return Store()


def tag_trial(size: int, long_value_bytes: int = 32, *, digest_prefix: bool = False) -> dict:
    kernel = load_tag_kernel()
    store = counting_store(kernel)
    current = kernel.CollectionTagSet(store)
    keys: list[str] = []
    candidate = 0
    while len(keys) < size:
        key = f"same-human-prefix-{candidate:08d}"
        candidate += 1
        if digest_prefix and hashlib.sha256(key.encode()).digest()[0] != 0:
            continue
        keys.append(key)
        current = current.insert(key)
    setup = dict(store.counts)
    old = current.root
    value = "é" * (long_value_bytes // 2)
    if digest_prefix:
        while True:
            value = f"same-human-prefix-{candidate:08d}"
            candidate += 1
            if hashlib.sha256(value.encode()).digest()[0] == 0:
                break
    store.counts.clear()
    changed = current.insert(value)
    insert = dict(store.counts)
    store.counts.clear()
    restored = changed.discard(value)
    remove = dict(store.counts)
    if restored.root != old:
        raise AssertionError("native persistent root failed to restore")
    # This full enumeration is correctness-oracle work, OUTSIDE point-edit counters.
    if set(kernel.CollectionTagSet(store, old).iter_tags()) != set(keys):
        raise AssertionError("old tag snapshot changed")
    return {"initial_tags": size, "operand_utf8_octets": len(value.encode()),
            "fixture_digest_prefix_bytes": 1 if digest_prefix else 0,
            "fixture_candidates": candidate, "setup": setup, "insert": insert, "remove": remove}


def make_tag_history(size: int, edits: int):
    kernel = load_tag_kernel()
    store = counting_store(kernel)
    current = kernel.CollectionTagSet(store)
    for i in range(size):
        current = current.insert(f"baseline-{i:08d}")
    roots = [current.root]
    operations = []
    for i in range(edits):
        add = i % 2 == 0
        current = current.insert("toggle") if add else current.discard("toggle")
        operations.append(add)
        roots.append(current.root)
    return kernel, store, roots, operations


def verify_tag_history(kernel, source, roots, operations) -> dict:
    verify = counting_store(kernel, nodes=source.nodes, verify_only=True)
    # Cold baseline validation, once. This iterator's output is deliberately consumed.
    for _ in kernel.CollectionTagSet(verify, roots[0]).iter_tags():
        pass
    baseline = dict(verify.counts)
    verify.counts.clear()
    current = kernel.CollectionTagSet(verify, roots[0])
    for expected, add in zip(roots[1:], operations, strict=True):
        current = current.insert("toggle") if add else current.discard("toggle")
        if current.root != expected:
            raise ValueError("snapshot disagrees with exact replay")
    return {"baseline": baseline, "incremental_replay": dict(verify.counts)}


def history_trial(size: int, edits: int) -> dict:
    kernel, source, roots, operations = make_tag_history(size, edits)
    repeated = counting_store(kernel, nodes=source.nodes, verify_only=True)
    for root in roots:
        for _ in kernel.CollectionTagSet(repeated, root).iter_tags():
            pass
    counts = verify_tag_history(kernel, source, roots, operations)
    return {"initial_tags": size, "edits": edits,
            "repeated_snapshot_validation": dict(repeated.counts), **counts}


class StructuralWalk:
    """SQLite-spooled DFS diagnostic. Source IDs are toy integers, NOT archive proofs.

    Source setup is separate; run-step reads indexed rows with bounded output.
    The file is trusted job state. No claim of tamper-proof scratch or power-loss
    testing is made. Source-qualified exact snapshot keys are required in production.
    """
    def __init__(self, path: Path, expected_source: str):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA synchronous=FULL")
        actual = self.db.execute("SELECT value FROM meta WHERE key='source'").fetchone()
        if actual != (expected_source,):
            self.db.close()
            raise ValueError("source/selection binding differs")
        self.vm_steps = 0
        self.db.set_progress_handler(self._tick, 1)

    def _tick(self):
        self.vm_steps += 1
        return 0

    @staticmethod
    def seed(path: Path, graph: dict[int, tuple[int, ...]], roots: tuple[int, ...]) -> str:
        # Finite fixture preparation is not a public ingestion implementation.
        if path.exists():
            raise FileExistsError(path)
        if any(type(k) is not int or k < 0 for k in graph):
            raise ValueError("fixture nodes must be nonnegative integers")
        if any(type(c) is not int or c < 0 for cs in graph.values() for c in cs):
            raise ValueError("fixture children must be nonnegative integers")
        if any(tuple(sorted(set(cs))) != cs for cs in graph.values()):
            raise ValueError("fixture child lists must be unique and ordered")
        if any(type(r) is not int or r < 0 for r in roots):
            raise ValueError("fixture roots must be nonnegative integers")
        source = hashlib.sha256(repr((sorted(graph.items()), roots)).encode()).hexdigest()
        with sqlite3.connect(path) as db:
            db.executescript("""
                CREATE TABLE nodes(id INTEGER PRIMARY KEY);
                CREATE TABLE edges(parent INTEGER, child INTEGER, PRIMARY KEY(parent,child)) WITHOUT ROWID;
                CREATE TABLE roots(position INTEGER PRIMARY KEY, node INTEGER);
                CREATE TABLE marks(node INTEGER PRIMARY KEY, state INTEGER);
                CREATE TABLE stack(depth INTEGER PRIMARY KEY, node INTEGER, after_child INTEGER, entered INTEGER);
                CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT) WITHOUT ROWID;
                CREATE TABLE counts(key TEXT PRIMARY KEY, value INTEGER) WITHOUT ROWID;
            """)
            db.executemany("INSERT INTO nodes VALUES (?)", ((k,) for k in graph))
            db.executemany("INSERT INTO edges VALUES (?,?)", ((k, c) for k, cs in graph.items() for c in cs))
            db.executemany("INSERT INTO roots VALUES (?,?)", enumerate(roots))
            db.execute("INSERT INTO meta VALUES ('source',?)", (source,))
        return source

    def step(self, limit: int) -> bool:
        if type(limit) is not int or limit < 1:
            raise ValueError("positive step budget required")
        counts = Counter()
        with self.db:
            for _ in range(limit):
                frame = self.db.execute("SELECT depth,node,after_child,entered FROM stack ORDER BY depth DESC LIMIT 1").fetchone()
                if frame is None:
                    root = self.db.execute("SELECT position,node FROM roots ORDER BY position LIMIT 1").fetchone()
                    if root is None:
                        break
                    self.db.execute("DELETE FROM roots WHERE position=?", (root[0],))
                    state = self.db.execute("SELECT state FROM marks WHERE node=?", (root[1],)).fetchone()
                    if state != (2,):
                        self.db.execute("INSERT INTO stack VALUES (0,?,-1,0)", (root[1],))
                    counts["root_selections"] += 1
                else:
                    depth, node, after, entered = frame
                    if not entered:
                        if self.db.execute("SELECT 1 FROM nodes WHERE id=?", (node,)).fetchone() is None:
                            raise ValueError("missing structural dependency")
                        self.db.execute("INSERT INTO marks VALUES (?,1)", (node,))
                        self.db.execute("UPDATE stack SET entered=1 WHERE depth=?", (depth,))
                        counts["node_visits"] += 1
                    else:
                        edge = self.db.execute("SELECT child FROM edges WHERE parent=? AND child>? ORDER BY child LIMIT 1", (node, after)).fetchone()
                        if edge is None:
                            self.db.execute("UPDATE marks SET state=2 WHERE node=?", (node,))
                            self.db.execute("DELETE FROM stack WHERE depth=?", (depth,))
                            counts["completed_nodes"] += 1
                        else:
                            child = edge[0]
                            self.db.execute("UPDATE stack SET after_child=? WHERE depth=?", (child, depth))
                            state = self.db.execute("SELECT state FROM marks WHERE node=?", (child,)).fetchone()
                            if state == (1,):
                                raise ValueError("structural dependency cycle")
                            if state is None:
                                self.db.execute("INSERT INTO stack VALUES (?,?, -1,0)", (depth + 1, child))
                            counts["edge_visits"] += 1
                counts["work_units"] += 1
            for name, count in counts.items():
                self.db.execute("INSERT INTO counts VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value=value+excluded.value", (name, count))
            self.db.execute("INSERT INTO counts VALUES ('step_calls',1) ON CONFLICT(key) DO UPDATE SET value=value+1")
            self.db.execute("INSERT INTO counts VALUES ('largest_step',?) ON CONFLICT(key) DO UPDATE SET value=MAX(value,excluded.value)", (counts["work_units"],))
        return self.db.execute("SELECT 1 FROM stack LIMIT 1").fetchone() is None and self.db.execute("SELECT 1 FROM roots LIMIT 1").fetchone() is None

    def counts(self) -> dict[str, int]:
        return dict(self.db.execute("SELECT key,value FROM counts"))

    def close(self):
        self.db.close()


def diamond(layers: int) -> dict[int, tuple[int, ...]]:
    if layers < 1:
        raise ValueError("positive layer count required")
    graph = {0: (1, 2)}
    for i in range(1, layers + 1):
        children = () if i == layers else (2 * i + 1, 2 * i + 2)
        graph[2 * i - 1] = children
        graph[2 * i] = children
    return graph


def graph_trial(layers: int, budget: int = 7, restart: bool = False) -> dict:
    graph = diamond(layers)
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "walk.sqlite3"
        source = StructuralWalk.seed(path, graph, (0, 0))
        walk = StructuralWalk(path, source)
        restarts = vm = 0
        try:
            while not walk.step(budget):
                if restart:
                    vm += walk.vm_steps
                    walk.close()
                    walk = StructuralWalk(path, source)
                    restarts += 1
            result = walk.counts()
            result.update(sqlite_vm_steps=vm + walk.vm_steps, reopen_count=restarts, source_binding_checks=1 + restarts)
        finally:
            walk.close()
    return {"layers": layers, "unique_nodes": len(graph),
            "unique_edges": sum(map(len, graph.values())), "budget": budget, **result}


def chain(n: int, label: str = "one") -> tuple[dict[str, bytes], list[str]]:
    objects = {}
    checkpoints = []
    previous = None
    for i in range(n):
        raw = json.dumps({"label": label, "revision": i, "previous": previous}, sort_keys=True).encode()
        key = hashlib.sha256(raw).hexdigest()
        objects[key] = raw
        checkpoints.append(key)
        previous = key
    return objects, checkpoints


def missing_suffix(objects: dict[str, bytes], head: str, verified_ancestor: str | None) -> int:
    # A receipt for the ancestor must already establish closure under the owner.
    # This toy counts only authenticated suffix discovery; it is NOT replication.
    seen = set()
    current = head
    reads = 0
    while current != verified_ancestor:
        if current is None or current in seen:
            raise ValueError("not a descendant of the verified checkpoint")
        seen.add(current)
        raw = objects[current]
        if hashlib.sha256(raw).hexdigest() != current:
            raise ValueError("changed checkpoint")
        current = json.loads(raw)["previous"]
        reads += 1
    return reads


def load_g22(root: Path = ROOT):
    raw = subprocess.check_output(["git", "show", G22_REF + ":" + G22_PATH], cwd=root, stderr=subprocess.PIPE)
    if hashlib.sha256(raw).hexdigest() != G22_SHA256:
        raise ValueError("G22 diagnostic source differs")
    module = types.ModuleType("riverhog_g22_sha_diagnostic")
    exec(compile(raw, G22_PATH, "exec"), module.__dict__)
    return module.Sha256


def continuation_trial(root: Path = ROOT) -> dict:
    sha = load_g22(root)
    # Exercise residual/padding boundaries; reload the versioned bytes every chunk.
    state = sha()
    native = hashlib.sha256()
    count = restores = max_saved = 0
    for length in (0, 1, 54, 1, 7, 1, 63, 64, 65, 127, 128, 129, 1024):
        raw = bytes(i % 251 for i in range(length))
        state.update(raw)
        native.update(raw)
        saved = state.export()
        max_saved = max(max_saved, len(saved))
        if state.digest() != native.digest() or saved != state.export():
            raise AssertionError("exact prefix or continuation changed during finalization")
        state = sha.restore(saved)
        count += len(raw)
        restores += 1
    return {"source_sha256": G22_SHA256, "input_octets": count,
            "state_restores": restores, "largest_state_octets": max_saved,
            "scope": "existing diagnostic codec and native hash parity; NOT production restart/crypto qualification"}


def report() -> dict:
    records = {"hashes": [hash_trial(n) for n in (16, 32, 64, 128)],
               "tags": [tag_trial(n) for n in (16, 128, 1024)] + [tag_trial(128, 65536), tag_trial(128, digest_prefix=True)],
               "tag_history": [history_trial(128, n) for n in (16, 32, 64, 128)],
               "shared_dag": [graph_trial(n, restart=r) for n in (16, 64, 256) for r in (False, True)]}
    objects, checkpoints = chain(4096)
    records["suffix"] = [{"history": 4096, "lag": lag, "checkpoint_reads": missing_suffix(objects, checkpoints[-1], checkpoints[-lag - 1])} for lag in (0, 1, 16, 128)]
    try:
        records["g22_continuation"] = {"status": "passed", **continuation_trial()}
    except (OSError, subprocess.CalledProcessError) as error:
        records["g22_continuation"] = {"status": "unavailable", "reason": str(error)}
    return {"format": "reference-annotation-growth-observations/v0", "production_conformance": False,
            "environment": {"python": platform.python_version(), "sqlite": sqlite3.sqlite_version},
            "source": {TAG_PATH: TAG_SHA256, "g22_ref": G22_REF,
                       "probe_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                       "tests_sha256": hashlib.sha256(Path(__file__).with_name("test_growth.py").read_bytes()).hexdigest()},
            "records": records,
            "limitations": ["Native tag AST slice only; no module/API/backend conformance.",
                            "Fixed-width hash probes are algorithm counterexamples, not a replay of every old-model call.",
                            "SQLite DFS uses toy structural identities and trusted job state; no archive or power-loss proof.",
                            "Counts exclude fixture generation and oracle checks unless separately named.",
                            "No claims for encryption, provider durability, full core validation, total machine cost or pinned product qualification."]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    data = report()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(data, sort_keys=True, indent=2) + "\n")
    print(f"Wrote diagnostic observations to {args.output}; production_conformance=false")


if __name__ == "__main__":
    main()
