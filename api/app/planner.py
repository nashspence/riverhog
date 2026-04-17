from __future__ import annotations

import json
import math
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
import numpy as np
from scipy.optimize import milp, Bounds, LinearConstraint
from scipy.sparse import lil_array
from sqlalchemy.orm import Session, selectinload
from sqlalchemy import select

from .config import PARTITIONER_STATE_DIR, PARTITION_CFG, PARTITION_ROOTS_DIR
from .models import Disc, DiscEntry, ArchivePiece, Job, JobFile
from .storage import canonical_tree_hash

MANIFEST = "MANIFEST.jsonl"
STORE = "files"
STATE = "state.json"
META_PAD = 512
j = lambda x: (json.dumps(x, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def atomic_json(path: Path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, separators=(",", ":"), ensure_ascii=False))
    tmp.replace(path)


def load_state(state_dir: Path, cfg=None):
    p = state_dir / STATE
    if p.exists():
        s = json.loads(p.read_text())
        s.setdefault("jobs", {})
        s.setdefault("items", [])
        s.setdefault("next_item", 0)
        s.setdefault("last_closed", "")
        return s
    if cfg is None:
        raise RuntimeError(f"no state at {state_dir}")
    s = {"cfg": cfg, "jobs": {}, "items": [], "next_item": 0, "last_closed": ""}
    state_dir.mkdir(parents=True, exist_ok=True)
    atomic_json(p, s)
    return s


def save_state(state_dir: Path, s):
    atomic_json(state_dir / STATE, s)


def ts_name(last: str = ""):
    now = datetime.now(timezone.utc).replace(microsecond=0)
    if last:
        prev = datetime.strptime(last, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
        if now <= prev:
            now = prev + timedelta(seconds=1)
    return now.strftime("%Y%m%dT%H%M%SZ")


def copy_span(src: str, dst: str, off: int = 0, size: int | None = None):
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    if size is None:
        size = os.path.getsize(src) - off
    if off == 0 and size == os.path.getsize(src):
        try:
            os.link(src, dst)
            return
        except OSError:
            shutil.copy2(src, dst)
            return
    with open(src, "rb") as s, open(dst, "wb") as d:
        s.seek(off)
        while size:
            b = s.read(min(1 << 20, size))
            if not b:
                break
            d.write(b)
            size -= len(b)


def sidecar_dict(f: dict, i: int = 0, n: int = 1):
    d = {
        "schema": "sidecar/v1",
        "path": f["rel"],
        "sha256": f["sha256"],
        "size": f["raw"],
        "mode": f["mode"],
        "mtime": f["mtime"],
    }
    if f["uid"] is not None:
        d["uid"] = f["uid"]
    if f["gid"] is not None:
        d["gid"] = f["gid"]
    if n > 1:
        d["part"] = {"index": i + 1, "count": n}
    return d


def sidecar_bytes(f: dict, i: int = 0, n: int = 1):
    return yaml.safe_dump(sidecar_dict(f, i, n), sort_keys=False, allow_unicode=True).encode()


def tree_plan(kids: dict, sizes: dict, cap: int):
    free, parts, stack = [cap], {}, [("", "dir")]
    while stack:
        node, why = stack.pop()
        if node not in kids or sizes[node] <= cap:
            idx = next((i for i, v in enumerate(free) if v >= sizes[node]), len(free))
            if idx == len(free):
                free.append(cap)
            free[idx] -= sizes[node]
            q = parts.setdefault(idx, {"pieces": [], "bytes": 0, "why": why})
            q["bytes"] += sizes[node]
            q.setdefault("nodes", []).append((node, why))
            continue
        order = sorted(kids[node], key=lambda x: (-sizes[x], str(x)))
        stack.extend((c, "spl") for c in reversed(order))
    return [parts[i] for i in sorted(parts)]


def job_lines(job: str, root: str, files: list[dict]):
    head = {"t": "job", "job": job, "root": root, "bytes": sum(f["raw"] for f in files), "files": len(files)}
    alls = [
        {
            "t": "all",
            "job": job,
            "id": f["id"],
            "src": f["rel"],
            "sha256": f["sha256"],
            "size": f["raw"],
            "mode": f["mode"],
            "mtime": f["mtime"],
            "uid": f["uid"],
            "gid": f["gid"],
        }
        for f in files
    ]
    return head, alls, len(j(head)) + sum(len(j(x)) for x in alls)


def put_len(job: str, rel: str, i: int, n: int, why: str = "___"):
    name = f"{STORE}/999999" + ("" if n == 1 else f".{i + 1:03d}")
    row = {"t": "put", "part": "00000000T000000Z", "job": job, "src": rel, "f": name, "m": name + ".meta.yaml", "b": 0, "why": why}
    if n > 1:
        w = max(3, len(str(n)))
        row["chunk"] = f"{i + 1:0{w}d}/{n:0{w}d}"
    return len(j(row))


def stage_pieces(state_dir: Path, job: str, files: list[dict], target: int, fixed: int):
    pool, cap = state_dir / "pool" / job, target - META_PAD - fixed
    if cap <= 0:
        raise RuntimeError(f"job manifest for {job} leaves no payload room")
    pool.mkdir(parents=True, exist_ok=True)
    for f in files:
        stub1 = put_len(job, f["rel"], 0, 1, "job")
        if f["raw"] <= target:
            if f["raw"] + len(sidecar_bytes(f)) + stub1 > cap:
                raise RuntimeError(f"file {f['rel']} in {job} cannot fit with required manifest overhead without forbidden chunking")
            store = pool / str(f["id"])
            copy_span(f["src"], str(store))
            f["pieces"] = [{"job": job, "rel": f["rel"], "file": f["id"], "store": str(store.relative_to(state_dir)), "data": f["raw"], "i": 0, "n": 1, "est": f["raw"] + len(sidecar_bytes(f)) + stub1}]
            continue
        n = max(2, math.ceil(f["raw"] / max(1, cap - len(sidecar_bytes(f, 0, 2)) - put_len(job, f["rel"], 0, 2, "vol"))))
        while True:
            room = cap - len(sidecar_bytes(f, 0, n)) - put_len(job, f["rel"], 0, n, "vol")
            if room <= 0:
                raise RuntimeError(f"chunk sidecar for {f['rel']} in {job} leaves no payload room")
            nn = max(2, math.ceil(f["raw"] / room))
            if nn == n:
                break
            n = nn
        room, pcs, off = cap - len(sidecar_bytes(f, 0, n)) - put_len(job, f["rel"], 0, n, "vol"), [], 0
        w = max(3, len(str(n)))
        for i in range(n):
            b = min(room, f["raw"] - off)
            store = pool / f"{f['id']}.{i + 1:0{w}d}"
            copy_span(f["src"], str(store), off, b)
            pcs.append({"job": job, "rel": f["rel"], "file": f["id"], "store": str(store.relative_to(state_dir)), "data": b, "i": i, "n": n, "est": b + len(sidecar_bytes(f, i, n)) + put_len(job, f["rel"], i, n, "vol")})
            off += b
        f["pieces"] = pcs


def leaves(node, kids):
    stack = [node]
    while stack:
        n = stack.pop()
        if n not in kids:
            yield n
        else:
            stack.extend(reversed(kids[n]))


def split_job(files: list[dict], kids: dict[str, list[str]], dirs: list[str], cap: int):
    kids, sizes, by_rel = ({k: v[:] for k, v in kids.items()}, {}, {f["rel"]: f for f in files})
    for f in files:
        sizes[f["rel"]] = sum(p["est"] for p in f["pieces"])
        if len(f["pieces"]) > 1:
            kids[f["rel"]] = [(f["rel"], p["i"]) for p in f["pieces"]]
        for p in f["pieces"]:
            sizes[(f["rel"], p["i"])] = p["est"]
    for d in reversed(dirs):
        sizes[d] = sum(sizes[c] for c in kids[d])
    by_leaf = {(f["rel"], p["i"]): (f, p) for f in files for p in f["pieces"]}
    parts = tree_plan(kids, sizes, cap)
    out = []
    for q in parts:
        cur = {"pieces": [], "bytes": 0, "why": q["why"]}
        for node, why in q.get("nodes", []):
            for leaf in leaves(node, kids):
                f, p = by_leaf[leaf] if leaf in by_leaf else (by_rel[leaf], by_rel[leaf]["pieces"][0])
                cur["pieces"].append({"job": f["job"], "file": f["id"], "rel": f["rel"], "store": p["store"], "data": p["data"], "i": p["i"], "n": p["n"]})
                cur["bytes"] += p["est"]
                if p["n"] > 1:
                    cur["why"] = "vol"
                elif why == "spl" and cur["why"] == "dir":
                    cur["why"] = "spl"
        out.append(cur)
    return out


def close_threshold(items: list[dict], fill: int, spill: int):
    return spill if any(x["hot"] for x in items) else fill


def _solve(items: list[dict], jobs: dict, cap: int, fill: int, spill: int, force=False):
    js = sorted({x["job"] for x in items})
    n, m = len(items), len(js)
    jix = {j: i for i, j in enumerate(js)}
    bytes_ = np.array([x["bytes"] for x in items], dtype=np.float64)
    hot = np.array([int(x["hot"]) for x in items], dtype=np.float64)
    fixed = np.array([jobs[j]["fixed"] for j in js], dtype=np.float64)
    item_job = np.array([jix[x["job"]] for x in items], dtype=int)
    payload_cap = cap - META_PAD
    diff = fill - spill
    nv, ih = n + m + 1 + int(force), n + m
    idd = ih + 1 if force else None
    rows = 1 + n + m + int(hot.sum()) + 2 + int(not force) + 2 * int(force)
    A = lil_array((rows, nv), dtype=np.float64)
    lo, hi, r = [], [], 0

    A[r, :n] = bytes_
    A[r, n : n + m] = fixed
    lo += [-np.inf]
    hi += [payload_cap]
    r += 1
    for i, jv in enumerate(item_job):
        A[r, i] = 1
        A[r, n + jv] = -1
        lo += [-np.inf]
        hi += [0]
        r += 1
    for jv in range(m):
        idx = np.where(item_job == jv)[0]
        A[r, idx] = -1
        A[r, n + jv] = 1
        lo += [-np.inf]
        hi += [0]
        r += 1
    for i in np.where(hot > 0)[0]:
        A[r, i] = 1
        A[r, ih] = -1
        lo += [-np.inf]
        hi += [0]
        r += 1
    A[r, :n] = -hot
    A[r, ih] = 1
    lo += [-np.inf]
    hi += [0]
    r += 1
    A[r, :n] = 1
    lo += [1]
    hi += [np.inf]
    r += 1
    if force:
        A[r, :n] = bytes_
        A[r, n : n + m] = fixed
        A[r, idd] = -1
        lo += [-np.inf]
        hi += [fill - META_PAD]
        r += 1
        A[r, :n] = -bytes_
        A[r, n : n + m] = -fixed
        A[r, idd] = -1
        lo += [-np.inf]
        hi += [-(fill - META_PAD)]
        r += 1
    else:
        A[r, :n] = bytes_
        A[r, n : n + m] = fixed
        A[r, ih] = diff
        lo += [fill - META_PAD]
        hi += [np.inf]
        r += 1

    integrality = np.ones(nv, dtype=int)
    bounds_lo, bounds_hi = np.zeros(nv), np.ones(nv)
    if force:
        integrality[idd] = 0
        bounds_hi[idd] = np.inf
    cons = LinearConstraint(A.tocsr(), np.array(lo, dtype=np.float64), np.array(hi, dtype=np.float64))
    bounds = Bounds(bounds_lo, bounds_hi)

    def run(c, extra=()):
        if extra:
            aa = lil_array((len(extra), nv), dtype=np.float64)
            lo2, hi2 = [], []
            for rr, (vec, a, b) in enumerate(extra):
                aa[rr] = vec
                lo2.append(a)
                hi2.append(b)
            ec = LinearConstraint(aa.tocsr(), np.array(lo2, dtype=np.float64), np.array(hi2, dtype=np.float64))
            lc = (cons, ec)
        else:
            lc = cons
        res = milp(c=np.array(c, dtype=np.float64), constraints=lc, integrality=integrality, bounds=bounds, options={"mip_rel_gap": 0})
        if not res.success or res.x is None:
            return None
        x = np.rint(res.x).astype(int)
        used = int(META_PAD + bytes_ @ x[:n] + fixed @ x[n : n + m])
        active = [items[i] for i, v in enumerate(x[:n]) if v]
        hotc = int(hot @ x[:n])
        return {"x": x, "active": active, "used": used, "hotc": hotc}

    if force:
        c1 = np.zeros(nv)
        c1[idd] = 1
        a = run(c1)
        if not a:
            return []
        d = int(round(a["x"][idd]))
        v1 = np.zeros(nv)
        v1[idd] = 1
        c2 = np.zeros(nv)
        c2[:n] = -bytes_
        c2[n : n + m] = -fixed
        b = run(c2, [(v1, d, d)])
        return b["active"] if b else a["active"]

    c1 = np.zeros(nv)
    c1[:n] = bytes_
    c1[n : n + m] = fixed
    c1[ih] = diff
    a = run(c1)
    if not a:
        return []
    s = a["used"] - close_threshold(a["active"], fill, spill)
    v1 = np.zeros(nv)
    v1[:n] = bytes_
    v1[n : n + m] = fixed
    v1[ih] = diff
    c2 = np.zeros(nv)
    c2[:n] = -bytes_
    c2[n : n + m] = -fixed
    b = run(c2, [(v1, s, s)])
    if not b:
        return a["active"]
    u = b["used"]
    v2 = np.zeros(nv)
    v2[:n] = bytes_
    v2[n : n + m] = fixed
    c3 = np.zeros(nv)
    c3[:n] = -hot
    c = run(c3, [(v1, s, s), (v2, u - META_PAD, u - META_PAD)])
    return c["active"] if c else b["active"]


def pick(items: list[dict], jobs: dict, cap: int, fill: int, spill: int, force=False):
    return _solve(items, jobs, cap, fill, spill, force) if items else []


def assign_paths(pieces: list[dict]):
    files = sorted({(p["job"], p["file"], p["rel"]) for p in pieces}, key=lambda x: (x[0], x[2], str(x[1])))
    base = {(j, fid): i for i, (j, fid, _) in enumerate(files)}
    out = {}
    for p in pieces:
        k, w = base[(p["job"], p["file"])], max(3, len(str(p["n"])))
        f = f"{STORE}/{k}" + ("" if p["n"] == 1 else f".{p['i'] + 1:0{w}d}")
        out[(p["job"], p["file"], p["i"])] = (f, f + ".meta.yaml")
    return out


def manifest_bytes(part: str, cfg: dict, jobs: dict, items: list[dict], fmap: dict):
    lines = [j({"t": "meta", "v": 11, "part": part, "target": cfg["target"], "fill": cfg["fill"], "spill_fill": cfg["spill_fill"], "store": STORE, "how": "files are raw; concat split files/N.001.. in lexical order to restore"})]
    for job in sorted({x["job"] for x in items}):
        lines += [j(jobs[job]["job_line"])] + [j(x) for x in jobs[job]["all_lines"]]
    for it in items:
        for p in it["pieces"]:
            f, m = fmap[(p["job"], p["file"], p["i"])]
            row = {"t": "put", "part": part, "job": p["job"], "src": p["rel"], "f": f, "m": m, "b": p["data"], "why": "vol" if p["n"] > 1 else it["why"]}
            if p["n"] > 1:
                w = max(3, len(str(p["n"])))
                row["chunk"] = f"{p['i'] + 1:0{w}d}/{p['n']:0{w}d}"
            lines.append(j(row))
    return b"".join(lines)


def build_disc(state_dir: Path, s: dict, items: list[dict], out_dir: Path, emit=True, part: str | None = None):
    part = part or ts_name(s.get("last_closed", ""))
    pieces = [p for it in items for p in it["pieces"]]
    fmap = assign_paths(pieces)
    man = manifest_bytes(part, s["cfg"], s["jobs"], items, fmap)
    payload = 0
    for it in items:
        meta = {f["id"]: f for f in s["jobs"][it["job"]]["files"]}
        for p in it["pieces"]:
            payload += p["data"] + len(sidecar_bytes(meta[p["file"]], p["i"], p["n"]))
    used = len(man) + payload
    if used > s["cfg"]["target"]:
        return None
    root = out_dir / part
    out = {"name": part, "path": str(root.resolve()), "used": used, "free": s["cfg"]["target"] - used, "jobs": sorted({x["job"] for x in items}), "items": [x["id"] for x in items], "pieces": []}
    if not emit:
        return out
    (root / STORE).mkdir(parents=True, exist_ok=True)
    (root / MANIFEST).write_bytes(man)
    for it in items:
        meta = {f["id"]: f for f in s["jobs"][it["job"]]["files"]}
        for p in it["pieces"]:
            src = state_dir / p["store"]
            f, m = fmap[(p["job"], p["file"], p["i"])]
            copy_span(str(src), str(root / f))
            (root / m).write_bytes(sidecar_bytes(meta[p["file"]], p["i"], p["n"]))
            out["pieces"].append({"job": p["job"], "job_file_id": p["file"], "relative_path": p["rel"], "payload_relpath": f, "sidecar_relpath": m, "payload_size_bytes": p["data"], "chunk_index": None if p["n"] == 1 else p["i"] + 1, "chunk_count": None if p["n"] == 1 else p["n"]})
    s["last_closed"] = part
    return out


def gc_state(state_dir: Path, s: dict, done: list[dict]):
    dead = {x["id"] for x in done}
    for it in [x for x in s["items"] if x["id"] in dead]:
        for p in it["pieces"]:
            try:
                (state_dir / p["store"]).unlink()
            except FileNotFoundError:
                pass
    s["items"] = [x for x in s["items"] if x["id"] not in dead]
    live = {x["job"] for x in s["items"]}
    for job in list(s["jobs"]):
        if job not in live:
            shutil.rmtree(state_dir / "pool" / job, ignore_errors=True)
            del s["jobs"][job]


def flush(state_dir: Path, s: dict, out_dir: Path, force=False):
    out = []
    while True:
        cand = pick(s["items"], s["jobs"], s["cfg"]["target"], s["cfg"]["fill"], s["cfg"]["spill_fill"], force)
        forced = force
        if not cand and sum(x["bytes"] for x in s["items"]) > s["cfg"]["buffer_max"] and s["items"]:
            cand, forced = (pick(s["items"], s["jobs"], s["cfg"]["target"], s["cfg"]["fill"], s["cfg"]["spill_fill"], True), True)
        if not cand:
            break
        part = ts_name(s.get("last_closed", ""))
        while cand:
            made = build_disc(state_dir, s, cand, out_dir, False, part)
            if made:
                break
            cand = cand[:-1]
        if not cand:
            break
        req = close_threshold(cand, s["cfg"]["fill"], s["cfg"]["spill_fill"])
        if not forced and made["used"] < req:
            break
        made = build_disc(state_dir, s, cand, out_dir, True, part)
        out.append(made)
        gc_state(state_dir, s, cand)
        if not force:
            continue
    return out


def build_job_structures(job: Job):
    files = []
    kids: dict[str, list] = {"": []}
    dirs = {""}
    explicit_dirs = {d.relative_path for d in job.directories}
    for d in explicit_dirs:
        parts = d.split("/")
        for i in range(1, len(parts) + 1):
            dirs.add("/".join(parts[:i]))
    for jf in sorted(job.files, key=lambda x: x.relative_path):
        if not jf.buffer_abs_path or not Path(jf.buffer_abs_path).exists():
            raise RuntimeError(f"job file {jf.relative_path} is not present in hot buffer")
        rel = jf.relative_path
        for parent in [""] + ["/".join(rel.split("/")[:i]) for i in range(1, len(rel.split("/")) )]:
            dirs.add(parent)
        files.append({"id": jf.id, "job": job.id, "src": jf.buffer_abs_path, "rel": rel, "raw": jf.size_bytes, "mode": jf.mode, "mtime": jf.mtime, "uid": jf.uid, "gid": jf.gid, "sha256": jf.actual_sha256 or jf.expected_sha256})
    for d in sorted(dirs, key=lambda x: (x.count("/"), x)):
        kids.setdefault(d, [])
    for d in sorted(dirs):
        if not d:
            continue
        parent = "/".join(d.split("/")[:-1])
        kids.setdefault(parent, [])
        if d not in kids[parent]:
            kids[parent].append(d)
    for f in files:
        parent = "/".join(f["rel"].split("/")[:-1])
        kids.setdefault(parent, [])
        kids[parent].append(f["rel"])
    for values in kids.values():
        values.sort()
    dir_list = sorted(dirs, key=lambda x: (x.count("/"), x))
    return files, kids, dir_list


def ingest_job(session: Session, job_id: str):
    job = session.execute(select(Job).where(Job.id == job_id).options(selectinload(Job.directories), selectinload(Job.files))).scalar_one()
    if job.status == "sealed":
        raise RuntimeError(f"job {job_id} already sealed")
    files, kids, dirs = build_job_structures(job)
    state_dir = PARTITIONER_STATE_DIR
    out_dir = PARTITION_ROOTS_DIR
    s = load_state(state_dir, PARTITION_CFG)
    if s["cfg"] != PARTITION_CFG:
        raise RuntimeError("partitioner state config mismatch")
    if job_id in s["jobs"] or any(x["job"] == job_id for x in s["items"]):
        raise RuntimeError(f"duplicate job {job_id}")
    if not files:
        job.status = "sealed"
        job.sealed_at = datetime.now(timezone.utc)
        save_state(state_dir, s)
        return {"job": job_id, "closed": [], "buffer_bytes": sum(x["bytes"] for x in s["items"])}

    jl, alls, fixed = job_lines(job_id, job.id, files)
    stage_pieces(state_dir, job_id, files, s["cfg"]["target"], fixed)
    s["jobs"][job_id] = {"root": job.id, "files": [{k: f[k] for k in ("id", "rel", "raw", "mode", "mtime", "uid", "gid", "sha256")} for f in files], "job_line": jl, "all_lines": alls, "fixed": fixed}

    def add_item(kind, hot, why, pieces, b):
        s["next_item"] += 1
        s["items"].append({"id": f"{s['next_item']:08d}", "job": job_id, "kind": kind, "hot": hot, "why": why, "pieces": pieces, "bytes": b})

    total = META_PAD + fixed + sum(p["est"] for f in files for p in f["pieces"])
    if total <= s["cfg"]["target"] and all(f["raw"] <= s["cfg"]["target"] for f in files):
        add_item("job", False, "job", [{"job": job_id, "file": p["file"], "rel": p["rel"], "store": p["store"], "data": p["data"], "i": p["i"], "n": p["n"]} for f in files for p in f["pieces"]], sum(p["est"] for f in files for p in f["pieces"]))
    else:
        for q in split_job(files, kids, dirs, s["cfg"]["target"] - META_PAD - fixed):
            add_item("rem", True, q["why"], q["pieces"], q["bytes"])
    closed = flush(state_dir, s, out_dir)
    job.status = "sealed"
    job.sealed_at = datetime.now(timezone.utc)
    save_state(state_dir, s)
    return {"job": job_id, "closed": closed, "buffer_bytes": sum(x["bytes"] for x in s["items"])}


def force_close_pending(session: Session):
    state_dir = PARTITIONER_STATE_DIR
    out_dir = PARTITION_ROOTS_DIR
    s = load_state(state_dir, PARTITION_CFG)
    closed = flush(state_dir, s, out_dir, force=True)
    save_state(state_dir, s)
    return closed


def import_closed_discs(session: Session, closed: list[dict]) -> list[str]:
    disc_ids: list[str] = []
    for item in closed:
        disc_id = item["name"]
        root = Path(item["path"])
        contents_hash, total_bytes, rows = canonical_tree_hash(root)
        disc = session.get(Disc, disc_id)
        if disc is None:
            disc = Disc(id=disc_id, status="offline", root_abs_path=str(root), contents_hash=contents_hash, total_root_bytes=total_bytes)
            session.add(disc)
        else:
            disc.root_abs_path = str(root)
            disc.contents_hash = contents_hash
            disc.total_root_bytes = total_bytes
        session.flush()
        session.query(DiscEntry).filter(DiscEntry.disc_id == disc_id).delete()
        session.query(ArchivePiece).filter(ArchivePiece.disc_id == disc_id).delete()
        for row in rows:
            kind = "payload"
            rel = row["relative_path"]
            if rel == MANIFEST:
                kind = "manifest"
            elif str(rel).endswith(".meta.yaml"):
                kind = "sidecar"
            session.add(DiscEntry(disc_id=disc_id, relative_path=str(rel), kind=kind, size_bytes=int(row["size_bytes"]), sha256=str(row["sha256"])))
        for p in item.get("pieces", []):
            session.add(ArchivePiece(disc_id=disc_id, job_file_id=p["job_file_id"], payload_relpath=p["payload_relpath"], sidecar_relpath=p["sidecar_relpath"], payload_size_bytes=p["payload_size_bytes"], chunk_index=p["chunk_index"], chunk_count=p["chunk_count"]))
        disc_ids.append(disc_id)
    session.commit()
    return disc_ids
