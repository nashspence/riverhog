import asyncio
import contextlib
import hashlib
import html
import json
import mimetypes
import os
import secrets
import shutil
import sqlite3
import subprocess
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import bagit
import requests
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from minio import Minio
from minio.deleteobjects import DeleteObject

STATE = Path("/state")
PACKAGES = Path("/packages")
REHYDRATED = Path("/rehydrated")
DB = STATE / "archive.db"
CLIENT = None


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def cfg_int(name: str, default: int) -> int:
    return int(env(name, str(default)))


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    STATE.mkdir(parents=True, exist_ok=True)
    PACKAGES.mkdir(parents=True, exist_ok=True)
    REHYDRATED.mkdir(parents=True, exist_ok=True)
    with db() as conn:
        conn.executescript(
            """
            pragma journal_mode = wal;
            create table if not exists files (
              id integer primary key autoincrement,
              original_name text not null,
              content_type text,
              size integer not null,
              sha256 text not null,
              object_key text not null,
              status text not null default 'staged',
              package_id integer,
              created_at text not null,
              foreign key(package_id) references packages(id)
            );
            create table if not exists packages (
              id integer primary key autoincrement,
              package_key text not null unique,
              iso_name text,
              iso_sha256 text,
              status text not null default 'sealed',
              bytes integer not null default 0,
              file_count integer not null default 0,
              manifest_sha256 text,
              burned_at text,
              created_at text not null,
              rehydrated_at text
            );
            create index if not exists idx_files_status on files(status);
            create index if not exists idx_files_package on files(package_id);
            """
        )


def s3() -> Minio:
    global CLIENT
    if CLIENT is None:
        CLIENT = Minio(
            env("S3_ENDPOINT", "garage:3900"),
            access_key=env("S3_ACCESS_KEY"),
            secret_key=env("S3_SECRET_KEY"),
            secure=env("S3_SECURE", "false").lower() == "true",
            region=env("S3_REGION", "garage"),
        )
        bucket = env("S3_BUCKET", "archive")
        if not CLIENT.bucket_exists(bucket):
            CLIENT.make_bucket(bucket)
    return CLIENT


def stage_bytes() -> int:
    with db() as conn:
        row = conn.execute(
            "select coalesce(sum(size), 0) n from files where status = 'staged'"
        ).fetchone()
    return int(row["n"])


def object_key(sha256: str) -> str:
    return f"objects/{sha256[:2]}/{sha256}"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def list_staged() -> list[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            "select * from files where status = 'staged' order by size desc, id asc"
        ).fetchall()


def choose_batch(limit: int) -> list[sqlite3.Row]:
    picked, used = [], 0
    for row in list_staged():
        if used + row["size"] <= limit or not picked:
            picked.append(row)
            used += row["size"]
        if used >= limit:
            break
    return picked


def run(*cmd: str, cwd: Path | None = None) -> None:
    subprocess.run(cmd, cwd=cwd, check=True)


def notify(payload: dict) -> None:
    if not env("HOMEASSISTANT_WEBHOOK"):
        return
    with contextlib.suppress(Exception):
        requests.post(env("HOMEASSISTANT_WEBHOOK"), json=payload, timeout=10)


def run_age_batchpass(*cmd: str, passphrase: str) -> None:
    rfd, wfd = os.pipe()
    try:
        os.write(wfd, passphrase.encode("utf-8"))
        os.close(wfd)
        wfd = None

        proc = subprocess.run(
            cmd,
            check=False,
            env={**os.environ, "AGE_PASSPHRASE_FD": str(rfd)},
            pass_fds=(rfd,),
            capture_output=True,
            text=True,
        )
    finally:
        with contextlib.suppress(OSError):
            if wfd is not None:
                os.close(wfd)
        with contextlib.suppress(OSError):
            os.close(rfd)

    if proc.returncode != 0:
        msg = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(
            msg or f"age command failed with exit code {proc.returncode}"
        )


def age_encrypt(infile: Path, outfile: Path, passphrase: str) -> None:
    run_age_batchpass(
        "age",
        "-e",
        "-j",
        "batchpass",
        "-o",
        str(outfile),
        str(infile),
        passphrase=passphrase,
    )


def age_decrypt(infile: Path, outfile: Path, passphrase: str) -> None:
    run_age_batchpass(
        "age",
        "-d",
        "-j",
        "batchpass",
        "-o",
        str(outfile),
        str(infile),
        passphrase=passphrase,
    )


def make_manifest(package_key: str, rows: Iterable[sqlite3.Row]) -> dict:
    rows = list(rows)
    return {
        "package": package_key,
        "created_at": now(),
        "bucket": env("S3_BUCKET", "archive"),
        "archive_blob": "archive.tar.age",
        "files": [
            {
                "id": row["id"],
                "original_name": row["original_name"],
                "content_type": row["content_type"],
                "size": row["size"],
                "sha256": row["sha256"],
                "object_key": row["object_key"],
                "payload_path": f"data/payload/{row['sha256']}",
            }
            for row in rows
        ],
    }


def package_manifest(iso_sha: str) -> dict:
    with db() as conn:
        pkg = conn.execute(
            "select * from packages where iso_sha256 = ?", (iso_sha,)
        ).fetchone()
        if not pkg:
            raise HTTPException(404, "disc not found")
        files = conn.execute(
            "select id, original_name, content_type, size, sha256, object_key, status, created_at from files where package_id = ? order by id",
            (pkg["id"],),
        ).fetchall()
    base = env("BASE_URL", "http://localhost:8000").rstrip("/")
    return {
        "disc": {
            "package_id": pkg["id"],
            "package_key": pkg["package_key"],
            "iso_sha256": pkg["iso_sha256"],
            "status": pkg["status"],
            "bytes": pkg["bytes"],
            "file_count": pkg["file_count"],
            "manifest_sha256": pkg["manifest_sha256"],
            "created_at": pkg["created_at"],
            "burned_at": pkg["burned_at"],
            "rehydrated_at": pkg["rehydrated_at"],
            "url": f"{base}/d/{pkg['iso_sha256']}",
        },
        "files": [dict(r) for r in files],
    }


def build_iso(rows: list[sqlite3.Row]) -> sqlite3.Row:
    if not rows:
        raise HTTPException(409, "no staged files available")
    passphrase = env("DISC_PASSPHRASE")
    if not passphrase:
        raise RuntimeError("DISC_PASSPHRASE is required")
    package_key = f"pkg-{secrets.token_hex(6)}"
    bucket = env("S3_BUCKET", "archive")
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        clear_root = td / package_key
        payload = clear_root / "payload"
        payload.mkdir(parents=True)
        manifest = clear_root / "manifest.json"
        manifest.write_text(
            json.dumps(make_manifest(package_key, rows), indent=2) + "\n"
        )
        run("ots", "stamp", str(manifest))
        manifest_sha = hashlib.sha256(manifest.read_bytes()).hexdigest()
        for row in rows:
            s3().fget_object(bucket, row["object_key"], str(payload / row["sha256"]))
        bagit.make_bag(str(clear_root), checksums=["sha256"])
        clear_tar = td / f"{package_key}.tar"
        with tarfile.open(clear_tar, "w") as tar:
            tar.add(clear_root, arcname=package_key)
        encrypted_blob = td / "archive.tar.age"
        age_encrypt(clear_tar, encrypted_blob, passphrase)
        iso_root = td / "iso-root"
        iso_root.mkdir()
        shutil.copy2(encrypted_blob, iso_root / encrypted_blob.name)
        iso_tmp = td / f"{package_key}.iso"
        run(
            "xorriso",
            "-as",
            "mkisofs",
            "-R",
            "-J",
            "-V",
            package_key,
            "-o",
            str(iso_tmp),
            str(iso_root),
        )
        iso_sha = sha256_file(iso_tmp)
        final_iso = PACKAGES / f"{iso_sha}.iso"
        shutil.move(iso_tmp, final_iso)
        with db() as conn:
            cur = conn.execute(
                """
                insert into packages(package_key, iso_name, iso_sha256, status, bytes, file_count, manifest_sha256, created_at)
                values (?, ?, ?, 'sealed', ?, ?, ?, ?)
                """,
                (
                    package_key,
                    final_iso.name,
                    iso_sha,
                    sum(r["size"] for r in rows),
                    len(rows),
                    manifest_sha,
                    now(),
                ),
            )
            package_id = cur.lastrowid
            conn.executemany(
                "update files set status = 'sealed', package_id = ? where id = ?",
                [(package_id, r["id"]) for r in rows],
            )
            row = conn.execute(
                "select * from packages where id = ?", (package_id,)
            ).fetchone()
    base = env("BASE_URL", "http://localhost:8000").rstrip("/")
    notify(
        {
            "event": "archive_disc_ready",
            "package_id": row["id"],
            "iso_sha256": row["iso_sha256"],
            "bytes": row["bytes"],
            "file_count": row["file_count"],
            "download_url": f"{base}/packages/{row['id']}/download",
            "disc_url": f"{base}/d/{row['iso_sha256']}",
            "stage_bytes_remaining": stage_bytes(),
        }
    )
    return row


def maybe_seal(force: bool = False) -> sqlite3.Row | None:
    limit = cfg_int("ISO_TARGET_BYTES", 50 * 1024**3) - cfg_int(
        "ISO_RESERVE_BYTES", 512 * 1024**2
    )
    if limit <= 0:
        raise RuntimeError("ISO_TARGET_BYTES must be larger than ISO_RESERVE_BYTES")
    if not force and stage_bytes() <= cfg_int("STAGE_THRESHOLD_BYTES", 100 * 1024**3):
        return None
    rows = choose_batch(limit)
    return build_iso(rows) if rows else None


def get_package(package_id: int) -> sqlite3.Row:
    with db() as conn:
        row = conn.execute(
            "select * from packages where id = ?", (package_id,)
        ).fetchone()
    if not row:
        raise HTTPException(404, "package not found")
    return row


def delete_staged_objects(package_id: int) -> None:
    with db() as conn:
        rows = conn.execute(
            "select object_key from files where package_id = ?", (package_id,)
        ).fetchall()
    errs = list(
        s3().remove_objects(
            env("S3_BUCKET", "archive"), [DeleteObject(r["object_key"]) for r in rows]
        )
    )
    if errs:
        raise RuntimeError(str(errs[0]))


def extract_iso(iso_path: Path, outdir: Path) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    run(
        "xorriso",
        "-osirrox",
        "on",
        "-indev",
        str(iso_path),
        "-extract",
        "/",
        str(outdir),
    )


def unpack_archive(iso_path: Path, passphrase: str) -> tuple[dict, Path]:
    with tempfile.TemporaryDirectory() as td:
        td = Path(td)
        iso_root = td / "iso"
        extract_iso(iso_path, iso_root)
        enc = iso_root / "archive.tar.age"
        if not enc.exists():
            raise HTTPException(400, "iso does not contain archive.tar.age")
        tar_path = td / "archive.tar"
        age_decrypt(enc, tar_path, passphrase)
        work = Path(tempfile.mkdtemp(prefix="rehydrate-", dir=REHYDRATED))
        with tarfile.open(tar_path, "r") as tar:
            tar.extractall(work)
        roots = [p for p in work.iterdir() if p.is_dir()]
        if len(roots) != 1:
            raise HTTPException(400, "archive layout is invalid")
        bag = bagit.Bag(str(roots[0]))
        bag.validate()
        manifest = json.loads((roots[0] / "data" / "manifest.json").read_text())
        return manifest, roots[0]


def upload_rehydrated(manifest: dict, root: Path) -> None:
    bucket = env("S3_BUCKET", "archive")
    for item in manifest["files"]:
        src = root / item["payload_path"]
        s3().fput_object(
            bucket,
            item["object_key"],
            str(src),
            content_type=item.get("content_type") or "application/octet-stream",
        )


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    for _ in range(60):
        with contextlib.suppress(Exception):
            s3()
            break
        await asyncio.sleep(2)

    async def watcher() -> None:
        while True:
            try:
                while stage_bytes() > cfg_int("STAGE_THRESHOLD_BYTES", 100 * 1024**3):
                    if not maybe_seal(force=True):
                        break
            except Exception as e:
                print(f"watcher error: {e}")
            await asyncio.sleep(cfg_int("POLL_SECONDS", 60))

    task = asyncio.create_task(watcher())
    try:
        yield
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


app = FastAPI(title="archive-stager", version="0.2.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    return {
        "ok": True,
        "stage_bytes": stage_bytes(),
        "bucket": env("S3_BUCKET", "archive"),
    }


@app.get("/files")
def files() -> list[dict]:
    with db() as conn:
        rows = conn.execute("select * from files order by id desc").fetchall()
    return [dict(r) for r in rows]


@app.get("/packages")
def packages() -> list[dict]:
    base = env("BASE_URL", "http://localhost:8000").rstrip("/")
    with db() as conn:
        rows = conn.execute("select * from packages order by id desc").fetchall()
    return [{**dict(r), "disc_url": f"{base}/d/{r['iso_sha256']}"} for r in rows]


@app.post("/ingest")
async def ingest(
    file: UploadFile = File(...), original_name: str | None = Form(default=None)
) -> dict:
    name = original_name or file.filename or f"upload-{int(time.time())}"
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        h = hashlib.sha256()
        size = 0
        while chunk := await file.read(1024 * 1024):
            tmp.write(chunk)
            h.update(chunk)
            size += len(chunk)
        tmp_path = Path(tmp.name)
    sha = h.hexdigest()
    key = object_key(sha)
    s3().fput_object(
        env("S3_BUCKET", "archive"),
        key,
        str(tmp_path),
        content_type=file.content_type
        or mimetypes.guess_type(name)[0]
        or "application/octet-stream",
    )
    tmp_path.unlink(missing_ok=True)
    with db() as conn:
        cur = conn.execute(
            "insert into files(original_name, content_type, size, sha256, object_key, status, created_at) values (?, ?, ?, ?, ?, 'staged', ?)",
            (name, file.content_type, size, sha, key, now()),
        )
        row = conn.execute(
            "select * from files where id = ?", (cur.lastrowid,)
        ).fetchone()
    return dict(row)


@app.post("/seal")
def seal() -> dict:
    row = maybe_seal(force=True)
    if not row:
        raise HTTPException(409, "nothing available to package")
    base = env("BASE_URL", "http://localhost:8000").rstrip("/")
    return {**dict(row), "disc_url": f"{base}/d/{row['iso_sha256']}"}


@app.get("/packages/{package_id}/download")
def download(package_id: int):
    row = get_package(package_id)
    if not row["iso_name"]:
        raise HTTPException(404, "iso already disposed")
    path = PACKAGES / row["iso_name"]
    if not path.exists():
        raise HTTPException(404, "iso file not found")
    return FileResponse(path, filename=path.name, media_type="application/octet-stream")


@app.post("/packages/{package_id}/burned")
def burned(package_id: int) -> dict:
    row = get_package(package_id)
    if row["status"] == "burned":
        return dict(row)
    delete_staged_objects(package_id)
    if row["iso_name"]:
        (PACKAGES / row["iso_name"]).unlink(missing_ok=True)
    with db() as conn:
        conn.execute(
            "update packages set status = ?, burned_at = ?, iso_name = null where id = ?",
            ("burned", now(), package_id),
        )
        conn.execute(
            "update files set status = 'burned' where package_id = ?", (package_id,)
        )
        row = conn.execute(
            "select * from packages where id = ?", (package_id,)
        ).fetchone()
    return dict(row)


@app.post("/rehydrate")
async def rehydrate(
    file: UploadFile = File(...), passphrase: str | None = Form(default=None)
) -> dict:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".iso") as tmp:
        h = hashlib.sha256()
        while chunk := await file.read(1024 * 1024):
            tmp.write(chunk)
            h.update(chunk)
        iso_path = Path(tmp.name)
    iso_sha = h.hexdigest()
    with db() as conn:
        row = conn.execute(
            "select * from packages where iso_sha256 = ?", (iso_sha,)
        ).fetchone()
    if not row:
        iso_path.unlink(missing_ok=True)
        raise HTTPException(404, "iso hash is unknown")
    if row["status"] not in {"burned", "rehydrated"}:
        iso_path.unlink(missing_ok=True)
        raise HTTPException(409, "package is not in an offline state")
    manifest, root = unpack_archive(iso_path, passphrase or env("DISC_PASSPHRASE"))
    upload_rehydrated(manifest, root)
    shutil.rmtree(root.parent, ignore_errors=True)
    iso_path.unlink(missing_ok=True)
    with db() as conn:
        conn.execute(
            "update packages set status = ?, rehydrated_at = ? where id = ?",
            ("rehydrated", now(), row["id"]),
        )
        row = conn.execute(
            "select * from packages where id = ?", (row["id"],)
        ).fetchone()
    return {
        "package": dict(row),
        "bucket": env("S3_BUCKET", "archive"),
        "keys": [f["object_key"] for f in manifest["files"]],
    }


@app.delete("/rehydrated/{package_id}")
def unrehydrate(package_id: int) -> dict:
    row = get_package(package_id)
    with db() as conn:
        files = conn.execute(
            "select object_key from files where package_id = ?", (package_id,)
        ).fetchall()
    errs = list(
        s3().remove_objects(
            env("S3_BUCKET", "archive"), [DeleteObject(r["object_key"]) for r in files]
        )
    )
    if errs:
        raise HTTPException(500, str(errs[0]))
    new_status = "burned" if row["burned_at"] else "sealed"
    with db() as conn:
        conn.execute(
            "update packages set status = ?, rehydrated_at = null where id = ?",
            (new_status, package_id),
        )
        row = conn.execute(
            "select * from packages where id = ?", (package_id,)
        ).fetchone()
    return dict(row)


@app.get("/d/{iso_sha}.json")
def disc_json(iso_sha: str) -> dict:
    return package_manifest(iso_sha)


@app.get("/d/{iso_sha}", response_class=HTMLResponse)
def disc_page(iso_sha: str) -> str:
    manifest = package_manifest(iso_sha)
    disc = manifest["disc"]
    rows = "".join(
        f"<tr><td>{f['id']}</td><td>{html.escape(f['original_name'])}</td><td>{html.escape(f.get('content_type') or '')}</td><td>{f['size']}</td><td><code>{f['sha256']}</code></td></tr>"
        for f in manifest["files"]
    )
    return f"""
<!doctype html>
<html>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>Disc {disc["iso_sha256"][:12]}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem auto; max-width: 1100px; padding: 0 1rem; line-height: 1.45; }}
    code {{ font-family: ui-monospace, monospace; word-break: break-all; }}
    table {{ border-collapse: collapse; width: 100%; }}
    th, td {{ border-bottom: 1px solid #ddd; padding: .5rem; text-align: left; vertical-align: top; }}
    .meta {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: .5rem 1rem; margin: 1rem 0 2rem; }}
  </style>
</head>
<body>
  <h1>Archived disc</h1>
  <div class='meta'>
    <div><strong>Package</strong><br>{html.escape(disc["package_key"])}</div>
    <div><strong>ISO SHA-256</strong><br><code>{disc["iso_sha256"]}</code></div>
    <div><strong>Status</strong><br>{html.escape(disc["status"])}</div>
    <div><strong>Bytes</strong><br>{disc["bytes"]}</div>
    <div><strong>Files</strong><br>{disc["file_count"]}</div>
    <div><strong>Manifest SHA-256</strong><br><code>{disc["manifest_sha256"]}</code></div>
    <div><strong>Created</strong><br>{html.escape(disc["created_at"])}</div>
    <div><strong>Burned</strong><br>{html.escape(disc["burned_at"] or "")}</div>
    <div><strong>Rehydrated</strong><br>{html.escape(disc["rehydrated_at"] or "")}</div>
  </div>
  <p><a href='/d/{disc["iso_sha256"]}.json'>JSON manifest</a></p>
  <table>
    <thead><tr><th>ID</th><th>Original name</th><th>Content type</th><th>Bytes</th><th>SHA-256</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</body>
</html>
"""
