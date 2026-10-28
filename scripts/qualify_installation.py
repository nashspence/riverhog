#!/usr/bin/env python3
"""Qualify final-version Riverhog uv-tool artifacts through a staged HTTP origin."""

from __future__ import annotations

import argparse
import hashlib
import http.server
import io
import json
import os
import re
import shutil
import signal
import stat
import string
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET
import zipfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import release
import release_installation as installation

ROOT = Path(__file__).resolve().parents[1]
FORMAT = "riverhog-installation-qualification/v1"
EVENT_SUBJECT = "1"
NATIVE_TRACE_INTERVAL_SECONDS = 1.0
NATIVE_TRACE_EVENT_LIMIT = 128
GOGURT_QUALIFICATION_ACTION_FAILURE_EXIT = 73
MACOS_QUALIFICATION_BUSY_DELAYS_SECONDS = (0.25, 0.5, 1.0, 2.0)
WINDOWS_TASK_XML_NAMESPACE = "http://schemas.microsoft.com/windows/2004/02/mit/task"
RIVERHOG_EVENT_PAGE = {
    "events": [
        {
            "id": "00000000-0000-4000-8000-000000000497",
            "type": "io.riverhog.riverhog.collection.finalized",
            "subject": EVENT_SUBJECT,
            "occurred_at": "2026-08-14T00:00:00.000000000Z",
            "payload": {
                "actor": {"principal_id": "riverhog-installation-qualification"},
                "initiator": {"principal_id": "riverhog-installation-qualification"},
                "collection_id": "1",
                "collection_created_at": "2026-08-14T00:00:00.000000000Z",
                "files_total": 1,
                "bytes_total": 1,
                "archive_root_sha256": "1" * 64,
            },
        }
    ],
    "next_cursor": "1",
    "has_more": False,
}
STOVE0_WORK_ID = "a" * 64
STOVE0_EVENT_PAGE = {
    "events": [
        {
            "id": "00000000-0000-4000-8000-000000000498",
            "type": "io.riverhog.stove0.work.created",
            "subject": STOVE0_WORK_ID,
            "occurred_at": "2026-08-14T00:00:00.000000000Z",
            "payload": {
                "work_id": STOVE0_WORK_ID,
                "phase": "eligible",
                "parent_work_id": None,
                "branch_set_sha256": None,
                "join_plan_sha256": None,
            },
        }
    ],
    "next_cursor": "1",
    "has_more": False,
}


class QualificationError(RuntimeError):
    """The staged installation differs from the v1 release contract."""


@dataclass(frozen=True)
class ListenerMountFixture:
    mount_point: Path
    setup: tuple[str, ...]
    teardown: tuple[str, ...]
    lock_file: Path | None = None


def _load_listener_mount_fixture(
    path: Path, *, lock_file: Path | None = None
) -> ListenerMountFixture:
    if not sys.platform.startswith("linux"):
        raise QualificationError("an operator mount fixture requires Linux")
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict) or set(value) != {"mount_point", "setup", "teardown"}:
        raise QualificationError("mount fixture requires mount_point, setup and teardown")
    point = value["mount_point"]
    if not isinstance(point, str) or not point or "\0" in point:
        raise QualificationError("mount fixture mount_point must be an absolute directory")
    mount_point = Path(point)
    if (
        not mount_point.is_absolute()
        or not mount_point.is_dir()
        or mount_point != mount_point.resolve()
    ):
        raise QualificationError("mount fixture requires an existing canonical absolute directory")
    for key in ("setup", "teardown"):
        argv = value[key]
        if (
            not isinstance(argv, list)
            or not argv
            or any(not isinstance(arg, str) or not arg or "\0" in arg for arg in argv)
        ):
            raise QualificationError(f"mount fixture {key} must be a nonempty argument list")
    if lock_file is not None and (
        not lock_file.is_absolute()
        or not lock_file.parent.is_dir()
        or lock_file != lock_file.resolve()
    ):
        raise QualificationError("mount fixture lock requires a canonical absolute file path")
    return ListenerMountFixture(
        mount_point, tuple(value["setup"]), tuple(value["teardown"]), lock_file
    )


class QualificationHandler(http.server.SimpleHTTPRequestHandler):
    requests: list[str] = []

    def do_GET(self) -> None:  # noqa: N802 - stdlib handler interface
        parsed = urllib.parse.urlsplit(self.path)
        type(self).requests.append(parsed.path)
        if parsed.path == "/v1/events":
            event_page = (
                STOVE0_EVENT_PAGE
                if self.headers.get("Authorization") == "Bearer qualification-token"
                else RIVERHOG_EVENT_PAGE
            )
            payload = json.dumps(event_page, sort_keys=True).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        super().do_GET()

    def log_message(self, _format: str, *_args: object) -> None:
        return


def _run(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str] | None = None,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=env,
        check=False,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    if completed.returncode != 0:
        detail = (
            (completed.stderr or "").strip()
            or (completed.stdout or "").strip()
            or "no captured diagnostic"
        )
        raise QualificationError(
            f"{Path(command[0]).name} exited {completed.returncode}: {detail[-4000:]}"
        )
    return completed


def _qualification_mount_operation(
    operation: str,
    command: list[str],
    *,
    cwd: Path,
) -> None:
    try:
        completed = subprocess.run(
            command,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
            stdin=subprocess.DEVNULL,
            timeout=30,
        )
    except subprocess.TimeoutExpired as exc:
        raise QualificationError(
            f"{operation} timed out after {exc.timeout} seconds; command={command!r}"
        ) from exc
    if completed.returncode == 0:
        return
    stdout = (completed.stdout or "").strip()[-2000:] or "<empty>"
    stderr = (completed.stderr or "").strip()[-2000:] or "<empty>"
    raise QualificationError(
        f"{operation} failed with exit {completed.returncode}; "
        f"command={command!r}; stdout={stdout!r}; stderr={stderr!r}"
    )


def _create_macos_qualification_image(image: Path, *, cwd: Path) -> None:
    """Create a disposable image through transient Disk Arbitration contention."""

    command = [
        "hdiutil",
        "create",
        "-ov",
        "-size",
        "16m",
        "-fs",
        "HFS+",
        "-volname",
        "GogurtQualification",
        str(image),
    ]
    completed: subprocess.CompletedProcess[str] | None = None
    attempts = 0
    for delay in (*MACOS_QUALIFICATION_BUSY_DELAYS_SECONDS, None):
        attempts += 1
        completed = subprocess.run(
            command,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode == 0:
            return
        resource_busy = (
            completed.returncode == 1 and "resource busy" in (completed.stderr or "").casefold()
        )
        if not resource_busy or delay is None:
            break
        time.sleep(delay)

    assert completed is not None
    stdout = (completed.stdout or "").strip()[-2000:] or "<empty>"
    stderr = (completed.stderr or "").strip()[-2000:] or "<empty>"
    raise QualificationError(
        "macOS qualification disk-image creation "
        f"failed with exit {completed.returncode} after {attempts} attempt(s); "
        f"command={command!r}; stdout={stdout!r}; stderr={stderr!r}"
    )


def _detach_macos_qualification_image(backing: Path, *, cwd: Path) -> None:
    """Release a settled disposable image through transient Disk Arbitration lag."""

    command = ["hdiutil", "detach", "-force", str(backing)]
    completed: subprocess.CompletedProcess[str] | None = None
    attempts = 0
    saw_resource_busy = False
    for delay in (*MACOS_QUALIFICATION_BUSY_DELAYS_SECONDS, None):
        attempts += 1
        completed = subprocess.run(
            command,
            cwd=cwd,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode == 0:
            return
        stderr = completed.stderr or ""
        if (
            saw_resource_busy
            and completed.returncode == 1
            and "no such file or directory" in stderr.casefold()
            and not backing.exists()
        ):
            return
        resource_busy = completed.returncode == 16 and "resource busy" in stderr.casefold()
        saw_resource_busy = saw_resource_busy or resource_busy
        if not resource_busy or delay is None:
            break
        time.sleep(delay)

    assert completed is not None
    stdout = (completed.stdout or "").strip()[-2000:] or "<empty>"
    stderr = (completed.stderr or "").strip()[-2000:] or "<empty>"
    raise QualificationError(
        "macOS qualification disk-image detachment "
        f"failed with exit {completed.returncode} after {attempts} attempt(s); "
        f"command={command!r}; stdout={stdout!r}; stderr={stderr!r}"
    )


@contextmanager
def _settled_qualification_mount(
    backing: Path,
    cleanup: Callable[[], None],
) -> Iterator[Path]:
    try:
        yield backing
    except BaseException as body_error:
        try:
            cleanup()
        except BaseException as cleanup_error:
            body_diagnostic = f"{type(body_error).__name__}: {body_error}"[-2000:]
            cleanup_diagnostic = f"{type(cleanup_error).__name__}: {cleanup_error}"[-4000:]
            raise QualificationError(
                f"qualification failed with {body_diagnostic}; "
                f"mount cleanup also failed with {cleanup_diagnostic}"
            ) from body_error
        raise
    else:
        cleanup()


@contextmanager
def _qualification_listener_mount(
    scratch: Path,
    *,
    observe_failure: Callable[[BaseException], None],
    settle_listener: Callable[[], None],
    fixture: ListenerMountFixture | None = None,
) -> Iterator[Path]:
    """Retain listener failure truth and settle its custody before releasing the mount."""

    with _qualification_mount(scratch, fixture=fixture) as mount:
        try:
            yield mount
        except BaseException as exc:
            observe_failure(exc)
            raise
        finally:
            settle_listener()


def _source_checkout(root: Path, destination: Path, source_sha: str) -> None:
    archive = subprocess.run(
        ["git", "archive", "--format=tar", source_sha],
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
    ).stdout
    destination.mkdir()
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as stream:
        stream.extractall(destination, filter="data")


def _wheel_records(
    checkout: Path, projects: list[release.Project], version: str
) -> list[dict[str, Any]]:
    validated = release._validate_distribution_artifacts(checkout, projects, version=version)
    records: list[dict[str, Any]] = []
    for filename, (project, _dependencies) in sorted(validated.items()):
        if not filename.endswith(".whl"):
            continue
        path = checkout / "dist" / filename
        records.append(
            {
                "kind": "wheel",
                "name": f"python/{filename}",
                "sha256": installation.sha256_file(path),
                "size": path.stat().st_size,
                "distribution": project.name,
            }
        )
    return records


def _build_distributions(checkout: Path, projects: list[release.Project]) -> None:
    dist = checkout / "dist"
    shutil.rmtree(dist, ignore_errors=True)
    dist.mkdir()
    for project in projects:
        _run(
            [
                "uv",
                "--no-config",
                "build",
                "--package",
                project.name,
                "--no-create-gitignore",
            ],
            cwd=checkout,
        )


def _require_client_native_module(wheel: Path) -> None:
    with zipfile.ZipFile(wheel) as archive:
        modules = [
            name
            for name in archive.namelist()
            if name.startswith("riverhog_client/_prepared_primary_native.")
            and name.endswith(".pyd")
        ]
    if len(modules) != 1:
        raise QualificationError(
            "staged Windows client wheel has no unique native write-bank module"
        )


def _stage_artifacts(
    root: Path,
    scratch: Path,
    *,
    version: str,
    source_sha: str,
    base_url: str,
) -> dict[str, Any]:
    checkout = scratch / "checkout"
    _source_checkout(root, checkout, source_sha)
    release.apply_release_version(checkout, version)
    _run(["uv", "lock", "--offline"], cwd=checkout)
    projects = release.validate_release_contract(checkout, expected_version=version)
    _build_distributions(checkout, projects)
    wheel_records = _wheel_records(checkout, projects, version)
    platform = _installation_platform()

    web_root = scratch / "web"
    assets = web_root / "assets"
    assets.mkdir(parents=True)
    for wheel in (checkout / "dist").glob("*.whl"):
        if platform == "windows-x64" and wheel.name.startswith("riverhog_client-"):
            _require_client_native_module(wheel)
        shutil.copy2(wheel, assets / wheel.name)

    output = scratch / "installation"
    output.mkdir()
    config = release._load_config(checkout)
    source_epoch = int(
        _run(
            ["git", "show", "-s", "--format=%ct", source_sha],
            cwd=root,
            capture=True,
        ).stdout.strip()
    )
    manifest, _records = installation.build_installation_artifacts(
        checkout,
        output,
        projects,
        wheel_records,
        version=version,
        source_sha=source_sha,
        source_epoch=source_epoch,
        repository=str(config["governance"]["repository"]),
        simple_index_path=str(config["installation"]["simple_index_path"]),
        platforms=(platform,),
        asset_base_url=base_url + "/assets/",
        index_base_url=base_url
        + "/"
        + str(config["installation"]["simple_index_path"]).format(version=version),
    )
    installation.verify_installation_artifacts(output, manifest, platforms=(platform,))
    for lock in (output / "installation").glob("pylock.*.toml"):
        shutil.copy2(lock, assets / lock.name)
    snapshot = output / str(manifest["index"]["snapshot_path"])
    with tarfile.open(snapshot, mode="r:gz") as archive:
        archive.extractall(web_root, filter="data")
    return manifest


def _server(web_root: Path) -> tuple[http.server.ThreadingHTTPServer, threading.Thread, str]:
    handler = lambda *args, **kwargs: QualificationHandler(  # noqa: E731
        *args, directory=str(web_root), **kwargs
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    address = server.server_address
    host, port = str(address[0]), int(address[1])
    return server, thread, f"http://{host}:{port}"


def _tool_environment(scratch: Path, root: str) -> dict[str, str]:
    environment = os.environ.copy()
    for name in (
        "PYTHONHOME",
        "PYTHONPATH",
        "UV_CONSTRAINT",
        "UV_DEFAULT_INDEX",
        "UV_FIND_LINKS",
        "UV_INDEX",
        "UV_INDEX_URL",
        "UV_PROJECT",
        "VIRTUAL_ENV",
    ):
        environment.pop(name, None)
    isolated = scratch / "tools" / root
    environment.update(
        {
            "UV_CACHE_DIR": str(scratch / "uv-cache"),
            "UV_PYTHON_INSTALL_DIR": str(scratch / "uv-python"),
            "UV_TOOL_BIN_DIR": str(isolated / "bin"),
            "UV_TOOL_DIR": str(isolated / "environments"),
            "RIVERHOG_ALLOW_INSECURE_HTTP": "true",
            "RIVERHOG_HTTP2": "false",
            "STOVE0_ALLOW_INSECURE_HTTP": "true",
            "STOVE0_HTTP2": "false",
        }
    )
    return environment


def _executable(bin_dir: Path, name: str) -> Path:
    suffix = ".exe" if os.name == "nt" else ""
    path = bin_dir / f"{name}{suffix}"
    if not path.is_file():
        raise QualificationError(f"installed entry point is absent: {path}")
    return path


def _tool_python(tool_dir: Path, root: str) -> Path:
    relative = Path("Scripts/python.exe") if os.name == "nt" else Path("bin/python")
    path = tool_dir / root / relative
    if not path.is_file():
        raise QualificationError(f"installed tool interpreter is absent: {path}")
    return path


def _installed_inventory(python: Path, cwd: Path, environment: dict[str, str]) -> dict[str, str]:
    program = (
        "import importlib.metadata as m,json,re;"
        "n=lambda v:re.sub(r'[-_.]+','-',v).lower();"
        "print(json.dumps({n(d.metadata['Name']):d.version "
        "for d in m.distributions()},sort_keys=True))"
    )
    completed = _run(
        [str(python), "-I", "-c", program],
        cwd=cwd,
        env=environment,
        capture=True,
    )
    value = json.loads(completed.stdout)
    if not isinstance(value, dict):
        raise QualificationError("installed distribution inventory is not an object")
    return {str(name): str(version) for name, version in value.items()}


def _python_version(python: Path, cwd: Path, environment: dict[str, str]) -> str:
    return _run(
        [str(python), "-I", "-c", "import platform;print(platform.python_version())"],
        cwd=cwd,
        env=environment,
        capture=True,
    ).stdout.strip()


def _run_client_operation(
    root: str,
    executable: Path,
    *,
    base_url: str,
    cwd: Path,
    environment: dict[str, str],
) -> str:
    if root == "a-riverhog-cli":
        environment["RIVERHOG_BASE_URL"] = base_url
        command = [str(executable), "event", "list", "--json"]
    elif root == "a-stove0-cli":
        environment["STOVE0_BASE_URL"] = base_url
        environment["STOVE0_TOKEN"] = "qualification-token"
        command = [str(executable), "--json", "event", "list"]
    else:
        raise QualificationError(f"no disposable client operation for {root}")
    completed = _run(command, cwd=cwd, env=environment, capture=True)
    payload = json.loads(completed.stdout)
    expected = STOVE0_EVENT_PAGE if root == "a-stove0-cli" else RIVERHOG_EVENT_PAGE
    if payload != expected:
        raise QualificationError(f"{root} changed the disposable event response")
    return "lifecycle-event-list"


def _run_gogurt(
    executable: Path,
    *,
    source_root: Path,
    scratch: Path,
    environment: dict[str, str],
    listener_lifecycle: bool,
    listener_lifecycle_repetitions: int,
    gogurt_evidence_dir: Path | None,
    reference: dict[str, str],
    listener_mount_fixture: ListenerMountFixture | None = None,
) -> str:
    environment = {
        **environment,
        "GOGURT_MOUNTED_VOLUME_PROVIDER": reference["mounted_volume_provider"],
        "GOGURT_LISTENER_HOST_PROVIDER": reference["listener_host_provider"],
    }
    for kind, name in (
        ("mounted-volume", reference["mounted_volume_provider"]),
        ("listener-host", reference["listener_host_provider"]),
    ):
        shown = json.loads(
            _run(
                [str(executable), "provider", kind, "show", name, "--json"],
                cwd=scratch,
                env=environment,
                capture=True,
            ).stdout
        )
        if shown.get("name") != name:
            raise QualificationError(f"installed Gogurt {kind} provider identity differs")
    fixtures = scratch / "gogurt-fixtures"
    shutil.copytree(source_root / "qualification/fixtures/gogurt", fixtures)
    mount = scratch / "gogurt-mount"
    mount.mkdir()
    publication = json.loads(
        _run(
            [
                str(executable),
                "write",
                "example-camera-card",
                str(mount),
                "--config",
                str(fixtures / "gogurt-routes.yaml"),
                "--json",
            ],
            cwd=scratch,
            env=environment,
            capture=True,
        ).stdout
    )
    if publication.get("marker") != {
        "format": "gogurt-route-marker/v1",
        "route": "example-camera-card",
    }:
        raise QualificationError("installed Gogurt did not publish its logical route marker")
    if (mount / ".gogurt").read_bytes() != b"example-camera-card\n":
        raise QualificationError("reference Gogurt provider representation differs")
    completed = _run(
        [
            str(executable),
            "run",
            str(mount),
            "--config",
            str(fixtures / "gogurt-routes.yaml"),
            "--dry-run",
        ],
        cwd=scratch,
        env=environment,
        capture=True,
    )
    if "gogurt would run" not in completed.stderr:
        raise QualificationError("installed Gogurt did not execute its portable foreground plan")
    if not listener_lifecycle:
        return "portable-foreground-dry-run"
    for repetition in range(1, listener_lifecycle_repetitions + 1):
        lifecycle_scratch = scratch / f"gogurt-listener-{repetition}"
        lifecycle_scratch.mkdir()
        evidence_dir = (
            gogurt_evidence_dir / f"repetition-{repetition}"
            if gogurt_evidence_dir is not None
            else None
        )
        _run_gogurt_listener_lifecycle(
            executable,
            scratch=lifecycle_scratch,
            environment=environment,
            evidence_dir=evidence_dir,
            exercise_extended_lifecycle=repetition == 1,
            listener_mount_fixture=listener_mount_fixture,
        )
    return "native-listener-lifecycle"


@contextmanager
def _operator_qualification_mount(scratch: Path, fixture: ListenerMountFixture) -> Iterator[Path]:
    import fcntl

    backing = fixture.mount_point
    descriptor = (
        os.open(backing, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        if fixture.lock_file is None
        else os.open(fixture.lock_file, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    )
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise QualificationError("operator mount fixture is already in use") from exc
        if backing.is_mount() or any(backing.iterdir()):
            raise QualificationError("operator mount fixture must start unmounted and empty")

        def teardown() -> None:
            _qualification_mount_operation(
                "operator qualification teardown", list(fixture.teardown), cwd=scratch
            )
            if backing.is_mount() or any(backing.iterdir()):
                raise QualificationError(
                    "operator teardown must leave the fixture unmounted and empty"
                )

        with _settled_qualification_mount(backing, teardown):
            _qualification_mount_operation(
                "operator qualification setup", list(fixture.setup), cwd=scratch
            )
            if not backing.is_mount() or any(backing.iterdir()):
                raise QualificationError("operator setup must provide a fresh mounted fixture")
            with tempfile.TemporaryFile(dir=backing) as probe:
                probe.write(b"qualification-write-probe")
                probe.flush()
            yield backing
    finally:
        os.close(descriptor)


@contextmanager
def _qualification_mount(
    scratch: Path, *, fixture: ListenerMountFixture | None = None
) -> Iterator[Path]:
    if fixture is not None:
        if not sys.platform.startswith("linux"):
            raise QualificationError("an operator mount fixture requires Linux")
        with _operator_qualification_mount(scratch, fixture) as mounted:
            yield mounted
        return
    if sys.platform.startswith("linux"):
        backing = scratch / "gogurt-listener-volume"
        backing.mkdir()
        _qualification_mount_operation(
            "Linux tmpfs qualification mount",
            [
                "sudo",
                "-n",
                "mount",
                "-t",
                "tmpfs",
                "-o",
                "size=16m",
                "gogurt-qualification",
                str(backing),
            ],
            cwd=scratch,
        )
        with _settled_qualification_mount(
            backing,
            lambda: _qualification_mount_operation(
                "Linux tmpfs qualification unmount",
                ["sudo", "-n", "umount", str(backing)],
                cwd=scratch,
            ),
        ) as mounted:
            yield mounted
        return
    if sys.platform == "darwin":
        backing = Path("/Volumes") / f"GogurtQualification-{os.getpid()}"
        image = scratch / "gogurt-listener.dmg"
        _create_macos_qualification_image(image, cwd=scratch)
        _qualification_mount_operation(
            "macOS qualification disk-image attachment",
            [
                "hdiutil",
                "attach",
                "-nobrowse",
                "-mountpoint",
                str(backing),
                str(image),
            ],
            cwd=scratch,
        )
        with _settled_qualification_mount(
            backing,
            # The listener and its action custody are settled before mount
            # release. Disk Arbitration can retain an incidental handle briefly
            # after that proof, so retry only its classified resource-busy exit.
            lambda: _detach_macos_qualification_image(backing, cwd=scratch),
        ) as mounted:
            yield mounted
        return
    if sys.platform == "win32":
        backing = scratch / "gogurt-listener-volume"
        backing.mkdir()
        drive = next(
            (
                f"{letter}:"
                for letter in reversed(string.ascii_uppercase[3:])
                if not Path(f"{letter}:\\").exists()
            ),
            None,
        )
        if drive is None:
            raise QualificationError("no disposable Windows drive letter is available")
        _qualification_mount_operation(
            "Windows qualification drive attachment",
            ["subst.exe", drive, str(backing)],
            cwd=scratch,
        )
        with _settled_qualification_mount(
            Path(drive + "\\"),
            lambda: _qualification_mount_operation(
                "Windows qualification drive detachment",
                ["subst.exe", drive, "/D"],
                cwd=scratch,
            ),
        ) as mounted:
            yield mounted
        return
    raise QualificationError(f"no disposable mount strategy for {sys.platform}")


def _listener_status(
    executable: Path,
    *,
    scratch: Path,
    environment: dict[str, str],
) -> dict[str, Any]:
    completed = _run(
        [str(executable), "listener", "status", "--json"],
        cwd=scratch,
        env=environment,
        capture=True,
    )
    value = json.loads(completed.stdout)
    if not isinstance(value, dict):
        raise QualificationError("installed Gogurt listener status is not an object")
    return value


def _process_is_running(pid: int) -> bool:
    if sys.platform == "win32":
        import ctypes

        kernel32 = cast(Any, ctypes).windll.kernel32
        handle = kernel32.OpenProcess(0x00100000, False, pid)
        if not handle:
            return kernel32.GetLastError() == 5
        try:
            return kernel32.WaitForSingleObject(handle, 0) == 0x00000102
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except (OverflowError, ProcessLookupError, ValueError):
        return False
    except PermissionError:
        return True
    return True


def _windows_current_user_sid(
    *,
    scratch: Path,
    environment: dict[str, str],
) -> str:
    completed = _run(
        [
            "powershell.exe",
            "-NoProfile",
            "-NonInteractive",
            "-Command",
            "[Console]::Out.Write([Security.Principal.WindowsIdentity]::GetCurrent().User.Value)",
        ],
        cwd=scratch,
        env=environment,
        capture=True,
    )
    sid = completed.stdout.strip()
    if re.fullmatch(r"S-[0-9]+(?:-[0-9]+)+", sid) is None:
        raise QualificationError("Windows returned an invalid current-user SID")
    return sid


def _windows_task_name(
    *,
    scratch: Path,
    environment: dict[str, str],
) -> str:
    sid = _windows_current_user_sid(scratch=scratch, environment=environment)
    return "Riverhog.Gogurt." + hashlib.sha256(sid.encode("ascii")).hexdigest()


def _windows_current_user_aliases(
    sid: str,
    *,
    scratch: Path,
    environment: dict[str, str],
) -> frozenset[str]:
    aliases = {sid.casefold()}
    username = environment.get("USERNAME", "").strip()
    domain = environment.get("USERDOMAIN", "").strip()
    if username:
        aliases.add(username.casefold())
        if domain:
            aliases.add(f"{domain}\\{username}".casefold())
    completed = _run(
        ["whoami.exe"],
        cwd=scratch,
        env=environment,
        capture=True,
    )
    account = completed.stdout.strip()
    if account:
        aliases.add(account.casefold())
        _, separator, unqualified = account.rpartition("\\")
        if separator and unqualified:
            aliases.add(unqualified.casefold())
    return frozenset(aliases)


def _verify_windows_task_definition(
    executable: Path,
    *,
    scratch: Path,
    environment: dict[str, str],
) -> None:
    sid = _windows_current_user_sid(scratch=scratch, environment=environment)
    task_name = "Riverhog.Gogurt." + hashlib.sha256(sid.encode("ascii")).hexdigest()
    completed = _run(
        ["schtasks.exe", "/Query", "/TN", task_name, "/XML"],
        cwd=scratch,
        env=environment,
        capture=True,
    )
    task = ET.fromstring(completed.stdout)
    ns = {"task": WINDOWS_TASK_XML_NAMESPACE}
    identity_paths = (
        "task:Triggers/task:LogonTrigger/task:UserId",
        "task:Principals/task:Principal/task:UserId",
    )
    expected = {
        "task:Principals/task:Principal/task:LogonType": "InteractiveToken",
        "task:Settings/task:MultipleInstancesPolicy": "IgnoreNew",
        "task:Settings/task:ExecutionTimeLimit": "PT0S",
        "task:Settings/task:RestartOnFailure/task:Interval": "PT1M",
        "task:Settings/task:RestartOnFailure/task:Count": "3",
    }
    expected_booleans = {
        "task:Settings/task:DisallowStartIfOnBatteries": False,
        "task:Settings/task:StopIfGoingOnBatteries": False,
        "task:Settings/task:AllowHardTerminate": False,
        "task:Settings/task:StartWhenAvailable": True,
    }
    mismatches = [
        path for path, value in expected.items() if task.findtext(path, namespaces=ns) != value
    ]
    aliases = _windows_current_user_aliases(
        sid,
        scratch=scratch,
        environment=environment,
    )
    mismatches.extend(
        path
        for path in identity_paths
        if (task.findtext(path, namespaces=ns) or "").casefold() not in aliases
    )
    boolean_values = {True: frozenset({"true", "1"}), False: frozenset({"false", "0"})}
    mismatches.extend(
        path
        for path, value in expected_booleans.items()
        if (task.findtext(path, namespaces=ns) or "").casefold() not in boolean_values[value]
    )
    run_level_path = "task:Principals/task:Principal/task:RunLevel"
    run_level = task.findtext(run_level_path, namespaces=ns)
    if run_level not in {None, "LeastPrivilege"}:
        mismatches.append(run_level_path)
    allow_start_path = "task:Settings/task:AllowStartOnDemand"
    allow_start = task.findtext(allow_start_path, namespaces=ns)
    if allow_start is not None and allow_start.casefold() not in boolean_values[True]:
        mismatches.append(allow_start_path)
    command_path = "task:Actions/task:Exec/task:Command"
    observed_command = task.findtext(command_path, namespaces=ns)
    try:
        same_executable = observed_command is not None and os.path.samefile(
            observed_command,
            executable,
        )
    except OSError:
        same_executable = False
    if not same_executable:
        mismatches.append(command_path)
    if mismatches:
        fields = ", ".join(sorted(set(mismatches)))
        raise QualificationError(
            f"installed Windows listener task settings differ from v1: {fields}"
        )


def _native_registration_file(environment: dict[str, str]) -> Path | None:
    home = Path(environment.get("HOME", str(Path.home())))
    if sys.platform.startswith("linux"):
        config_root = Path(environment.get("XDG_CONFIG_HOME", str(home / ".config")))
        return config_root / "systemd" / "user" / "gogurt-listener.service"
    if sys.platform == "darwin":
        return home / "Library" / "LaunchAgents" / "io.github.nashspence.gogurt.plist"
    return None


def _wait_for_listener(
    executable: Path,
    predicate: Callable[[dict[str, Any]], bool],
    *,
    scratch: Path,
    environment: dict[str, str],
    timeout_seconds: float = 30,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    status = _listener_status(
        executable,
        scratch=scratch,
        environment=environment,
    )
    while not predicate(status) and time.monotonic() < deadline:
        time.sleep(0.2)
        status = _listener_status(
            executable,
            scratch=scratch,
            environment=environment,
        )
    if not predicate(status):
        raise QualificationError(
            f"installed Gogurt listener did not reach expected state: {status}"
        )
    return status


def _native_listener_snapshot(
    *,
    scratch: Path,
    environment: dict[str, str],
) -> dict[str, object] | None:
    getuid = getattr(os, "getuid", None)
    if sys.platform == "darwin" and getuid is not None:
        native = subprocess.run(
            [
                "launchctl",
                "print",
                f"gui/{getuid()}/io.github.nashspence.gogurt",
            ],
            cwd=scratch,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        fields: dict[str, str | int | list[int]] = {}
        for key, value in re.findall(
            r"(?m)^\s*(state|pid|runs|last exit code|last terminating signal)\s*=\s*([^\n]+)$",
            native.stdout,
        ):
            normalized = value.strip()
            if key == "state":
                if re.fullmatch(r"[A-Za-z0-9_-]{1,64}", normalized):
                    fields[key] = normalized
            elif re.fullmatch(r"-?[0-9]{1,20}", normalized):
                fields[key] = int(normalized)
        return {"returncode": native.returncode, "fields": fields}
    if sys.platform.startswith("linux"):
        native = subprocess.run(
            [
                "systemctl",
                "--user",
                "show",
                "gogurt-listener.service",
                "--property="
                "ActiveState,SubState,Result,ExecMainCode,ExecMainStatus,NRestarts,MainPID",
            ],
            cwd=scratch,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        linux_fields: dict[str, str | int | list[int]] = {}
        for line in native.stdout.splitlines():
            key, separator, value = line.partition("=")
            if separator != "=" or key not in {
                "ActiveState",
                "SubState",
                "Result",
                "ExecMainCode",
                "ExecMainStatus",
                "NRestarts",
                "MainPID",
            }:
                continue
            normalized = value.strip()
            if re.fullmatch(r"-?[0-9]{1,20}", normalized):
                linux_fields[key] = int(normalized)
            elif re.fullmatch(r"[A-Za-z0-9_-]{1,64}", normalized):
                linux_fields[key] = normalized
        main_code = linux_fields.get("ExecMainCode")
        main_status = linux_fields.get("ExecMainStatus")
        if main_code in {2, 3} and isinstance(main_status, int):
            linux_fields["TerminationKind"] = "core-dump" if main_code == 3 else "signal"
            try:
                linux_fields["TerminationSignal"] = signal.Signals(main_status).name
            except ValueError:
                pass
        return {"returncode": native.returncode, "fields": linux_fields}
    if sys.platform == "win32":
        task_name = _windows_task_name(scratch=scratch, environment=environment)
        script = (
            "$ErrorActionPreference='Stop';"
            "try {"
            "$service=New-Object -ComObject 'Schedule.Service';"
            "$service.Connect();"
            f"$task=$service.GetFolder('\\').GetTask('{task_name}');"
            "$pids=@($task.GetInstances(0)|ForEach-Object {[int]$_.EnginePID}|Sort-Object -Unique);"
            "$fields=[ordered]@{state=[int]$task.State;"
            "last_result=[int]$task.LastTaskResult;instance_pids=$pids};"
            "$result=[ordered]@{returncode=0;fields=$fields};"
            "[Console]::Out.Write(($result|ConvertTo-Json -Compress));exit 0"
            "} catch { exit 3 }"
        )
        native = subprocess.run(
            [
                "powershell.exe",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                script,
            ],
            cwd=scratch,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if native.returncode == 0:
            return _parse_windows_native_snapshot(json.loads(native.stdout))
        return {"returncode": native.returncode, "fields": {}}
    return None


def _parse_windows_native_snapshot(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError("Task Scheduler state is not an object")
    returncode = value.get("returncode")
    if not isinstance(returncode, int) or isinstance(returncode, bool):
        raise ValueError("Task Scheduler state lacks a numeric return code")
    fields_value = value.get("fields")
    if not isinstance(fields_value, dict):
        raise ValueError("Task Scheduler fields are not an object")
    fields: dict[str, int | list[int]] = {}
    for key in ("state", "last_result"):
        item = fields_value.get(key)
        if isinstance(item, int) and not isinstance(item, bool):
            fields[key] = item
    pids = fields_value.get("instance_pids")
    if (
        isinstance(pids, list)
        and len(pids) <= 32
        and all(isinstance(item, int) and not isinstance(item, bool) and item > 0 for item in pids)
    ):
        fields["instance_pids"] = sorted(set(pids))
    return {"returncode": returncode, "fields": fields}


class _NativeLifecycleTrace:
    """Retain bounded native state transitions before a supervisor overwrites them."""

    def __init__(
        self,
        *,
        scratch: Path,
        environment: dict[str, str],
        probe: Callable[[], dict[str, object] | None] | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.scratch = scratch
        self.environment = environment
        self.probe = probe or (
            lambda: _native_listener_snapshot(scratch=scratch, environment=environment)
        )
        self.clock = clock
        self.started_at = clock()
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.events: list[dict[str, object]] = []
        self.previous: dict[str, object] | None = None
        self.thread = threading.Thread(
            target=self._run,
            name="gogurt-native-lifecycle-trace",
            daemon=True,
        )

    def _capture(self) -> None:
        try:
            snapshot = self.probe()
        except (OSError, subprocess.SubprocessError, ValueError) as exc:
            snapshot = {"probe_error": type(exc).__name__}
        self._record(snapshot)

    def _record(self, snapshot: dict[str, object] | None) -> None:
        if snapshot is None or snapshot == self.previous:
            return
        self.previous = snapshot
        event = {
            "elapsed_milliseconds": max(0, int((self.clock() - self.started_at) * 1000)),
            **snapshot,
        }
        with self.lock:
            if len(self.events) < NATIVE_TRACE_EVENT_LIMIT:
                self.events.append(event)

    def _run(self) -> None:
        self._capture()
        while not self.stop_event.wait(NATIVE_TRACE_INTERVAL_SECONDS):
            self._capture()

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> tuple[dict[str, object], ...]:
        self.stop_event.set()
        self.thread.join(timeout=6)
        if not self.thread.is_alive():
            self._capture()
        with self.lock:
            return tuple(dict(item) for item in self.events)


class _FailureSnapshotOnlyNativeLifecycleTrace:
    """Avoid perturbing Task Scheduler; its terminal result remains queryable on failure."""

    def start(self) -> None:
        return

    def stop(self) -> tuple[dict[str, object], ...]:
        return ()


def _native_lifecycle_trace(
    *,
    scratch: Path,
    environment: dict[str, str],
) -> _NativeLifecycleTrace | _FailureSnapshotOnlyNativeLifecycleTrace:
    if sys.platform == "win32":
        return _FailureSnapshotOnlyNativeLifecycleTrace()
    return _NativeLifecycleTrace(scratch=scratch, environment=environment)


def _retain_gogurt_failure_evidence(
    executable: Path,
    *,
    state_dir: Path,
    scratch: Path,
    environment: dict[str, str],
    evidence_dir: Path | None,
    failure: BaseException,
    phase: str,
    native_transitions: tuple[dict[str, object], ...] = (),
) -> None:
    if evidence_dir is None:
        return
    try:
        evidence_dir.mkdir(parents=True, exist_ok=True)
        diagnostic = " ".join(f"{type(failure).__name__}: {failure}".splitlines())[:4096]
        (evidence_dir / f"{phase}-failure.txt").write_text(
            diagnostic + "\n",
            encoding="utf-8",
        )
        status = subprocess.run(
            [str(executable), "listener", "status", "--json"],
            cwd=scratch,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        status_payload = {
            "returncode": status.returncode,
            "stdout": status.stdout[-65536:],
            "stderr": status.stderr[-65536:],
        }
        (evidence_dir / f"{phase}-status.json").write_text(
            json.dumps(status_payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        native = _native_listener_snapshot(scratch=scratch, environment=environment)
        if native is not None:
            (evidence_dir / f"{phase}-native.json").write_text(
                json.dumps(native, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        if native_transitions:
            (evidence_dir / f"{phase}-native-transitions.json").write_text(
                json.dumps(
                    {
                        "format": "gogurt-native-lifecycle-trace/v1",
                        "events": native_transitions,
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
        log_sources = {
            *state_dir.glob("listener.log*"),
            *state_dir.glob("listener.fatal.log*"),
        }
        for source in sorted(log_sources):
            if source.is_symlink():
                continue
            info = source.stat()
            if not stat.S_ISREG(info.st_mode) or info.st_size > 2 * 1024 * 1024:
                continue
            shutil.copy2(source, evidence_dir / source.name)
    except (OSError, subprocess.SubprocessError, ValueError) as evidence_error:
        try:
            evidence_dir.mkdir(parents=True, exist_ok=True)
            diagnostic = " ".join(
                f"{type(evidence_error).__name__}: {evidence_error}".splitlines()
            )[:4096]
            (evidence_dir / f"{phase}-evidence-error.txt").write_text(
                diagnostic + "\n",
                encoding="utf-8",
            )
        except OSError:
            return


def _run_gogurt_listener_lifecycle(
    executable: Path,
    *,
    scratch: Path,
    environment: dict[str, str],
    evidence_dir: Path | None,
    exercise_extended_lifecycle: bool,
    listener_mount_fixture: ListenerMountFixture | None = None,
) -> None:
    initial = _listener_status(
        executable,
        scratch=scratch,
        environment=environment,
    )
    if initial.get("installed") is not False:
        raise QualificationError("listener qualification would replace an existing installation")
    state_dir = Path(str(initial["state_dir"]))
    native_trace = _native_lifecycle_trace(scratch=scratch, environment=environment)
    native_trace.start()

    action = scratch / "gogurt-listener-action.py"
    action.write_text(
        "import sys,time\n"
        "from pathlib import Path\n"
        "sentinel=Path(sys.argv[2])\n"
        "with sentinel.open('a', encoding='utf-8') as stream:\n"
        "    stream.write('run\\n')\n"
        "time.sleep(0.75)\n",
        encoding="utf-8",
    )
    failure = scratch / "gogurt-listener-failure.py"
    failure.write_text(
        f"raise SystemExit({GOGURT_QUALIFICATION_ACTION_FAILURE_EXIT})\n",
        encoding="utf-8",
    )
    custody_pid = scratch / "gogurt-listener-custody.pid"
    custody = scratch / "gogurt-listener-custody.py"
    custody.write_text(
        "import os,signal,sys,time\n"
        "from pathlib import Path\n"
        "if hasattr(signal, 'SIGTERM'):\n"
        "    signal.signal(signal.SIGTERM, lambda *_args: None)\n"
        "path=Path(sys.argv[2])\n"
        "staged=path.with_name(path.name + '.tmp')\n"
        "staged.write_text(str(os.getpid()), encoding='utf-8')\n"
        "os.replace(staged, path)\n"
        "while True:\n"
        "    time.sleep(1)\n",
        encoding="utf-8",
    )
    sentinel = scratch / "gogurt-listener-sentinel.txt"
    routes = scratch / "gogurt-listener-routes.yaml"
    routes.write_text(
        "schema_version: 1\n"
        "kind: gogurt.routes\n"
        "routes:\n"
        "  qualification-success:\n"
        "    command:\n"
        '      - "{python}"\n'
        f"      - {json.dumps(str(action))}\n"
        '      - "{mount_point}"\n'
        f"      - {json.dumps(str(sentinel))}\n"
        "  qualification-failure:\n"
        "    command:\n"
        '      - "{python}"\n'
        f"      - {json.dumps(str(failure))}\n"
        '      - "{mount_point}"\n'
        "  qualification-custody:\n"
        "    command:\n"
        '      - "{python}"\n'
        f"      - {json.dumps(str(custody))}\n"
        '      - "{mount_point}"\n'
        f"      - {json.dumps(str(custody_pid))}\n",
        encoding="utf-8",
    )

    installed = False
    active_uninstall_pid: int | None = None
    lifecycle_failure_retained = False

    def retain_lifecycle_failure(exc: BaseException) -> None:
        nonlocal lifecycle_failure_retained
        _retain_gogurt_failure_evidence(
            executable,
            state_dir=state_dir,
            scratch=scratch,
            environment=environment,
            evidence_dir=evidence_dir,
            failure=exc,
            phase="lifecycle",
            native_transitions=native_trace.stop(),
        )
        lifecycle_failure_retained = True

    def settle_listener() -> None:
        nonlocal installed
        if not installed:
            native_trace.stop()
            return
        try:
            removed = json.loads(
                _run(
                    [str(executable), "listener", "uninstall", "--json"],
                    cwd=scratch,
                    env=environment,
                    capture=True,
                ).stdout
            )
        except BaseException as exc:
            _retain_gogurt_failure_evidence(
                executable,
                state_dir=state_dir,
                scratch=scratch,
                environment=environment,
                evidence_dir=evidence_dir,
                failure=exc,
                phase="uninstall",
                native_transitions=native_trace.stop(),
            )
            raise
        installed = False
        native_trace.stop()
        if removed.get("installed") is not False or removed.get("health") != "absent":
            raise QualificationError("Gogurt listener uninstall left native registration")
        if Path(str(removed["state_dir"])).exists():
            raise QualificationError("Gogurt listener uninstall left durable state")
        if active_uninstall_pid is not None and _process_is_running(active_uninstall_pid):
            raise QualificationError("Gogurt listener uninstall left its action process alive")

    try:
        with _qualification_listener_mount(
            scratch,
            observe_failure=retain_lifecycle_failure,
            settle_listener=settle_listener,
            fixture=listener_mount_fixture,
        ) as mount:

            def write_marker(route: str, *, force: bool = False) -> None:
                command = [
                    str(executable),
                    "write",
                    route,
                    str(mount),
                    "--config",
                    str(routes),
                    "--json",
                ]
                if force:
                    command.append("--force")
                _run(
                    command,
                    cwd=scratch,
                    env=environment,
                    capture=True,
                )

            installed_payload = json.loads(
                _run(
                    [
                        str(executable),
                        "listener",
                        "install",
                        "--config",
                        str(routes),
                        "--interval",
                        "0.2",
                        "--autorun",
                        "--json",
                    ],
                    cwd=scratch,
                    env=environment,
                    capture=True,
                ).stdout
            )
            installed = True
            if installed_payload.get("health") != "healthy":
                raise QualificationError("installed Gogurt listener did not report healthy")
            mounted_volume_provider = installed_payload.get("mounted_volume_provider")
            listener_host_provider = installed_payload.get("listener_host_provider")
            if (
                not isinstance(mounted_volume_provider, dict)
                or mounted_volume_provider.get("name")
                != environment["GOGURT_MOUNTED_VOLUME_PROVIDER"]
                or not isinstance(listener_host_provider, dict)
                or listener_host_provider.get("name")
                != environment["GOGURT_LISTENER_HOST_PROVIDER"]
            ):
                raise QualificationError("installed Gogurt listener lost exact provider identity")
            if exercise_extended_lifecycle and sys.platform == "win32":
                _verify_windows_task_definition(
                    executable,
                    scratch=scratch,
                    environment=environment,
                )
            human = _run(
                [str(executable), "listener", "status"],
                cwd=scratch,
                env=environment,
                capture=True,
            )
            if "health: healthy" not in human.stdout:
                raise QualificationError("Gogurt listener human status differs from JSON status")

            state_dir = Path(str(installed_payload["state_dir"]))
            if os.name != "nt":
                if stat.S_IMODE(state_dir.stat().st_mode) != 0o700:
                    raise QualificationError("Gogurt listener state directory is not private")
                private_names = (
                    "listener.json",
                    "listener.sqlite3",
                    "heartbeat.json",
                    "listener.lock",
                    "listener.log",
                    "listener.fatal.log",
                )
                if any(
                    stat.S_IMODE((state_dir / name).stat().st_mode) != 0o600
                    for name in private_names
                ):
                    raise QualificationError("Gogurt listener state files are not private")

            routes_content = routes.read_text(encoding="utf-8")
            routes.write_text("not: [valid", encoding="utf-8")
            failed = _wait_for_listener(
                executable,
                lambda status: status.get("health") == "failed",
                scratch=scratch,
                environment=environment,
            )
            if "global configuration" not in str(failed.get("diagnostic")):
                raise QualificationError("Gogurt global config failure lacks a diagnostic")
            human_failed = _run(
                [str(executable), "listener", "status"],
                cwd=scratch,
                env=environment,
                capture=True,
            )
            if (
                "health: failed" not in human_failed.stdout
                or "diagnostic:" not in human_failed.stdout
            ):
                raise QualificationError("Gogurt human status hid global config failure")
            if sentinel.exists():
                raise QualificationError("Gogurt dispatched while global config was invalid")
            routes.write_text(routes_content, encoding="utf-8")
            _wait_for_listener(
                executable,
                lambda status: status.get("health") == "healthy",
                scratch=scratch,
                environment=environment,
            )
            write_marker("qualification-success")

            def one_completed(status: dict[str, Any]) -> bool:
                dispatches = status.get("dispatches")
                counts = dispatches.get("counts") if isinstance(dispatches, dict) else None
                return isinstance(counts, dict) and counts.get("completed") == 1

            _wait_for_listener(
                executable,
                one_completed,
                scratch=scratch,
                environment=environment,
            )
            time.sleep(1)
            if sentinel.read_text(encoding="utf-8").splitlines() != ["run"]:
                raise QualificationError(
                    "Gogurt listener dispatched one observation more than once"
                )

            stopped = json.loads(
                _run(
                    [str(executable), "listener", "stop", "--json"],
                    cwd=scratch,
                    env=environment,
                    capture=True,
                ).stdout
            )
            if stopped.get("health") != "stopped":
                raise QualificationError("Gogurt listener stop did not preserve stopped state")
            for operation in ("start", "restart"):
                payload = json.loads(
                    _run(
                        [str(executable), "listener", operation, "--json"],
                        cwd=scratch,
                        env=environment,
                        capture=True,
                    ).stdout
                )
                if payload.get("health") != "healthy":
                    raise QualificationError(f"Gogurt listener {operation} did not become healthy")
                time.sleep(0.5)
                if sentinel.read_text(encoding="utf-8").splitlines() != ["run"]:
                    raise QualificationError(
                        f"Gogurt listener replayed a completed observation after {operation}"
                    )

            before_reinstall = _wait_for_listener(
                executable,
                lambda status: status.get("health") == "healthy",
                scratch=scratch,
                environment=environment,
            )
            reinstalled = json.loads(
                _run(
                    [
                        str(executable),
                        "listener",
                        "install",
                        "--config",
                        str(routes),
                        "--interval",
                        "0.2",
                        "--autorun",
                        "--json",
                    ],
                    cwd=scratch,
                    env=environment,
                    capture=True,
                ).stdout
            )
            if reinstalled.get("health") != "healthy":
                raise QualificationError("same-version listener reinstall did not become healthy")
            before_heartbeat = before_reinstall.get("heartbeat")
            after_heartbeat = reinstalled.get("heartbeat")
            before_pid = before_heartbeat.get("pid") if isinstance(before_heartbeat, dict) else None
            after_pid = after_heartbeat.get("pid") if isinstance(after_heartbeat, dict) else None
            if (
                not isinstance(before_heartbeat, dict)
                or not isinstance(after_heartbeat, dict)
                or before_pid != after_pid
            ):
                raise QualificationError(
                    "exact healthy listener reinstall churned its process "
                    f"(before_health={before_reinstall.get('health')}, "
                    f"before_pid={before_pid}, after_pid={after_pid})"
                )
            time.sleep(0.5)
            if sentinel.read_text(encoding="utf-8").splitlines() != ["run"]:
                raise QualificationError("same-version listener reinstall replayed completed work")

            if exercise_extended_lifecycle:
                custody_pid.unlink(missing_ok=True)
                write_marker("qualification-custody", force=True)

                def custody_active(status: dict[str, Any]) -> bool:
                    heartbeat = status.get("heartbeat")
                    return (
                        custody_pid.is_file()
                        and isinstance(heartbeat, dict)
                        and isinstance(heartbeat.get("active_dispatch"), str)
                    )

                _wait_for_listener(
                    executable,
                    custody_active,
                    scratch=scratch,
                    environment=environment,
                )
                stopped_action_pid = int(custody_pid.read_text(encoding="utf-8"))
                stopped = json.loads(
                    _run(
                        [str(executable), "listener", "stop", "--json"],
                        cwd=scratch,
                        env=environment,
                        capture=True,
                    ).stdout
                )
                counts = stopped.get("dispatches", {}).get("counts", {})
                if (
                    stopped.get("health") != "stopped"
                    or not isinstance(counts, dict)
                    or counts.get("running", 0) != 0
                    or counts.get("uncertain", 0) < 1
                    or _process_is_running(stopped_action_pid)
                ):
                    raise QualificationError(
                        "Gogurt listener stop did not settle active action custody"
                    )
                restarted = json.loads(
                    _run(
                        [str(executable), "listener", "start", "--json"],
                        cwd=scratch,
                        env=environment,
                        capture=True,
                    ).stdout
                )
                if restarted.get("health") != "healthy":
                    raise QualificationError(
                        "Gogurt listener did not restart after cooperative custody settlement"
                    )

            write_marker("qualification-failure", force=True)

            def failure_visible(status: dict[str, Any]) -> bool:
                dispatches = status.get("dispatches")
                attention = dispatches.get("attention") if isinstance(dispatches, dict) else None
                return isinstance(attention, list) and any(
                    isinstance(item, dict)
                    and item.get("state") == "retry"
                    and item.get("exit_code") == GOGURT_QUALIFICATION_ACTION_FAILURE_EXIT
                    for item in attention
                )

            _wait_for_listener(
                executable,
                failure_visible,
                scratch=scratch,
                environment=environment,
            )
            if exercise_extended_lifecycle:
                custody_pid.unlink(missing_ok=True)
                write_marker("qualification-custody", force=True)
                _wait_for_listener(
                    executable,
                    custody_active,
                    scratch=scratch,
                    environment=environment,
                )
                active_uninstall_pid = int(custody_pid.read_text(encoding="utf-8"))
                registration = _native_registration_file(environment)
                if registration is not None:
                    registration.unlink()
                    manager_owned = _listener_status(
                        executable,
                        scratch=scratch,
                        environment=environment,
                    )
                    if manager_owned.get("installed") is not True:
                        raise QualificationError(
                            "native manager registration disappeared with its definition file"
                        )
    except BaseException as exc:
        # Mount construction/release errors occur outside the listener-body
        # observer. Preserve them too without replacing a pre-settlement state
        # snapshot when only the combined terminal diagnostic changed.
        if not lifecycle_failure_retained:
            retain_lifecycle_failure(exc)
        elif evidence_dir is not None:
            try:
                diagnostic = " ".join(f"{type(exc).__name__}: {exc}".splitlines())[:4096]
                (evidence_dir / "lifecycle-failure.txt").write_text(
                    diagnostic + "\n",
                    encoding="utf-8",
                )
            except OSError:
                pass
        raise


def _run_recovery(
    executable: Path,
    *,
    scratch: Path,
    environment: dict[str, str],
) -> str:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))
    from riverhog_archive_contracts import MemberHistoryBinding

    from tests.support.qualification.recovery_archive import (
        PASSPHRASE,
        PASSPHRASE_ID,
        write_archive,
    )

    archive = scratch / "recovery-archive"
    archive.mkdir()
    fixture = write_archive(archive)
    passphrases = scratch / "passphrases.json"
    passphrases.write_text(
        # codeql[py/clear-text-storage-sensitive-data]
        json.dumps({PASSPHRASE_ID: PASSPHRASE}, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    passphrases.chmod(0o600)
    output = scratch / "recovered"
    try:
        _run(
            [
                str(executable),
                str(archive),
                str(output),
                "--passphrases-file",
                str(passphrases),
            ],
            cwd=scratch,
            env=environment,
        )
    finally:
        passphrases.unlink(missing_ok=True)
    receipt = json.loads((output / "recovery.json").read_bytes())
    if (
        receipt.get("complete") is not True
        or receipt.get("archive_root_sha256") != fixture.archive_root_sha256
        or receipt.get("artifacts") != len(fixture.members)
        or receipt.get("metadata_pins", {}).get("description_sha256") != fixture.description_sha256
        or receipt.get("metadata_pins", {}).get("tag_head_sha256") != fixture.tag_head_sha256
    ):
        raise QualificationError("installed recovery did not complete the exact four contents")
    rows = [
        json.loads(line) for line in (output / "recovery-members.jsonseq").read_bytes().splitlines()
    ]
    if {row["artifact_id"] for row in rows} != set(fixture.members):
        raise QualificationError("installed recovery mapping omits an artifact")
    expected_bindings = {binding.artifact_id: binding for binding in fixture.history_bindings}
    for row in rows:
        artifact_id = row["artifact_id"]
        payload = (output.joinpath(*row["components"])).read_bytes()
        if payload != fixture.members[artifact_id]:
            raise QualificationError("installed recovery payload differs from the fixture")
        binding = MemberHistoryBinding.from_mapping(row["binding"])
        if binding != expected_bindings[artifact_id]:
            raise QualificationError("installed recovery member history binding differs")
        history = binding.verify_descriptor(output.joinpath(*row["history"]).read_bytes())
        sidecar = output.joinpath(*row["sidecar"])
        primary = sidecar.read_bytes()
        if len(primary) != history.primary.journal.prefix_bytes or (
            hashlib.sha256(primary).hexdigest() != history.primary.journal.prefix_sha256
        ):
            raise QualificationError("installed recovery primary history differs")
    if (
        not (output / "metadata/description.json").is_file()
        or not (output / "metadata/tags/head.json").is_file()
    ):
        raise QualificationError("installed recovery omitted metadata authority")
    for journal in fixture.journals.values():
        digest = hashlib.sha256(journal).hexdigest()
        if (output / "provenance/journals" / f"{digest}.jsonseq").read_bytes() != journal:
            raise QualificationError("installed recovery shared history differs")
    for relative_path, raw in fixture.history_objects.items():
        recovered = output.joinpath("structure", *relative_path.removesuffix(".age").split("/"))
        if recovered.read_bytes() != raw:
            raise QualificationError("installed recovery member history structure differs")
    return hashlib.sha256((output / "recovery.json").read_bytes()).hexdigest()


def _installation_platform() -> str:
    platform = {
        "darwin": "macos-arm64",
        "linux": "linux-x64",
        "win32": "windows-x64",
    }.get(sys.platform)
    if platform is None:
        raise QualificationError(f"unsupported qualification platform: {sys.platform}")
    return platform


def _install_command(component: dict[str, Any], manifest: dict[str, Any]) -> list[str]:
    platform = _installation_platform()
    command = [
        "uv",
        "--no-config",
        "--allow-insecure-host",
        "127.0.0.1",
        "tool",
        "install",
        f"{component['root']}=={manifest['version']}",
        "--index",
        str(manifest["index"]["url"]),
        "--default-index",
        "https://pypi.org/simple",
        "--index-strategy",
        "first-index",
        "--python",
        str(manifest["toolchain"]["python"]),
        "--managed-python",
        "--no-build",
    ]
    for item in component["platform_requirements"][platform]:
        if item["name"] != component["root"]:
            command.extend(("--with", str(item["requirement"])))
    return command


def _installed_client_write_bank(
    python: Path, *, scratch: Path, environment: dict[str, str]
) -> dict[str, Any]:
    code = r"""import ctypes,hashlib,json,sqlite3,tempfile
from pathlib import Path
from riverhog_client import _prepared_primary_native as native
limits=(4096,2,32768,32768,65536,32)
recovery=(4096,8,32768,32768,65536,32)
quota=native.WriteQuota(limits,recovery)
assert quota.admit("database",8192,0)
assert not quota.admit("database",4096,8192)
assert quota.recovering
assert quota.admit("database",12288,0)
report=quota.report()
assert report["forward"]["database_pages_attempted"]==2
assert report["recovery"]["database_pages_attempted"]==3
assert report["refusals"]==1
assert report["first_refusal"]["reason"]=="database page grant"
module=Path(native.__file__)
assert module.suffix.lower()==".pyd"
assert native.compiler.startswith("MSVC ")
with sqlite3.connect(":memory:") as runtime:
    source_id=runtime.execute("SELECT sqlite_source_id()").fetchone()[0]
sleep_limits=(4096,1,4096,4096,8192,8)
sleep_writes=native.WriteQuota(sleep_limits,sleep_limits)
sleep_calls=native.CallbackQuota(
    sleep_writes,(4,1000),(4,1000),
    work=native.NativeWorkQuota(sleep_writes, (1 << 40, 1 << 60), (1 << 40, 1 << 60)),
    buffers=(1 << 40, 1 << 40),
)
assert sleep_calls.admit_sleep(1)
assert not sleep_calls.admit_sleep(1)
assert not sleep_calls.admit_sleep(1)
assert not sleep_calls.admit()
assert sleep_calls.admit_sleep(1)
sleep_report=sleep_calls.report()
sleep_refused=sleep_writes.report()["first_refusal"]["reason"]=="VFS sleep grant"
assert sleep_refused and not sleep_report["pending_error"]
for method,amount,expected in (
    ("read",7,(4,9000)),("write",7,(10,9000)),
    ("open_database",0,(24,63000)),("open_journal_pinned",0,(30,63000)),
    ("open_journal_new",0,(32,63000)),("bind_journal",0,(10,0)),
    ("close",0,(3,200000)),("pin_close",0,(1,0)),
):
    assert native.filesystem_cost(method,(2,3),amount)==expected
work_writes=native.WriteQuota(sleep_limits,sleep_limits)
work=native.NativeWorkQuota(work_writes,(4,200000),(4,200000))
credit=work.acquire((1,0),(3,200000))
assert credit is not None and credit.report()["pending"]
work_writes.failed()
assert work.admit((1,0))
assert not work.admit((1,0))
work_refused=work_writes.report()["first_refusal"]["reason"]=="native filesystem attempt grant"
work_report=work.report()
for bank in ("forward","recovery"):
    assert work_report[bank]["filesystem_api_attempts_charged"]==4
    assert work_report[bank]["internal_sleep_microseconds_charged"]==200000
    assert work_report[bank]["close_filesystem_api_attempts_prepaid"]==3
    assert work_report[bank]["close_internal_sleep_microseconds_prepaid"]==200000
assert work_refused
del work,work_writes
credit.consume()
assert not credit.report()["pending"]
try:
    credit.consume()
except ValueError:
    pass
else:
    raise AssertionError("consumed native close credit was reusable")
output_writes=native.WriteQuota(sleep_limits,sleep_limits)
output_calls=native.CallbackQuota(
    output_writes,(8,0),(8,0),
    work=native.NativeWorkQuota(output_writes,(0,0),(0,0)),
    buffers=(7,11),
)
assert output_calls.admit_buffer(7,False)
assert not output_calls.admit_buffer(1,False)
assert not output_calls.admit_buffer(11,True)
assert output_calls.admit_buffer(11,True)
assert not output_calls.admit_buffer(1,True)
output_report=output_calls.report()
assert output_report["forward"]["output_buffer_bytes_charged"]==7
assert output_report["recovery"]["output_buffer_bytes_charged"]==11
assert output_writes.report()["first_refusal"]["reason"]=="VFS output-buffer grant"
vfs_limits=(4096,128,1048576,1048576,8388608,1024)
vfs_quota=native.WriteQuota(vfs_limits,vfs_limits)
vfs_reads=native.ReadQuota(vfs_quota,(8388608,1024),(8388608,1024))
vfs_callbacks=native.CallbackQuota(
    vfs_quota,(8192, 0),(8192, 0),
    work=native.NativeWorkQuota(vfs_quota, (1 << 40, 1 << 60), (1 << 40, 1 << 60)),
    buffers=(1 << 40, 1 << 40),
)
from riverhog_client._prepared_primary_runtime import _admit_process_budget, _qualified_runtime
try:
    native.current_process_budget()
except RuntimeError as exc:
    assert "not configured" in str(exc)
else:
    raise AssertionError("installed client acquired a heap owner before binary admission")
qualified_runtime=_qualified_runtime()
assert qualified_runtime["sqlite_source_id"]==source_id
heap=_admit_process_budget(67108864)
vfs=native.WriteVfs(source_id,vfs_reads,vfs_callbacks,heap)
try:
    identity=vfs.identity()
    assert identity["sqlite_source_id"]==source_id
    assert identity["base_vfs"]=="win32"
    library_digest=hashlib.sha256(Path(identity["sqlite_library_path"]).read_bytes()).hexdigest()
    assert library_digest==qualified_runtime["sqlite_library_sha256"]
    with tempfile.TemporaryDirectory(prefix="native-write-proof-",dir=".") as directory:
        uri=(Path(directory)/"primary.sqlite3").resolve().as_uri()+"?vfs="+vfs.name
        path=Path(directory)/"primary.sqlite3"
        path.touch()
        kernel=ctypes.WinDLL("kernel32",use_last_error=True)
        kernel.CreateFileW.argtypes=[ctypes.c_wchar_p,ctypes.c_uint32,ctypes.c_uint32,
            ctypes.c_void_p,ctypes.c_uint32,ctypes.c_uint32,ctypes.c_void_p]
        kernel.CreateFileW.restype=ctypes.c_void_p
        kernel.CloseHandle.argtypes=[ctypes.c_void_p]
        kernel.CloseHandle.restype=ctypes.c_int
        handle=kernel.CreateFileW(str(path.resolve()),0x80,3,None,3,0x00200000,None)
        assert handle not in (None,ctypes.c_void_p(-1).value)
        connection=None
        try:
            vfs.bind(str(path.resolve()),handle,None)
            assert vfs.bound
            bootstrap_cost=native.retry_bootstrap_cost((10,25))
            assert bootstrap_cost==(171,20825000)
            vfs.admit_retries((10,25),(2,3),bootstrap_cost)
            retry_report=heap.retry_report()
            assert retry_report["state"]=="admitted"
            assert retry_report["selected_retry_count"]==2
            assert retry_report["selected_delay_milliseconds"]==3
            assert retry_report["native_close_attempted"]
            assert retry_report["native_result"]==retry_report["native_close_result"]==0
            assert retry_report["first_failure_stage"]==""
            connection=sqlite3.connect(uri,uri=True)
            assert connection.execute("PRAGMA journal_mode=PERSIST").fetchone()==("persist",)
            connection.execute("PRAGMA synchronous=EXTRA")
            connection.execute("PRAGMA journal_size_limit=4096")
            connection.execute("CREATE TABLE proof (body BLOB NOT NULL)")
            connection.execute("INSERT INTO proof VALUES (?)",(b"exact installed original",))
            connection.commit()
            row=connection.execute("SELECT body FROM proof").fetchone()
            assert row==(b"exact installed original",)
        finally:
            if connection is not None:
                connection.close()
            journal=path.with_name(path.name+"-journal")
            original_files=(path.read_bytes(),journal.read_bytes())
            vfs_work_report=vfs_callbacks.work.report()
            stopped_writes=native.WriteQuota(vfs_limits,vfs_limits)
            stopped_reads=native.ReadQuota(stopped_writes,(8388608,1024),(8388608,1024))
            stopped_work=native.NativeWorkQuota(stopped_writes,(0,0),(0,0))
            stopped_callbacks=native.CallbackQuota(
                stopped_writes,(32,0),(32,0),stopped_work,
                buffers=(1 << 40, 1 << 40),
            )
            vfs.arm(stopped_reads,stopped_callbacks)
            try:
                unexpected=sqlite3.connect(uri,uri=True)
            except sqlite3.Error as exc:
                assert exc.sqlite_errorcode==sqlite3.SQLITE_FULL
            else:
                unexpected.close()
                raise AssertionError("native filesystem work bypassed an empty grant")
            assert stopped_writes.recovering and vfs.open_files==0 and not vfs.quarantined
            stopped_reason=stopped_writes.report()["first_refusal"]["reason"]
            assert stopped_reason=="native filesystem attempt grant"
            assert all(not any(bank.values()) for bank in stopped_work.report().values())
            output_stopped=native.WriteQuota(vfs_limits,vfs_limits)
            output_reads=native.ReadQuota(output_stopped,(8388608,1024),(8388608,1024))
            output_work=native.NativeWorkQuota(output_stopped,(1<<40,1<<60),(1<<40,1<<60))
            output_empty=native.CallbackQuota(
                output_stopped,(32,0),(32,0),output_work,buffers=(0,0),
            )
            vfs.arm(output_reads,output_empty)
            try:
                unexpected=sqlite3.connect(uri,uri=True)
            except sqlite3.Error as exc:
                assert exc.sqlite_errorcode==sqlite3.SQLITE_FULL
            else:
                unexpected.close()
                raise AssertionError("native connection bypassed an empty output-byte grant")
            assert output_stopped.recovering and vfs.open_files==0 and not vfs.quarantined
            assert output_stopped.report()["first_refusal"]["reason"]=="VFS output-buffer grant"
            assert all(not any(bank.values()) for bank in output_work.report().values())
            assert all(
                not bank["output_buffer_bytes_charged"]
                for key,bank in output_empty.report().items()
                if key in ("forward","recovery")
            )
            vfs.unbind()
            assert not vfs.bound and not vfs.quarantined
            assert all(not any(bank.values()) for bank in stopped_work.report().values())
            assert all(not any(bank.values()) for bank in output_work.report().values())
            assert kernel.CloseHandle(handle)
            assert (path.read_bytes(),journal.read_bytes())==original_files
        moved=path.with_name("primary-moved.sqlite3")
        path.rename(moved)
        moved.rename(path)
        assert heap.retry_report()==retry_report
    assert vfs.open_files==0
    read_report=vfs_reads.report()["forward"]
    resize_calls=vfs_quota.resize_report()["forward"]["resize_calls"]
    callback_report=vfs_callbacks.report()
    callback_slots=callback_report["forward"]["callback_slots_charged"]
    assert vfs_work_report["forward"]["filesystem_api_attempts_charged"]>0
    assert vfs_work_report["forward"]["internal_sleep_microseconds_charged"]>0
    assert vfs_work_report["recovery"]["close_filesystem_api_attempts_prepaid"]>0
    heap_report=heap.report()
    assert heap_report["heap_limit_bytes"]==67108864
    assert callback_slots>0 and not callback_report["pending_error"]
finally:
    vfs.close()
vfs_calls=vfs_quota.report()["forward"]["write_calls"]
assert vfs_calls>0 and read_report["read_calls"]>0 and resize_calls>0 and not vfs_quota.recovering
print(json.dumps({"compiler":native.compiler,
    "module_sha256":hashlib.sha256(module.read_bytes()).hexdigest(),
    "tracking_bytes":quota.tracking_bytes,
    "forward_pages":2,"recovery_pages":3,"refusals":1,
    "storage_callbacks_native":True,"vfs_write_calls":vfs_calls,
    "vfs_resize_calls":resize_calls,"vfs_callback_slots":callback_slots,
    "vfs_read_calls":read_report["read_calls"],"vfs_read_bytes":read_report["read_bytes"],
    "sqlite_heap_limit_bytes":heap_report["heap_limit_bytes"],
    "sqlite_heap_peak_bytes":heap_report["sqlite_heap_peak_bytes"],
    "sqlite_logger_disabled_at_admission":heap_report["logger_disabled_at_admission"],
    "callback_buffer_forward_bytes":output_report["forward"]["output_buffer_bytes_charged"],
    "callback_buffer_recovery_bytes":output_report["recovery"]["output_buffer_bytes_charged"],
    "vfs_auxiliary_buffer_bytes":callback_report["forward"]["output_buffer_bytes_charged"],
    "vfs_buffer_refusal_verified":True,
    "callback_sleep_forward_microseconds":sleep_report["forward"]["sleep_microseconds_charged"],
    "callback_sleep_recovery_microseconds":sleep_report["recovery"]["sleep_microseconds_charged"],
    "callback_sleep_refusal_verified":sleep_refused,
    "retry_bootstrap_filesystem_api_attempts_prepaid":
        retry_report["bootstrap_filesystem_api_attempts_prepaid"],
    "retry_bootstrap_sleep_microseconds_prepaid":
        retry_report["bootstrap_sleep_microseconds_prepaid"],
    "retry_count":retry_report["selected_retry_count"],
    "retry_delay_milliseconds":retry_report["selected_delay_milliseconds"],
    "retry_admission_verified":retry_report["state"]=="admitted",
    "retry_bootstrap_close_verified":retry_report["native_close_attempted"]
        and retry_report["native_close_result"]==0,
    "retry_bootstrap_pin_released":True,
    "native_work_bank_forward_attempts":work_report["forward"]["filesystem_api_attempts_charged"],
    "native_work_bank_recovery_attempts":work_report["recovery"]["filesystem_api_attempts_charged"],
    "native_work_bank_forward_sleep_microseconds":
        work_report["forward"]["internal_sleep_microseconds_charged"],
    "native_work_bank_recovery_sleep_microseconds":
        work_report["recovery"]["internal_sleep_microseconds_charged"],
    "native_work_bank_refusal_verified":work_refused,
    "native_filesystem_recipes_verified":True,
    "runtime_binary_admission_verified":True,
    "native_close_credit_detached_verified":not credit.report()["pending"],
    "vfs_native_filesystem_attempts":
        vfs_work_report["forward"]["filesystem_api_attempts_charged"],
    "vfs_native_internal_sleep_microseconds":
        vfs_work_report["forward"]["internal_sleep_microseconds_charged"],
    "vfs_native_recovery_close_attempts_prepaid":
        vfs_work_report["recovery"]["close_filesystem_api_attempts_prepaid"],
    "vfs_native_work_refusal_verified":True,
    "vfs_native_binding_close_after_rearm_verified":True,
    "vfs_binding_verified":True,
    "vfs_original_exact":True,"sqlite_source_id":source_id,
    "sqlite_library_sha256":library_digest}))
"""
    result = _run([str(python), "-I", "-c", code], cwd=scratch, env=environment, capture=True)
    proof = json.loads(result.stdout)
    if (
        not isinstance(proof, dict)
        or proof.get("forward_pages") != 2
        or proof.get("recovery_pages") != 3
        or proof.get("refusals") != 1
        or type(proof.get("tracking_bytes")) is not int
        or proof["tracking_bytes"] <= 0
        or re.fullmatch(r"[0-9a-f]{64}", str(proof.get("module_sha256", ""))) is None
        or not str(proof.get("compiler", "")).startswith("MSVC ")
        or proof.get("storage_callbacks_native") is not True
        or type(proof.get("vfs_write_calls")) is not int
        or proof["vfs_write_calls"] <= 0
        or type(proof.get("vfs_resize_calls")) is not int
        or proof["vfs_resize_calls"] <= 0
        or type(proof.get("vfs_read_calls")) is not int
        or proof["vfs_read_calls"] <= 0
        or type(proof.get("vfs_read_bytes")) is not int
        or proof["vfs_read_bytes"] <= 0
        or type(proof.get("vfs_callback_slots")) is not int
        or proof["vfs_callback_slots"] <= 0
        or type(proof.get("callback_sleep_forward_microseconds")) is not int
        or proof["callback_sleep_forward_microseconds"] != 1000
        or type(proof.get("callback_sleep_recovery_microseconds")) is not int
        or proof["callback_sleep_recovery_microseconds"] != 1000
        or proof.get("callback_sleep_refusal_verified") is not True
        or type(proof.get("retry_bootstrap_filesystem_api_attempts_prepaid")) is not int
        or proof["retry_bootstrap_filesystem_api_attempts_prepaid"] != 171
        or type(proof.get("retry_bootstrap_sleep_microseconds_prepaid")) is not int
        or proof["retry_bootstrap_sleep_microseconds_prepaid"] != 20825000
        or type(proof.get("retry_count")) is not int
        or proof["retry_count"] != 2
        or type(proof.get("retry_delay_milliseconds")) is not int
        or proof["retry_delay_milliseconds"] != 3
        or proof.get("retry_admission_verified") is not True
        or proof.get("retry_bootstrap_close_verified") is not True
        or proof.get("retry_bootstrap_pin_released") is not True
        or type(proof.get("native_work_bank_forward_attempts")) is not int
        or proof["native_work_bank_forward_attempts"] != 4
        or type(proof.get("native_work_bank_recovery_attempts")) is not int
        or proof["native_work_bank_recovery_attempts"] != 4
        or type(proof.get("native_work_bank_forward_sleep_microseconds")) is not int
        or proof["native_work_bank_forward_sleep_microseconds"] != 200000
        or type(proof.get("native_work_bank_recovery_sleep_microseconds")) is not int
        or proof["native_work_bank_recovery_sleep_microseconds"] != 200000
        or proof.get("native_work_bank_refusal_verified") is not True
        or proof.get("native_filesystem_recipes_verified") is not True
        or proof.get("native_close_credit_detached_verified") is not True
        or type(proof.get("vfs_native_filesystem_attempts")) is not int
        or proof["vfs_native_filesystem_attempts"] <= 0
        or type(proof.get("vfs_native_internal_sleep_microseconds")) is not int
        or proof["vfs_native_internal_sleep_microseconds"] <= 0
        or type(proof.get("vfs_native_recovery_close_attempts_prepaid")) is not int
        or proof["vfs_native_recovery_close_attempts_prepaid"] <= 0
        or proof.get("vfs_native_work_refusal_verified") is not True
        or proof.get("vfs_native_binding_close_after_rearm_verified") is not True
        or proof.get("runtime_binary_admission_verified") is not True
        or proof.get("sqlite_logger_disabled_at_admission") is not True
        or type(proof.get("callback_buffer_forward_bytes")) is not int
        or proof["callback_buffer_forward_bytes"] != 7
        or type(proof.get("callback_buffer_recovery_bytes")) is not int
        or proof["callback_buffer_recovery_bytes"] != 11
        or type(proof.get("vfs_auxiliary_buffer_bytes")) is not int
        or proof["vfs_auxiliary_buffer_bytes"] <= 0
        or proof.get("vfs_buffer_refusal_verified") is not True
        or type(proof.get("sqlite_heap_limit_bytes")) is not int
        or proof.get("sqlite_heap_limit_bytes") != 67108864
        or type(proof.get("sqlite_heap_peak_bytes")) is not int
        or not 0 < proof["sqlite_heap_peak_bytes"] <= 67108864
        or proof.get("vfs_binding_verified") is not True
        or proof.get("vfs_original_exact") is not True
        or not isinstance(proof.get("sqlite_source_id"), str)
        or not proof["sqlite_source_id"]
        or re.fullmatch(r"[0-9a-f]{64}", str(proof.get("sqlite_library_sha256", ""))) is None
    ):
        raise QualificationError("installed native client write-bank proof is invalid")
    return proof


def _qualify_component(
    component: dict[str, Any],
    manifest: dict[str, Any],
    *,
    source_root: Path,
    artifact_root: Path,
    scratch: Path,
    base_url: str,
    all_project_names: set[str],
    listener_lifecycle: bool,
    listener_lifecycle_repetitions: int,
    gogurt_evidence_dir: Path | None,
    listener_mount_fixture: ListenerMountFixture | None = None,
) -> dict[str, Any]:
    root = str(component["root"])
    environment = _tool_environment(scratch, root)
    tool_dir = Path(environment["UV_TOOL_DIR"])
    bin_dir = Path(environment["UV_TOOL_BIN_DIR"])
    lock = artifact_root / str(component["lock"]["path"])
    command = _install_command(component, manifest)
    request_offset = len(QualificationHandler.requests)
    _run(command, cwd=scratch, env=environment)
    _run(command, cwd=scratch, env=environment)
    _run([*command, "--reinstall"], cwd=scratch, env=environment)

    python = _tool_python(tool_dir, root)
    if _python_version(python, scratch, environment) != manifest["toolchain"]["python"]:
        raise QualificationError(f"{root} did not use the exact managed CPython patch")
    inventory = _installed_inventory(python, scratch, environment)
    platform = _installation_platform()
    expected_first_party = {
        str(item["name"]): str(item["version"])
        for item in component["first_party_closure"][platform]
    }
    actual_first_party = {
        name: version for name, version in inventory.items() if name in all_project_names
    }
    if actual_first_party != expected_first_party:
        raise QualificationError(f"{root} installed another first-party closure")
    native_write_bank = None
    if platform == "windows-x64" and "riverhog-client" in expected_first_party:
        native_write_bank = _installed_client_write_bank(
            python, scratch=scratch, environment=environment
        )
    locked = {str(item["name"]): str(item["version"]) for item in component["resolved_packages"]}
    if any(locked.get(name) != version for name, version in inventory.items()):
        raise QualificationError(f"{root} installed a version outside its PEP 751 lock")
    sync = _run(
        [
            "uv",
            "--no-config",
            "--allow-insecure-host",
            "127.0.0.1",
            "pip",
            "sync",
            str(lock),
            "--python",
            str(python),
            "--dry-run",
            "--strict",
            "--no-build",
        ],
        cwd=scratch,
        env=environment,
        capture=True,
    )
    if "Would make no changes" not in sync.stdout + sync.stderr:
        raise QualificationError(f"{root} is not already synchronized to its PEP 751 lock")

    qualification_first_party: dict[str, str] = {}
    gogurt_providers: dict[str, str] | None = None
    if root == "gogurt":
        raw_reference = manifest["qualification"]["gogurt_providers"]["platforms"][platform]
        if not isinstance(raw_reference, dict):
            raise QualificationError("Gogurt provider qualification is invalid")
        reference_fields = (
            "listener_host_distribution",
            "listener_host_provider",
            "mounted_volume_distribution",
            "mounted_volume_provider",
        )
        gogurt_providers = {key: str(raw_reference[key]) for key in reference_fields}
        raw_reference_closure = raw_reference.get("first_party_closure")
        if not isinstance(raw_reference_closure, list):
            raise QualificationError("Gogurt provider qualification closure is invalid")
        qualification_first_party = {
            str(item["name"]): str(item["version"])
            for item in raw_reference_closure
            if isinstance(item, dict) and set(item) == {"name", "version"}
        }
        if len(qualification_first_party) != len(raw_reference_closure):
            raise QualificationError("Gogurt provider qualification closure is invalid")
        distributions = (
            gogurt_providers["mounted_volume_distribution"],
            gogurt_providers["listener_host_distribution"],
        )
        qualified_version = str(manifest["version"])
        _run(
            [
                "uv",
                "--no-config",
                "--allow-insecure-host",
                "127.0.0.1",
                "pip",
                "install",
                *(f"{distribution}=={qualified_version}" for distribution in distributions),
                "--index",
                str(manifest["index"]["url"]),
                "--default-index",
                "https://pypi.org/simple",
                "--index-strategy",
                "first-index",
                "--python",
                str(python),
                "--no-build",
                "--strict",
            ],
            cwd=scratch,
            env=environment,
        )
        qualified_inventory = _installed_inventory(python, scratch, environment)
        qualified_first_party = {
            name: installed_version
            for name, installed_version in qualified_inventory.items()
            if name in all_project_names
        }
        if qualified_first_party != expected_first_party | qualification_first_party:
            raise QualificationError("Gogurt provider qualification changed another package")

    entry_points = [str(value) for value in component["entry_points"]]
    executables = [_executable(bin_dir, name) for name in entry_points]
    primary = executables[0]
    version_result = _run([str(primary), "--version"], cwd=scratch, env=environment, capture=True)
    if version_result.stdout.strip() != manifest["version"]:
        raise QualificationError(f"{root} --version differs from the installed release")
    _run([str(primary), "--help"], cwd=scratch, env=environment, capture=True)

    if root in {"a-riverhog-cli", "a-stove0-cli"}:
        operation = _run_client_operation(
            root,
            primary,
            base_url=base_url,
            cwd=scratch,
            environment=environment,
        )
    elif root == "gogurt":
        assert gogurt_providers is not None
        operation = _run_gogurt(
            primary,
            source_root=source_root,
            scratch=scratch,
            environment=environment,
            listener_lifecycle=listener_lifecycle,
            listener_lifecycle_repetitions=listener_lifecycle_repetitions,
            gogurt_evidence_dir=gogurt_evidence_dir,
            reference=gogurt_providers,
            listener_mount_fixture=listener_mount_fixture,
        )
    else:
        operation = _run_recovery(
            primary,
            scratch=scratch,
            environment=environment,
        )

    requests = QualificationHandler.requests[request_offset:]
    wheel_assets = {
        "/assets/" + asset
        for asset in installation.platform_wheel_assets(
            manifest, set(expected_first_party | qualification_first_party), platform=platform
        )
    }
    if not wheel_assets <= set(requests):
        raise QualificationError(f"{root} did not consume every staged first-party wheel")
    if any(path.endswith((".tar.gz", ".zip")) for path in requests):
        raise QualificationError(f"{root} consumed a non-wheel staged distribution")

    _run(
        ["uv", "--no-config", "tool", "uninstall", root],
        cwd=scratch,
        env=environment,
    )
    if (tool_dir / root).exists() or any(path.exists() for path in executables):
        raise QualificationError(f"{root} uninstall left an installed environment or entry point")
    return {
        "root": root,
        "first_party": expected_first_party,
        "qualification_first_party": qualification_first_party,
        "entry_points": entry_points,
        "operation": operation,
        "python": manifest["toolchain"]["python"],
        "reinstall": "passed",
        "uninstall": "passed",
        "wheel_only": True,
        "native_client_write_bank": native_write_bank,
    }


def qualify(
    root: Path,
    *,
    version: str,
    listener_lifecycle: bool = False,
    listener_lifecycle_repetitions: int = 1,
    gogurt_evidence_dir: Path | None = None,
    listener_mount_fixture: Path | None = None,
    listener_mount_lock: Path | None = None,
) -> dict[str, Any]:
    if listener_lifecycle_repetitions < 1:
        raise QualificationError("listener lifecycle repetitions must be at least one")
    if listener_mount_fixture is not None and not listener_lifecycle:
        raise QualificationError(
            "an operator mount fixture requires listener lifecycle qualification"
        )
    if listener_mount_lock is not None and listener_mount_fixture is None:
        raise QualificationError("a listener mount lock requires an operator mount fixture")
    fixture = (
        _load_listener_mount_fixture(listener_mount_fixture, lock_file=listener_mount_lock)
        if listener_mount_fixture is not None
        else None
    )
    release._ensure_clean(root)
    source_sha = release._source_sha(root)
    actual_uv = _run(["uv", "--version"], cwd=root, capture=True).stdout.split()[1]
    tools = installation.qualified_tool_versions(root)
    if actual_uv != tools["uv"]:
        raise QualificationError("qualification is not running the mise-locked uv version")
    with tempfile.TemporaryDirectory(prefix="riverhog-install-qualification.") as temporary:
        scratch = Path(temporary)
        web_root = scratch / "web"
        web_root.mkdir()
        server, thread, base_url = _server(web_root)
        try:
            manifest = _stage_artifacts(
                root,
                scratch,
                version=version,
                source_sha=source_sha,
                base_url=base_url,
            )
            artifact_root = scratch / "installation"
            expected_index = base_url + "/" + str(manifest["index"]["path"])
            if manifest["index"]["url"] != expected_index:
                raise QualificationError("staged index URL does not match its HTTP origin")
            projects = release.validate_release_contract(root)
            all_project_names = {project.name for project in projects}
            results = [
                _qualify_component(
                    component,
                    manifest,
                    source_root=root,
                    artifact_root=artifact_root,
                    scratch=scratch,
                    base_url=base_url,
                    all_project_names=all_project_names,
                    listener_lifecycle=listener_lifecycle,
                    listener_lifecycle_repetitions=listener_lifecycle_repetitions,
                    gogurt_evidence_dir=gogurt_evidence_dir,
                    listener_mount_fixture=fixture,
                )
                for component in manifest["components"]
            ]
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
    return {
        "format": FORMAT,
        "source_sha": source_sha,
        "version": version,
        "uv": tools["uv"],
        "python": tools["python"],
        "platform": sys.platform,
        "components": results,
        "staged_http": "passed",
        "published": False,
        "listener_lifecycle": listener_lifecycle,
        "listener_lifecycle_repetitions": listener_lifecycle_repetitions,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--summary", type=Path)
    parser.add_argument(
        "--listener-lifecycle",
        action="store_true",
        help="Exercise the real per-user service manager and a disposable mounted volume.",
    )
    parser.add_argument(
        "--listener-lifecycle-repetitions",
        type=int,
        default=1,
        help="Run isolated listener lifecycles repeatedly from the same candidate artifacts.",
    )
    parser.add_argument(
        "--listener-mount-fixture",
        type=Path,
        help=(
            "Linux operator JSON with an existing absolute mount_point and setup/teardown "
            "argument lists; commands run unattended with a 30-second limit."
        ),
    )
    parser.add_argument(
        "--listener-mount-lock",
        type=Path,
        help="Hold this shared Linux fixture lock through listener settling and teardown.",
    )
    parser.add_argument(
        "--gogurt-evidence-dir",
        type=Path,
        help="Retain bounded dummy lifecycle status and logs when Gogurt qualification fails.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        payload = qualify(
            ROOT,
            version=args.version,
            listener_lifecycle=args.listener_lifecycle,
            listener_lifecycle_repetitions=args.listener_lifecycle_repetitions,
            gogurt_evidence_dir=args.gogurt_evidence_dir,
            listener_mount_fixture=args.listener_mount_fixture,
            listener_mount_lock=args.listener_mount_lock,
        )
    except (
        OSError,
        QualificationError,
        installation.InstallationError,
        release.ReleaseError,
        subprocess.CalledProcessError,
        ValueError,
    ) as exc:
        raise SystemExit(f"installation qualification error: {exc}") from exc
    rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.summary is not None:
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(rendered, encoding="utf-8")
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
