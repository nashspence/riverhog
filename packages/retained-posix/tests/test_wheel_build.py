"""Normal native wheels preserve source and remain independent of editable artifacts."""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import sysconfig
import zipfile
from pathlib import Path

OWNER = Path(__file__).resolve().parents[1]
NATIVE = sys.platform in {"linux", "darwin"}


def _run(command, *, cwd):
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True, timeout=120)
    assert result.returncode == 0, (result.stdout + result.stderr)[-2800:]
    return result


def _source(tmp_path):
    source = tmp_path / "source"
    shutil.copytree(
        OWNER,
        source,
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.so", "*.pyd", "build", "dist"),
    )
    return source


def _snapshot(source):
    return {
        path.relative_to(source).as_posix(): path.read_bytes()
        for path in source.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc"
    }


def _installed_owner(python, cwd):
    program = """
import os,sys
from pathlib import Path
from retained_posix import Resource,Borrow
if sys.platform not in {'linux','darwin'}:
 try:Resource('file')
 except RuntimeError:pass
 else:raise AssertionError('unsupported native profile accepted')
else:
 path=Path(sys.argv[1])/'data';path.write_bytes(b'owned');path.chmod(0o600)
 root=Resource('directory');root.acquire(os.fsencode(path.parent))
 owner=Resource('file')
 assert not hasattr(owner,'_test_fault')
 created=path.parent/'created'
 owner.acquire(b'created',parent=root,writable=True,create=True,exclusive=True)
 borrow=Borrow(owner);borrow.acquire()
 assert os.fstat(borrow.fileno()).st_mode & 0o777 == 0o600
 os.write(borrow.fileno(),b'original');borrow.close()
 owner.close();owner.close()
 assert owner.received and owner.metrics['close_calls']==1
 existing=Resource('file');existing.acquire(b'created',parent=root,writable=True,create=True)
 existing.close()
 rejected=Resource('file')
 try:rejected.acquire(b'created',parent=root,create=True,exclusive=True)
 except FileExistsError:pass
 else:raise AssertionError('exclusive creation replaced existing storage')
 rejected.close();root.close()
 assert root.received and created.read_bytes()==b'original'
 created.unlink()
"""
    _run([str(python), "-I", "-c", program, str(cwd)], cwd=cwd)


def test_normal_wheel_preserves_source_and_does_not_reuse_an_editable_binary(tmp_path):
    source = _source(tmp_path)
    for existing in (False, True) if NATIVE else (False,):
        artifact = (
            source / "src/retained_posix" / ("_native" + sysconfig.get_config_var("EXT_SUFFIX"))
        )
        if existing:
            artifact.write_bytes(b"independent existing editable artifact")
        before = _snapshot(source)
        wheels = tmp_path / f"wheels-{existing}"
        _run(
            ["uv", "build", "--offline", "--wheel", "--out-dir", str(wheels), str(source)],
            cwd=tmp_path,
        )
        assert _snapshot(source) == before
        (wheel,) = wheels.glob("*.whl")
        with zipfile.ZipFile(wheel) as archive:
            names = archive.namelist()
            assert len(names) == len(set(names))
            native = [name for name in names if name.endswith((".so", ".pyd"))]
            assert bool(native) == NATIVE
            if NATIVE:
                assert len(native) == 1
                assert archive.read(native[0]) != b"independent existing editable artifact"
        environment = tmp_path / f"installed-{existing}"
        _run(["uv", "venv", "--python", sys.executable, str(environment)], cwd=tmp_path)
        python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        _run(
            ["uv", "pip", "install", "--offline", "--no-deps", "--python", str(python), str(wheel)],
            cwd=tmp_path,
        )
        _installed_owner(python, tmp_path)


def test_editable_install_keeps_its_actual_native_owner_importable(tmp_path):
    source = _source(tmp_path)
    environment = tmp_path / "editable"
    _run(["uv", "venv", "--python", sys.executable, str(environment)], cwd=tmp_path)
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    _run(
        [
            "uv",
            "pip",
            "install",
            "--offline",
            "--no-deps",
            "--python",
            str(python),
            "--no-sources",
            "-e",
            str(source),
        ],
        cwd=tmp_path,
    )
    _installed_owner(python, tmp_path)
    assert bool(list((source / "src/retained_posix").glob("_native*.so"))) == NATIVE
