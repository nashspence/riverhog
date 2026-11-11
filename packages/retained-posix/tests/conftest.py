from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def fault_native(tmp_path_factory):
    from setuptools import Distribution, Extension
    from setuptools.command.build_ext import build_ext

    package = Path(__file__).resolve().parents[1]
    output = tmp_path_factory.mktemp("retained-posix-fault-native")
    extension = Extension(
        "_retained_posix_test._native",
        sources=[str(package / "native/owner.c")],
        define_macros=[("RETAINED_POSIX_TESTING", "1")],
        extra_compile_args=["-std=c11", "-Wall", "-Wextra", "-Werror"],
    )
    command = build_ext(Distribution({"ext_modules": [extension]}))
    command.build_lib = str(output)
    command.build_temp = str(output / "build")
    command.ensure_finalized()
    command.run()
    path = Path(command.get_ext_fullpath(extension.name))
    spec = importlib.util.spec_from_file_location(extension.name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert hasattr(module.Resource("file"), "_test_fault")
    return module, path


@pytest.fixture(autouse=True)
def actual_fault_owner(request, monkeypatch):
    if sys.platform not in {"linux", "darwin"}:
        return
    if request.module.__name__.endswith("test_receiving"):
        import retained_posix

        native, path = request.getfixturevalue("fault_native")
        monkeypatch.setattr(request.module, "Resource", native.Resource)
        monkeypatch.setattr(request.module, "Borrow", native.Borrow)
        monkeypatch.setattr(retained_posix, "Resource", native.Resource)
        monkeypatch.setattr(retained_posix, "Borrow", native.Borrow)
        monkeypatch.setattr(request.module, "FAULT_EXTENSION", path)
