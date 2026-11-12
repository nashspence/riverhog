from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def fault_native(tmp_path_factory):
    from setuptools import Distribution, Extension
    from setuptools.command.build_ext import build_ext

    package = Path(__file__).resolve().parents[2] / "packages/retained-resource"
    output = tmp_path_factory.mktemp("retained-resource-fault-native")
    extension = Extension(
        "_retained_resource_test._native",
        sources=[str(package / "native/owner.c")],
        define_macros=[("RETAINED_RESOURCE_TESTING", "1")],
        extra_compile_args=(
            ["/std:c11", "/W4"]
            if sys.platform == "win32"
            else ["-std=c11", "-Wall", "-Wextra", "-Werror"]
        ),
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
