"""Build the retained POSIX outcome owner with the platform wheel."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        if self.target_name != "wheel" or sys.platform not in self.config["native-platforms"]:
            return

        from setuptools import Distribution, Extension
        from setuptools.command.build_ext import build_ext

        root = Path(self.root)
        extension = Extension(
            "retained_posix._native",
            sources=[str(root / "native/owner.c")],
            extra_compile_args=["-std=c11", "-Wall", "-Wextra", "-Werror"],
        )
        with tempfile.TemporaryDirectory(prefix="riverhog-native-build-") as temporary:
            distribution = Distribution({"ext_modules": [extension]})
            command = build_ext(distribution)
            command.build_lib = str(root / "src")
            command.build_temp = temporary
            command.ensure_finalized()
            command.run()
        build_data["pure_python"] = False
        build_data["infer_tag"] = True
        artifact = Path(command.get_ext_fullpath(extension.name))
        build_data["artifacts"].append(artifact.relative_to(root).as_posix())
