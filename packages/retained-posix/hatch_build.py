"""Build the retained POSIX outcome owner with the platform wheel."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Any

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        self._build_scratch: tempfile.TemporaryDirectory[str] | None = None
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
        editable = version == "editable"
        temporary = tempfile.TemporaryDirectory(prefix="riverhog-native-build-")
        self._build_scratch = temporary
        try:
            distribution = Distribution({"ext_modules": [extension]})
            command = build_ext(distribution)
            command.build_lib = str(root / "src" if editable else Path(temporary.name) / "lib")
            command.build_temp = str(Path(temporary.name) / "objects")
            command.ensure_finalized()
            command.run()
            artifact = Path(command.get_ext_fullpath(extension.name))
            build_data["pure_python"] = False
            build_data["infer_tag"] = True
            if editable:
                build_data["artifacts"].append(artifact.relative_to(root).as_posix())
                temporary.cleanup()
                self._build_scratch = None
            else:
                build_data["force_include"][str(artifact)] = "retained_posix/" + artifact.name
        except BaseException:
            temporary.cleanup()
            self._build_scratch = None
            raise

    def finalize(self, version: str, build_data: dict[str, Any], artifact_path: str) -> None:
        if self._build_scratch is not None:
            self._build_scratch.cleanup()
            self._build_scratch = None
