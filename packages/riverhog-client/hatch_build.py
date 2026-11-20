"""Build the private Windows write-accounting extension with the wheel."""

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
            "riverhog_client._prepared_primary_native",
            sources=[
                str(root / "native/quota_module.c"),
                str(root / "native/write_quota.c"),
                str(root / "native/write_vfs.c"),
            ],
            include_dirs=[str(root / "native")],
            extra_compile_args=["/std:c11", "/W4", "/WX", "/Brepro"],
            extra_link_args=["/Brepro"],
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
