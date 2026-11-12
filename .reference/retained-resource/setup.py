"""Reference build; pin matches the audited repository's setuptools lock."""
import os
import sys
from setuptools import Extension, setup

if sys.platform not in {"linux", "darwin", "win32"}:
    raise RuntimeError("Only the researched POSIX/UCRT profiles are selected")
setup(ext_modules=[Extension(
    "retained_resource._native", ["native/owner.c"],
    define_macros=[("RETAINED_RESOURCE_TESTING", "1")]
        if os.environ.get("RETAINED_RESOURCE_TESTING") == "1" else [],
    extra_compile_args=["/std:c11", "/W4"] if sys.platform == "win32"
        else ["-std=c11", "-Wall", "-Wextra", "-Werror"],
)])
