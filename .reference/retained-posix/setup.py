"""Reference build; integration must adopt the repository-locked build policy."""
import os
import sys
from setuptools import Extension, setup

if sys.platform not in {"linux", "darwin"}:
    raise RuntimeError("This reference requires native Linux or Darwin")
setup(ext_modules=[Extension(
    "retained_posix._native", ["native/owner.c"],
    define_macros=[("RETAINED_POSIX_TESTING", "1")] if os.environ.get("RETAINED_POSIX_TESTING") == "1" else [],
    extra_compile_args=["-std=c11", "-Wall", "-Wextra", "-Werror"],
)])
