"""Read exact source identity without exposing Git status contents."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path


def source_identity(root: Path) -> dict[str, object]:
    try:
        checkout = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=False,
        )
        if checkout.returncode != 0 or Path(checkout.stdout.strip()).resolve() != root.resolve():
            return {"source_sha": "", "source_clean": False}
        revision = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--verify", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
        status = subprocess.run(
            ["git", "-C", str(root), "status", "--porcelain=v1", "--untracked-files=all"],
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        return {"source_sha": "", "source_clean": False}
    sha = revision.stdout.strip()
    verified = revision.returncode == 0 and re.fullmatch(r"[0-9a-f]{40}", sha) is not None
    return {
        "source_sha": sha if verified else "",
        "source_clean": verified and status.returncode == 0 and not status.stdout.strip(),
    }
