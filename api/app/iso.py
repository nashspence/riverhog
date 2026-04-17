from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

from .config import ISO_AUTHORING_COMMAND
from .storage import iso_volume_label, registered_iso_storage_path


def create_iso_from_partition_root(
    disc_id: str,
    root: Path,
    *,
    requested_label: str | None = None,
) -> Path:
    tool = shutil.which(ISO_AUTHORING_COMMAND)
    if tool is None:
        raise RuntimeError(f"{ISO_AUTHORING_COMMAND} is not installed")
    if not root.exists() or not root.is_dir():
        raise RuntimeError(f"partition root {root} is missing")

    output = registered_iso_storage_path(disc_id)
    output.parent.mkdir(parents=True, exist_ok=True)
    temp = output.with_suffix(".iso.tmp")
    temp.unlink(missing_ok=True)

    label = iso_volume_label(requested_label or disc_id)
    cmd = [
        tool,
        "-as",
        "mkisofs",
        "-iso-level",
        "3",
        "-full-iso9660-filenames",
        "-joliet",
        "-joliet-long",
        "-rational-rock",
        "-udf",
        "-volid",
        label,
        "-o",
        str(temp),
        ".",
    ]
    result = subprocess.run(
        cmd,
        cwd=root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        temp.unlink(missing_ok=True)
        message = (result.stderr or result.stdout).strip() or "iso authoring failed"
        raise RuntimeError(message)

    temp.replace(output)
    return output
