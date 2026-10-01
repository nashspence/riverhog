"""Independent installed recovery proof over service-produced encrypted objects."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path

from riverhog_canonical_json import canonical_json_bytes


def qualify_offline_history_recovery(
    archive: Path,
    *,
    archive_root_sha256: str,
    passphrases: dict[str, str],
    artifact_id: str,
    payload_bytes: int,
    payload_sha256: str,
    primary: bytes,
    journals: dict[str, bytes],
) -> None:
    command = shutil.which("a-riverhog-recovery-tool")
    if command is None:
        raise RuntimeError("installed recovery tool is required for lifecycle qualification")
    keys = archive.parent / "passphrases.json"
    keys.write_bytes(canonical_json_bytes(passphrases))
    keys.chmod(0o600)
    environment = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("RIVERHOG_", "STOVE0_"))
    }
    for mode in ("declared-hints", "id-layout"):
        output = archive.parent / mode
        result = subprocess.run(
            [
                command,
                str(archive),
                str(output),
                "--passphrases-file",
                str(keys),
                "--layout-mode",
                mode,
                "--expected-archive-root-sha256",
                archive_root_sha256,
            ],
            cwd=archive.parent,
            env=environment,
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        assert result.returncode == 0, result.stderr[-4000:]
        receipt = json.loads((output / "recovery.json").read_bytes())
        assert receipt["archive_root_sha256"] == archive_root_sha256
        members = [
            json.loads(line)
            for line in (output / "recovery-members.jsonseq").read_bytes().splitlines()
        ]
        assert len(members) == 1
        member = members[0]
        assert member["artifact_id"] == artifact_id
        payload = output.joinpath(*member["components"]).read_bytes()
        assert (
            len(payload) == payload_bytes and hashlib.sha256(payload).hexdigest() == payload_sha256
        )
        assert output.joinpath(*member["sidecar"]).read_bytes() == primary
        history = json.loads(output.joinpath(*member["history"]).read_bytes())
        assert history["artifact_id"] == artifact_id and int(history["roots"]["record_count"]) == 2
        for digest, raw in journals.items():
            assert (output / "provenance" / "journals" / (digest + ".jsonseq")).read_bytes() == raw
