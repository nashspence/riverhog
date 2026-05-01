from __future__ import annotations

import hashlib
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if len(args) == 2 and args[0] == "stamp":
        manifest_path = Path(args[1])
        digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
        proof_path = manifest_path.with_name(f"{manifest_path.name}.ots")
        proof_path.write_bytes(
            b"OpenTimestamps deterministic harness proof v1\n"
            + f"file: {manifest_path.name}\nsha256: {digest}\n".encode()
        )
        return 0

    if len(args) == 4 and args[0] == "verify" and args[2] == "-f":
        proof_path = Path(args[1])
        manifest_path = Path(args[3])
        digest = hashlib.sha256(manifest_path.read_bytes()).hexdigest().encode()
        if digest not in proof_path.read_bytes():
            print("deterministic proof digest mismatch", file=sys.stderr)
            return 1
        return 0

    if len(args) != 2 or args[0] != "stamp":
        print(
            "usage: ots_stamp_command stamp <manifest> | verify <proof> -f <manifest>",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
