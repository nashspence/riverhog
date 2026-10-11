"""Apply growth amendment to the approved pre-v1 hard-cut guide without modifying it."""
import argparse
import hashlib
from pathlib import Path
import subprocess
import tempfile

BEFORE = "b954c2965509ce7e69361dd6663646f8920ed5d707733e671ab2425d7c77cd4b"
AFTER = "1fe4e9a92c4df98949dfe9b52d9d495f186281502f348151491950b72325d39c"

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("guide", type=Path, help="the current #961 approved onboarding text (Alembic v1 baseline retained; no superseded pre-v1 schema support)")
    args = parser.parse_args()
    data = args.guide.read_bytes()
    if hashlib.sha256(data).hexdigest() != BEFORE:
        raise SystemExit("Guide differs from corrected #961 approved input; reconcile, do not bypass the guard.")
    patch = Path(__file__).resolve().with_name("AGENTS.growth.patch")
    with tempfile.TemporaryDirectory() as scratch:
        target = Path(scratch) / "AGENTS.md"
        target.write_bytes(data)
        subprocess.run(["git", "apply", "--check", str(patch)], cwd=scratch, check=True)
        subprocess.run(["git", "apply", str(patch)], cwd=scratch, check=True)
        if hashlib.sha256(target.read_bytes()).hexdigest() != AFTER:
            raise SystemExit("Patched guide differs from exact approved amendment.")
    print("Approved Alembic-preserving v1 guide and growth amendment verified; input was not modified.")

if __name__ == "__main__":
    main()
