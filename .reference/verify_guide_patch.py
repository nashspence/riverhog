"""Apply growth amendment to the approved pre-v1 hard-cut guide without modifying it."""
import argparse
import hashlib
from pathlib import Path
import subprocess
import tempfile

BEFORE = "871e30337e5452dad2f3ea8377c0139f2b7900036fb14c80ec3fe7397aaeb19b"
AFTER = "8f78a6acba90afb1e300577a5a8d145b69a7597c76e758b5bb84d49780ed8349"

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("guide", type=Path, help="the current corrected #961 approved onboarding text (no migrations or compatibility)")
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
    print("Corrected pre-v1 hard-cut guide and growth amendment verified; input was not modified.")

if __name__ == "__main__":
    main()
