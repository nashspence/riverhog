"""Apply the exact guide amendment in scratch; do not modify the supplied guide."""
import argparse
import hashlib
from pathlib import Path
import subprocess
import tempfile

BEFORE = "55f9289fd03100d69c9768790754c87931759cd37b572a100a647f6e4194fcdd"
AFTER = "84595617b207ef5a4ed7ae3703d170a662e59a33531dd018b246f2beee9e365a"

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("guide", type=Path, help="the original approved onboarding text")
    args = parser.parse_args()
    data = args.guide.read_bytes()
    if hashlib.sha256(data).hexdigest() != BEFORE:
        raise SystemExit("Guide differs from approved input; reconcile, do not bypass the guard.")
    patch = Path(__file__).resolve().with_name("AGENTS.growth.patch")
    with tempfile.TemporaryDirectory() as scratch:
        target = Path(scratch) / "AGENTS.md"
        target.write_bytes(data)
        subprocess.run(["git", "apply", "--check", str(patch)], cwd=scratch, check=True)
        subprocess.run(["git", "apply", str(patch)], cwd=scratch, check=True)
        if hashlib.sha256(target.read_bytes()).hexdigest() != AFTER:
            raise SystemExit("Patched guide differs from exact approved amendment.")
    print("Exact onboarding growth amendment verified; input was not modified.")

if __name__ == "__main__":
    main()
