"""Activate only in the maintained publication helper, never its parent."""

import json
import os
import sys
from pathlib import Path

destination = os.environ.get("RIVERHOG_991_DIAGNOSTIC_DIR")
if destination and sys.orig_argv[1:3] == ["-m", "tests.support.qualification.publication_profile"]:
    try:
        from riverhog_991_probe import install

        install(Path(destination))
    except BaseException as error:
        output = Path(destination)
        output.mkdir(parents=True, exist_ok=True)
        (output / "bootstrap-failure.json").write_text(
            json.dumps(
                {
                    "attribution_valid": False,
                    "error_type": type(error).__name__,
                    "publication_started": False,
                }
            )
            + "\n"
        )
        raise SystemExit("publication diagnostic bootstrap failed") from error
