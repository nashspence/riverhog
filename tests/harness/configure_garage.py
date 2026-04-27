from __future__ import annotations

import json

from arc_core.runtime_config import load_runtime_config
from arc_core.stores.s3_support import create_s3_client

EXPECTED_LIFECYCLE_CONFIGURATION = {
    "Rules": [
        {
            "ID": "abort-incomplete-riverhog-uploads",
            "Status": "Enabled",
            "Filter": {},
            "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 3},
        }
    ]
}


def _normalize_lifecycle_configuration(payload: dict[str, object]) -> dict[str, object]:
    rules = []
    for rule in payload.get("Rules", []):
        if not isinstance(rule, dict):
            continue
        rules.append(
            {
                "ID": rule.get("ID"),
                "Status": rule.get("Status"),
                "Filter": rule.get("Filter", {}),
                "AbortIncompleteMultipartUpload": {
                    "DaysAfterInitiation": rule.get("AbortIncompleteMultipartUpload", {}).get(
                        "DaysAfterInitiation"
                    )
                },
            }
        )
    return {"Rules": rules}


def main() -> None:
    config = load_runtime_config()
    client = create_s3_client(config)
    client.put_bucket_lifecycle_configuration(
        Bucket=config.s3_bucket,
        LifecycleConfiguration=EXPECTED_LIFECYCLE_CONFIGURATION,
    )
    actual = client.get_bucket_lifecycle_configuration(Bucket=config.s3_bucket)
    normalized = _normalize_lifecycle_configuration(actual)
    if normalized != EXPECTED_LIFECYCLE_CONFIGURATION:
        raise SystemExit(
            "unexpected lifecycle configuration:\n"
            + json.dumps(normalized, indent=2, sort_keys=True)
        )


if __name__ == "__main__":
    main()
