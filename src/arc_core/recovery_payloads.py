from __future__ import annotations

import base64

FIXTURE_AGE_PREFIX = b"fixture-age-plugin-batchpass/v1\n"


def encrypt_recovery_payload(content: bytes) -> bytes:
    return FIXTURE_AGE_PREFIX + base64.b64encode(content) + b"\n"


def decrypt_recovery_payload(content: bytes) -> bytes:
    if not content.startswith(FIXTURE_AGE_PREFIX):
        raise ValueError("recovery payload is missing the expected prefix")
    return base64.b64decode(content[len(FIXTURE_AGE_PREFIX) :].strip(), validate=True)
