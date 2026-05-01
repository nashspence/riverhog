# ADR-0012: Use Pins and Fetch Manifests for Hot Residency

## Decision

Riverhog uses pins to declare desired hot residency and fetch manifests to recover missing bytes.

## Reason

The system needs one stable selector-based mechanism for keeping content hot and for guiding recovery when content is archived-only.
