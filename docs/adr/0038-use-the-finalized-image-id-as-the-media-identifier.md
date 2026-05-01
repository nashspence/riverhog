# ADR-0038: Use the Finalized Image ID as the Media Identifier

## Decision

Riverhog uses the finalized compact UTC image ID as the durable media-facing image identifier.

## Reason

The same artifact needs one stable identity across API records, ISO filenames, disc manifests, copy IDs, labels, and recovery flows.
