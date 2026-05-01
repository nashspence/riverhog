# ADR-0002: Make the API and CLI Contract Explicit

## Decision

Riverhog treats the HTTP API, CLIs, and behavioral invariants as an explicit product contract.

## Reason

The archive system needs stable operator-facing behavior that can be tested, documented, and preserved across implementation changes.
