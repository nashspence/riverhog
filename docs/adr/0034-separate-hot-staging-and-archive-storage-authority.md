# ADR-0034: Separate Hot, Staging, and Archive Storage Authority

## Decision

Riverhog keeps committed hot files, incomplete upload staging, and collection archive packages as distinct storage concerns.

## Reason

Browsing, resumable upload staging, and cold archive preservation have different authority and lifecycle boundaries.
