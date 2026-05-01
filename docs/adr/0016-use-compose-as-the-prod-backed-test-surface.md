# ADR-0016: Use Compose as the Prod-Backed Test Surface

## Decision

Riverhog uses the checked-in Compose stack as the canonical acceptance-test surface for production-backed behavior.

## Reason

The acceptance harness needs to exercise the app with real service boundaries while keeping the stack reproducible locally.
