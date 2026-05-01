# ADR-0021: Treat Hot Availability as Derived

## Decision

Riverhog treats hot availability as a materialized projection of catalog state, not as source-of-truth state.

## Reason

Hot bytes can be browsed, downloaded, lost, or rebuilt, but user intent and archive membership must remain unambiguous.
