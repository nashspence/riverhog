# ADR-0006: Use SQLite as Authoritative Catalog

## Decision

Riverhog uses SQLite as the durable authoritative catalog.

## Reason

The MVP needs restart-safe archive state without introducing a separate database service.
