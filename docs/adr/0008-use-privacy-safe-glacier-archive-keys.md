# ADR-0008: Use Privacy-Safe Glacier Archive Keys

## Decision

Riverhog stores collection archive packages under hashed collection identifiers rather than raw logical names.

## Reason

Archive object keys must not leak collection IDs or logical file paths.
