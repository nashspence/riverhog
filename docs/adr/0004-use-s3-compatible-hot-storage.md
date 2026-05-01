# ADR-0004: Use S3-Compatible Hot Storage

## Decision

Riverhog stores committed hot files in an S3-compatible object store.

## Reason

The project needs a storage boundary that works with the local Garage stack and production-like object-store backends.
