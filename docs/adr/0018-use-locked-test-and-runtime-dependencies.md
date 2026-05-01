# ADR-0018: Use Locked Test and Runtime Dependencies

## Decision

Riverhog runs local quality lanes in locked uv environments and builds containers from hashed lockfiles.

## Reason

The project needs reproducible checks and container builds without allowing test/runtime dependency drift.
