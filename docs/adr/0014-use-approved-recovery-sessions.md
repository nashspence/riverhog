# ADR-0014: Use Approved Recovery Sessions

## Decision

Riverhog restores collections and rebuilds images through durable recovery sessions that require approval before restore work begins.

## Reason

Cold-archive recovery can carry cost, latency, and operator consequences that must be made explicit before execution.
