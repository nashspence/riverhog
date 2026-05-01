# ADR-0019: Keep Real-Device and Live-AWS Checks Opt-In

## Decision

Riverhog keeps real optical-device validation and live AWS Glacier restore validation outside the default test flow.

## Reason

Those checks require physical devices, credentials, operator action, destructive media writes, live restore behavior, or long cloud latency.
