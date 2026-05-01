# ADR-0036: Use Outbound Webhooks for Operator Notifications

## Decision

Riverhog emits configured outbound webhooks for operator-relevant archive and recovery events.

## Reason

Recovery readiness and persistent archive failures need notification without creating additional product API surface.
