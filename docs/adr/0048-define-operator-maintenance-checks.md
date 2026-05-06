# ADR-0048: Define Operator Maintenance Checks

## Decision

`arc maintenance` owns explicit operator checks for setup health, recovery cost information, and notification delivery.

Each maintenance check has a healthy outcome and a needs-attention outcome in the accepted operator surface.

Operators see recovery cost information, not billing internals, unless they deliberately inspect detailed reports.

Notification delivery tests belong to the notification maintenance check. Live delivery tests are opt-in or harness-controlled; normal maintenance summaries do not send real notifications.

## Reason

Operators need clear maintenance answers that distinguish "nothing to do" from "fix this before relying on Riverhog" without requiring knowledge of internal service names or billing APIs.
