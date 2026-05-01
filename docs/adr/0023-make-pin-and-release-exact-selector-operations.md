# ADR-0023: Make Pin and Release Exact-Selector Operations

## Decision

Riverhog makes pin and release operate only on exact canonical selectors.

## Reason

Overlapping broad and narrow pins must not affect each other accidentally.
