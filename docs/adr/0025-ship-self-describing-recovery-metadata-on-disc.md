# ADR-0025: Ship Self-Describing Recovery Metadata on Disc

## Decision

Every Riverhog disc carries encrypted metadata sufficient to map, verify, and manually recover the represented content.

## Reason

Recovery must remain possible when the API is unavailable or a collection spans multiple discs.
