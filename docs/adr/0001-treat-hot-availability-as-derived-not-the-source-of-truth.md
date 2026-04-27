# ADR 0001: Treat hot availability as derived, not the source of truth

## Status

Accepted.

## Context

Direct user mutation of hot bytes or browsing surfaces makes intent ambiguous and
complicates restore, release, and reconciliation.

## Decision

Hot availability is a derived materialization of authoritative API and catalog
state. The committed hot namespace may be browsed and downloaded, but it is not
an authoritative control surface.

Users express intent through pin and release operations. The API state and
catalog remain the system of record for archive membership, resumable upload
state, and hot residency.

## Consequences

- users express intent through pin and release operations
- the system can reconcile hot state safely
- direct object-store or WebDAV mutation is never authoritative
