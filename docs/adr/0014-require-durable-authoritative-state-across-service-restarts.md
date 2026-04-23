# ADR 0014: Require durable authoritative state across service restarts

## Status

Accepted.

## Context

Operators must be able to stop, restart, or replace the API process while keeping the archive's authoritative state
intact.

## Decision

Authoritative archive state is durable across service restarts.

At minimum, the durable state includes:

- closed collections and their coverage summaries
- finalized images and registered physical copies
- exact pins and the fetch manifests bound to those pins
- hot-residency state and any unexpired resumable-upload progress

Derived projections may be rebuilt during restart, but the externally visible post-restart state must remain equivalent
to the pre-restart state except for behavior that is explicitly time-bound by contract, such as TTL expiry.

## Consequences

- archive resources remain addressable across service restarts
- resumable upload progress remains resumable until the published expiry rules end it
- implementations may rebuild read-only projections from durable authoritative state as an internal detail
