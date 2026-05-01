# ADR-0031: Route Total Physical Loss Through Image Rebuild

## Decision

Riverhog handles finalized images with no protected copies through image-rebuild recovery sessions, not ordinary replacement burn backlog.

## Reason

Once every protected copy is lost or damaged, the system must rebuild from collection archives before more physical copies can be trusted.
