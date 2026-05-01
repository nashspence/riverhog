# ADR-0005: Use tusd for Resumable Uploads

## Decision

Riverhog delegates resumable byte transport to `tusd` while retaining ownership of archive state.

## Reason

The system needs resumable upload mechanics without making the upload transport authoritative.
