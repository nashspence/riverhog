# ADR-0028: Separate Byte-Complete from Uploaded Fetch State

## Decision

Riverhog distinguishes fully received recovery bytes from verified, materialized fetch-entry uploads.

## Reason

Receiving the expected byte count is not the same as proving the logical file is valid and materialized.
