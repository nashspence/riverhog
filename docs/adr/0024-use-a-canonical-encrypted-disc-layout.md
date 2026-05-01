# ADR-0024: Use a Canonical Encrypted Disc Layout

## Decision

Riverhog ISOs use one canonical root layout with only `README.md` in plaintext and all other leaf files encrypted.

## Reason

Optical media must be automatable, human-inspectable at the safe boundary, and privacy-preserving at the file-name boundary.
