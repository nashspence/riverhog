# ADR-0013: Upload Recovery by Logical File

## Decision

Riverhog tracks fetch upload progress per logical file, not per disc fragment.

## Reason

Recovery needs server-side verification against logical plaintext while allowing optical clients to stream raw recovery bytes.
