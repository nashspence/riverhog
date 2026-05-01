# ADR-0035: Enforce Least-Privilege Storage Credentials

## Decision

Riverhog uses storage credentials that cannot cross the hot/staging and archive authority boundaries.

## Reason

A credential intended for one storage role should not be able to mutate or inspect the other role's objects.
