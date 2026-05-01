# ADR-0032: Allow Recovery Sessions to Cover Multiple Images

## Decision

Riverhog recovery sessions may cover multiple finalized images when their rebuild work shares restored collection archives.

## Reason

The operator-facing recovery unit should match the cold-archive work being restored, not merely one image row at a time.
