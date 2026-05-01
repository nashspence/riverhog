# ADR-0015: Expose WebDAV as Read-Only Browsing

## Decision

Riverhog exposes committed hot files through a read-only WebDAV sidecar.

## Reason

Operators need a familiar browsing surface without giving WebDAV authority over archive state or staging paths.
