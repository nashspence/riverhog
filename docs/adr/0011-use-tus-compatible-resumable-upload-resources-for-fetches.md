# ADR 0011: Use tus-compatible resumable upload resources for ingest and recovery

## Status

Accepted.

## Context

Riverhog now has two user-visible resumable upload workflows:

- collection ingest from `arc`
- fetch fulfillment from `arc-disc`

Both need direct streaming into the server without a bespoke byte-transport protocol. Some logical files may exceed
100 GB and fetch recovery may span multiple discs. Both workflows therefore need:

- reliable offset-based resume
- upload expiry for incomplete partial server-side state
- checksum verification
- good progress reporting

The transport should be commodity rather than bespoke when the protocol already exists and fits the problem.

## Decision

- each collection-upload file and each fetch-manifest entry uses one resumable upload resource
- the JSON API creates or resumes that resource for the caller and returns its upload URL
- the upload URL speaks tus-compatible resumable upload semantics
- the required protocol surface is tus core plus checksum and expiration support
- collection ingest streams logical file bytes into the upload resource for that file
- `arc-disc` streams raw encrypted payload-object bytes from optical media into that upload resource without owning
  decryption or final logical-file validation
- split logical files stream into the same upload resource in ascending part order
- upload-session offsets, lengths, and transport checksums are measured against the ordered recovery-byte stream, not the
  final logical-file plaintext length
- if an operator switches to a candidate copy whose advertised recovery-byte stream differs for the current span, the
  server may reject resume and require that span to restart at its boundary
- if temporary buffering is needed internally, it uses conventional temporary storage as an implementation detail only

## Consequences

- reliable resume does not require reimplementing upload offsets, expiry, and checksum negotiation
- collection-upload sessions and fetch manifests remain the domain contracts while tus handles byte transport
- the server still owns domain-specific binding of uploads to manifest entries, any required decryption, final logical
  hash validation, and pin-based cleanup
