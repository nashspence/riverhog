# ADR 0021: Define archive protection from two physical copies and Glacier state

## Status

Accepted.

## Context

Finalized images and collections need a strong protection/compliance model.

The opinionated archival workflow depends on answering:

- how many physical copies are still required
- whether current physical copy registration is enough on its own
- what Glacier state exists for each finalized image
- whether one finalized image or one collection is fully protected yet

## Decision

- every finalized image carries a required physical-copy count, defaulting to `2`
- every finalized image carries explicit Glacier archive metadata:
  `state`, `object_path`, `stored_bytes`, `backend`, `storage_class`,
  `last_uploaded_at`, `last_verified_at`, and `failure`
- finalized-image summaries expose:
  `protection_state`, `physical_copies_required`, `physical_copies_registered`,
  `physical_copies_missing`, and `glacier`
- collection summaries expose:
  `protection_state`, `protected_bytes`, and `image_coverage`
- `protection_state` uses three values:
  `unprotected`, `partially_protected`, and `protected`
- a finalized image is `protected` only when:
  - at least the required number of physical copies are registered or verified, and
  - Glacier state is `uploaded`
- a collection is `protected` only when every logical file is covered by protected finalized
  images
- collection coverage lists the finalized images currently covering that collection together
  with their registered copies and Glacier state

## Consequences

- registering one or even two physical copies can improve coverage while still leaving a
  finalized image or collection only `partially_protected`
- a stable protection-state model
