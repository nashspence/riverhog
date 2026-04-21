# ADR 0008: Derive canonical collection ids and keep search minimal

## Status

Accepted.

## Context

The MVP needs deterministic collection identifiers, a minimal search model that supports target-based actions, and a
hot namespace that can scale beyond one flat directory of collection roots.

ADR 0001 already makes the hot tree a projection from metadata rather than the source of truth. That means collection
ids can safely carry slash-delimited structure into the projected namespace as long as the namespace stays unambiguous.

## Decision

- the collection id is the canonical relative path beneath the staging root for the directory passed to `close`
- collection ids may be slash-delimited relative paths such as `tax/2022`
- collection ids must be canonical: reject empty segments, `.` segments, `..` segments, repeated `/`, and equivalent
  non-canonical spellings
- the collection-id namespace is prefix-free on path segments:
  - if `tax` exists, reject `tax/2022`
  - if `tax/2022` exists, reject `tax`
  - more generally, no collection id may be an ancestor or descendant of another collection id
- re-closing the same path fails with `conflict`
- search is case-insensitive substring match over collection id and full logical file path
- `archived_bytes` means bytes covered by at least one registered copy
- after `close`, the whole collection is hot even if no pin exists yet

## Consequences

- fixture-driven acceptance tests can assert predictable ids for both flat and nested collections
- the projected hot layout can scale as a normal nested tree instead of an ever-expanding flat directory
- collection creation must validate against the full existing catalog, not just the candidate id in isolation
- API and CLI collection lookups must treat slash-bearing ids as first-class and avoid ambiguous routing or encoding
  behavior
- search remains simple and directly actionable
- byte coverage values have stable meaning
