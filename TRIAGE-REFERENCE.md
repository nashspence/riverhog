# NON-AUTHORITATIVE TRIAGE REFERENCE

Issue: #870
Base: `e0413a288fe3e817db16d5e887b08622a24b3c94`
Convention: #903

This branch preserves the contained private catalog-sync traversal experiment that
temporarily landed on `main` as commits `d1184252fb97df66b74aef57e42cc8d6d306f4d0`
and `e0413a288fe3e817db16d5e887b08622a24b3c94`.

The experiment consists of:

- `packages/riverhog-client/src/riverhog_client/_catalog_sync.py`
- `packages/riverhog-client/tests/test_catalog_sync_engine.py`

It is reference material for triage and possible later integration only. It is not an
accepted public or private abstraction, supported contract, release requirement, or
authorization to merge these changes.

The owning issue and subsequent maintainer decisions determine whether any part of this
experiment should be integrated. Revalidate it against then-current `main` and accepted
decisions rather than applying it mechanically.

The exact commit linked from #870 is the handoff identity. This branch name is navigation only.
