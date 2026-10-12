# Final handoff cleanup — checks actually run

Parent reference: cac600aeced78973f8f2d94307fbd8adc7dec2e8.
Audited source: 152d095038df0c0d0ed642cf265d47b11c65a754.
Only external reference navigation, approval routing and recommendation metadata
are changed; production code and diagnostic algorithms are not changed.

## Completed local checks

* 13 navigation regression tests passed with warnings as errors. They cover one-
  packet/family reads, bounded output, exact pagination, headings, bad arguments,
  path escape, Unicode, coverage and valid dependencies.
* The new reading map has 20 cards and routes all 64 existing G families exactly
  once. Its 34 predecessor edges are acyclic. Long excerpts have a maximum of
  12,000 characters with explicit continuation; cards are at most 289 words.
* 10 unchanged inventory integrity/policy tests passed. The unchanged source
  verifier checked 64 families, 86 projects and 182 anchors against the exact
  separate audited checkout. This is not dynamic operation coverage.
* Local navigation Markdown paths and anchors resolve. All proposed family costs,
  owners, scopes and witnesses are unchanged. Only recommendation routing and three
  superseded approval-status sentences changed; functional invariants are retained.
  This cleanup supplies no new approval.
* All 47 tracked historical Python/patch/evidence/counter/source-verification files
  selected by the preservation check are byte-identical to the parent. Historical
  test totals retain their original source identity and were not rerun or summed.
* git diff --check passed. Final tree and remote publication identity are recorded
  in the issue after publication; no local timestamp substitutes for an exact SHA.

Commands (REF is the reference checkout; AUDITED is separate):

```sh
python "$REF/.reference/integration/read.py" --check
python -W error -m unittest discover -s "$REF/.reference/integration" -p test_read.py -q
python -m unittest discover -s "$REF/.reference/v1_inventory" -p test_inventory_package.py -q
python -S "$REF/.reference/v1_inventory/verify_inventory.py" --repo "$AUDITED"
git -C "$REF" diff --check
```

Current canonical research-inventory SHA-256:
2f00f44312dd19585bcf8b4c1312923e23516e74cdcd71f158a282fa39ddca2b.
The changed digest covers recommendation/purpose/approval-status routing only, not new product
promises or a replacement operation registry.

## Not performed or claimed

No native product, historical algorithm-model, full locked lint/unit/build/dist,
HTTP/authority, PostgreSQL, process-kill/power-loss, image/recovery/security/scale,
CI/CodeQL or timing qualification was run for this cleanup. No actual operation
has acquired a conformance pass. The reader's character/card limits protect this
reference reading path only; they are not a model context-capacity guarantee or
Riverhog product extent limit. Packet status must be tracked in GitHub.
