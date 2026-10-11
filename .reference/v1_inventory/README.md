# Repo-wide growth inventory: integration reference

This is the reviewed proposal for #964, not a product registry or current growth
conformance result. Audited source: `152d095038df0c0d0ed642cf265d47b11c65a754`.
**Start with the [approved policy and pending design decisions](DECISIONS.md)**
(recorded in [#964 comment 6111028968](https://github.com/nashspence/riverhog/issues/964#issuecomment-6111028968)).
Then read [the inventory index](INVENTORY.md) and [the proposed structure](STRUCTURE.md).
[declaration_example.json](declaration_example.json) illustrates the proposed
cost-vector representation; it is not an implemented or accepted wire schema.

## Complete proposal data

The eight family files retain every reviewed scope, bound, invariant, witness,
owner and source anchor. [metadata.json](metadata.json) supplies notation and
common non-conformance qualifications; [sources.json](sources.json) supplies
source hashes. [distributions.json](distributions.json) maps all 86 distributions.
None of those mappings establishes dynamic method-level Closure coverage.

[load_inventory.py](load_inventory.py) reconstructs the entire original reviewed
inventory object, restores repeated URLs/default fields, and verifies its exact
canonical-object SHA-256. Run from any location with Python 3.11 or newer:

```bash
python /path/to/reference/.reference/v1_inventory/load_inventory.py
python /path/to/reference/.reference/v1_inventory/load_inventory.py --output /tmp/inventory.json
python -S /path/to/reference/.reference/v1_inventory/verify_inventory.py --repo /path/to/152d0950-checkout
python -m unittest discover -s /path/to/reference/.reference/v1_inventory -p test_inventory_package.py -v
```

The output file must not already exist. The verifier requires the exact inventory
source, which is newer than the retained runtime tree of the reference branch.
It reads source files but does not execute product code. Its checks do not prove
asymptotic bounds or complete public-operation applicability.

[verification.json](verification.json) is the original static scan/reference
report. The raw 203 literal-route and 341 export-site counts are scope aids, not
runtime discovery. Static verification was repeated for publication; Docker,
PostgreSQL, codec, end-to-end, locked gate and release tests were not run.

## Authority and design decisions

The maintainer approved the policy directions in [DECISIONS.md](DECISIONS.md):
narrow real indexed/sorted cost allowances (never mislabeled strict linear),
named maintained schema/query/predicate profiles, and qualified component/
codec/provider profiles. These directions permit ordinary declarations and
narrow semantics-preserving fixes to proceed.

**Two design submissions remain** before broad changes: G22 online graph
acceptance/validation preserving current rejection points, and G26 compact
shared-history discovery preserving query/scope parity and protected identity/
state consequences. The preferred directions are not approvals of algorithms,
schemas, migrations or final bounds. Issue #964 is the decision authority.

Original `STRUCTURE.md` and JSON family records remain unmodified as the
reviewed *proposal*; their statements that all four policy directions are
unapproved are historical and superseded by the later issue decision. The
canonical-object inventory digest is unchanged.

G01–G62 remain proposed public families; G63–G64 are internal engineering
budgets. Do not freeze sample sizes, specific SQL indexes, source locations or
measured seconds as product promises. Do not copy this reference taxonomy into
`main` as a competing contract registry. Full method/route discovery, final
operation-level bounds, proof and v1 acceptance remain necessary.
