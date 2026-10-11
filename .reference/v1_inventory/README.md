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

[load_inventory.py](load_inventory.py) reconstructs the **current decision-
reconciled proposal**, restores repeated URLs/default fields, and verifies its
exact canonical-object SHA-256. The *previous* reviewed inventory and its digest
remain recoverable from the earlier #903 commit; this revision deliberately
changes G08/G26 policy text and the reference inventory digest. Run from any location with Python 3.11 or newer:

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

**Both G22 and G26 design submissions have been supplied** as source-pinned
reference work; G22's online acceptance model and G26's exact shared-history
query contract still require integrated proof. **For G26 specifically, the
compact derived-index identity revision and pre-v1 clean schema hard cut are
explicitly approved** in
[#964 comment 6112668478](https://github.com/nashspence/riverhog/issues/964#issuecomment-6112668478).
No obsolete pre-v1 schema migration/backward-compatibility route, rolling
replacement or old-cursor adapter
is permitted. Other individual formulas, final production schemas and growth
qualification remain unestablished. Issue #964 owns the decision.

The current `STRUCTURE.md` is a proposal with an approved G26 identity policy
overlay, and G08/G26 family records explicitly distinguish the forbidden legacy
bridge from intentional Alembic v1/bootstrap/future forward-only revision support.
The original pre-decision inventory and its SHA-256
`1a4da40cd1fc70490e68b7ec3187667cb4162f7a63487e46af439a7e67e4d62f`
remain in the parent reference commit. The new inventory digest is checked by
`load_inventory.py` and is not a runtime conformance claim.

G01–G62 remain proposed public families; G63–G64 are internal engineering
budgets. Do not freeze sample sizes, specific SQL indexes, source locations or
measured seconds as product promises. Do not copy this reference taxonomy into
`main` as a competing contract registry. Full method/route discovery, final
operation-level bounds, proof and v1 acceptance remain necessary.
