# Repo-wide growth inventory: integration reference

This is the reviewed proposal for #964, not a product registry or current growth
conformance result. Audited source: `152d095038df0c0d0ed642cf265d47b11c65a754`.
Read [the inventory index](INVENTORY.md) and [the proposed structure](STRUCTURE.md).
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

## Authority and remaining decisions

The issue adopts this as the working inventory and structure for owner-declaration
work. Final exact profiles/bounds remain subject to the stated decisions and
actual conformance evidence. In particular, keep G22 online graphs, G02/G28/G40
general evaluators, G26 shared-history representation, and indexed/ordered
exceptions explicit. Do not hide those costs in an unqualified linear label.

G01-G62 are proposed public families. G63-G64 are internal engineering budgets.
Do not freeze sample sizes, counters' implementation details, run times or source
locations as product promises. Do not copy this reference taxonomy into main.
The historical no-publication statement at the end of STRUCTURE.md describes the
original inventory task; this package is now published as an issue-owned reference.
