# Growth inventory — query one family

Start at [HANDOFF](../HANDOFF.md) and the [decision directory](../integration/DECISIONS.md).
This is an external #903 scope proposal, not a runtime registry or conformance pass.
All individual work/memory/formula fields remain proposals until their actual owners
establish executable declarations and qualifying evidence.

```sh
python .reference/integration/read.py --family G26
python .reference/integration/read.py --list
```

[INVENTORY.md](INVENTORY.md) lists the existing families. The eight JSON parts,
metadata.json, sources.json and distributions.json retain the full scope, invariants,
source anchors and common limitations. load_inventory.py reconstructs the canonical
proposal and checks its digest. verify_inventory.py checks the separately pinned
source, not runtime growth. test_inventory_package.py protects this representation.

The final cleanup updates only recommendation and approval-status routing text and its derived
canonical digest; no proposed bounds, source fingerprints, family ownership or
historical execution claims are changed. Current architecture approvals are in
[DECISIONS.md](DECISIONS.md); a recommendation string is not issue authority.

For integrity checks, use the separate audited-source checkout:

```sh
python .reference/v1_inventory/load_inventory.py
python -S .reference/v1_inventory/verify_inventory.py --repo "$AUDITED"
python -m unittest discover -s .reference/v1_inventory -p test_inventory_package.py -q
```

The source checker deliberately rejects any SHA other than
152d095038df0c0d0ed642cf265d47b11c65a754. Reconcile new source rather than relabel
this evidence. The branch's runtime base is older; do not mistake it for AUDITED.
G01–G62 route public families; G63/G64 are internal budgets. The 64-family,
86-distribution, 182-anchor inventory is not method-level dynamic Closure coverage.
Keep workload/target authority in #963 and exact operation declarations at their
owners; no mainline policy ledger or wholesale copy of this inventory is intended.
