# Current decision entry point

Use [the compact decision directory](../integration/DECISIONS.md), then one
[work packet](../integration/README.md). GitHub owns decisions; the reference
only routes the integrator to them.

G02/G40, G63/G64 and the scoped G38/G39 changes are approved in #964 comment
6118314165. G26's Merkle refinement and G27/G28's revised discovery semantics
are approved in comment 6118482544. G22's bounded semantics-preserving design
has been supplied; its implementation/backend and proofs are not a missing
external design submission. Exact formulas/vectors and production conformance
remain open integration work, not presumed passes.

Do not use old “to supply next” instructions. The full preceding decision prose
is preserved byte-for-byte in Git history at
cac600aeced78973f8f2d94307fbd8adc7dec2e8:.reference/v1_inventory/DECISIONS.md.
No prior issue approvals or evidence were deleted or broadened.

[STRUCTURE.md](STRUCTURE.md) is the detailed declaration proposal;
[G02_G40_G63_G64_INTEGRATION.md](G02_G40_G63_G64_INTEGRATION.md) is the
profile/owner brief. [INVENTORY.md](INVENTORY.md) and JSON parts are research
coverage aids only. Use read.py --family Gnn rather than loading all entries.
