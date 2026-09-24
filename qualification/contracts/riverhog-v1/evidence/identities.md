# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `f66c412535996ed99e9d1d493842d83ca908e04dec9a891b122d8b4d2f81ec98` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `071ab1f39c7fa415304d3e6604c5fa3a8358af670b804c8a746b528323fef58b` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `4fcd58527fec2b6c17f5b56c551c5532e8a766e1ff17c0768293bb2ab093aca2` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `cd8e90fcf94fc38eaa96bab0fbd51698c93f60f0292f39ecb7057987dda600ca` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `ca39cbfe19af53bd3673401a56944a37e2052efbd132caf7f9a239a26cefd491` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
