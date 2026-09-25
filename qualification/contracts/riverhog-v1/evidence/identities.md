# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `4e6c20a2b287f04d9a662724dfe96412e546b632217ca56d04741c81195e3896` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `dc6aba176ac178e23f9aba2a0a510d2c3a5e18b6869e6a687fcc8fd510763c73` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `4d9e9e17578a10e3ec68fb8b13bb61e66a25354f3b7eb9f2bb4566fbc7189c02` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `fd6ce4005cf4cf3f2157965fb1fe2d8a2d0d88472cbd93c0bd959cb304a3a42f` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `cbeeb94a9a86b15f8208bb901c60a1d8a9adf530534d61c84055313d50e67502` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
