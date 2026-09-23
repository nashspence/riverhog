# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `2b64d18954c3e797122d4b7bb8fbd3b65312d76b7be25fb49f4ab88179225aa8` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `6af6ec6863dcd206d239e9c3e237af75a4d64eb72502f60d55c6a9e797854a8d` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `c50b56b83f8d697bceaee4c5b6eddd3dbc6c8c41fc18198a4607929485a3a0ac` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `4e0588590d677e24b19a438fba75283b93eea146355255326223b6faff9f4c13` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `77635401bf8553e6939f3e017a18891b2d2e8ebe64bf58a0d50b879068377d3d` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
