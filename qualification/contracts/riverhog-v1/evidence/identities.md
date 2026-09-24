# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `4e86a1b0525dfd08e68edd45b7e967645bc1c39b3db0297612498b02ade9959c` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `fc93888503d7580c115f41f6305b57aa70e431fec71dbc473ff1f70e88b8e362` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `040daad2036813ee76497912240584534ea5be0b2f51ec00ccc69bff7e31e529` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `c15fbc37d8c36e7ea1156bcfb221e0331bdbf5ce534583ca1148ec05a6e8121f` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `1ab7e8ae1e99a3c8f00a25348eca55ba0264558ace88c63b66d8fef8abf2a1bb` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
