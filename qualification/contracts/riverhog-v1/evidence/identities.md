# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `b54dad8f2f96c081905194d9ba3ceb40c1a4ea7af6a14ce4bfc954da2b979a0e` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `abcb41700d079624e8eff4c79498b99b3c07385515d57e618755090ee81e9e07` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `2bd64d2ffa38f82da659b4556248b21d46e5fed48fb1448f7e19bb39ba073e61` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `726cf9d55c1b148cc9f8edb8c4e056ad55788ddf09a87150549504bd8775b133` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `608e858c356d2d48b72e502cf43aaa18b00ac845c1ebcb1a5ac7000a42f805fb` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
