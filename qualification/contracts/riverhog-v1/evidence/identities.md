# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `b54dad8f2f96c081905194d9ba3ceb40c1a4ea7af6a14ce4bfc954da2b979a0e` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `f60b8c6ece15fe136ce6f97aa347602a8a0372e524ec8f02acc6fcbe3053fef6` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `c9268032540fef94ee7bcf5baef8d4e05e58553714af4f56c4f6a5e6c4cf7393` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `acfb3f6cd51a38f5c74f7e2d821793b3f9ba6ad8cf578ccdac9eca4de50404b0` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `606f08b8d944682b40dd0503374c9acc01506ffe29c1c257d991f7401560ea59` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
