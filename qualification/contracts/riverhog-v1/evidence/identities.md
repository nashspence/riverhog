# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `f66c412535996ed99e9d1d493842d83ca908e04dec9a891b122d8b4d2f81ec98` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `761a772c8a7c24e405358b6abfd69d3c370b1bbe984fc3b6a05b0b5806bddf77` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `7f1dfbb8c90fefeedab65f4a7edb5062a4c2c1653d6ef5e8022bc963d48db9d4` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `a62ab99c4ab64e4d7449d8505e56264db48b8a5368a9c36000b8d1fce30d45ed` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `c158f4deba258500f7f7ce693c2d796b06947aacd813a49198ea8388c60be1e8` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
