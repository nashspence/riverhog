# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `45602305c354e10489f7aaaea8f420623b0bc7e6d230fb332b791b9194dd60e8` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `b8b77d4bc2fe184c90c65dc203112075d290983ad8c74999db671e2c357ac4e4` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `53e60eae937e8b6faa5dc3cc64865b1516318cfc016157683cf9b8569d067d45` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `9ba017e58c2dc4264c709d417bfed3866715fdca89a5a6a6e1a94e2d6c558c14` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `28bcf83d6684a814a5610f212e5a8bbfca0acb8f54094d68d0bfb92a85839024` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
