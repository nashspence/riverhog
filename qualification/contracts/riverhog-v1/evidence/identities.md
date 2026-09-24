# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `45602305c354e10489f7aaaea8f420623b0bc7e6d230fb332b791b9194dd60e8` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `dc1c61373bdbbee0ffc63e7513c299b6de876d0f23f5f1fd338ec003a05a2266` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `10c3a305e966b9b14371d6b3185a0dee88434fbebb1666dbb8da9c60e18e338d` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `8119eea4c616501c48635e6fbe55c0cc9eabb625e186d0f5a8f606921188ccd7` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `00bb8288eae54d1a6bb2890c1ef79f9b7f712139036f921442b7e02c5d745d52` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
