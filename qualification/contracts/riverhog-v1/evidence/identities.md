# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `5af7171d406deec927ce70092a60eb033f51c53f2da16b7f1cea794e03e807be` |
| <a id="i-adaf527633"></a>`boundary_legacy_sha256` | `5af7171d406deec927ce70092a60eb033f51c53f2da16b7f1cea794e03e807be` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `00c7c3d1e496c9303835c0c41f808d40c054c94395e504ae8ffd128ec976f2f6` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `f0b11453e667325e323c842d514627d0075ac126f06fe3a26884c01d5d2ab9a0` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `8d7ce9eda63ae193a317ecae52be69614f2b913d65f18ebe4fe717d69dddb2da` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `4015f3efc1c8dbe07916e952b9e1180841e698535b8dfc468831835c4f727a49` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact](../../riverhog-v1.json). It cannot be embedded inside the document bytes that it identifies.
