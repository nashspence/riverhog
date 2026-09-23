# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `61019a0fad7fa7a8fbbbc4596183e915c7c5c121a1b3137e2ad6ab2a2977aac1` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `f17b99d1e935171b5182d716d9b29dea79375565df26bfde32476ae0893df736` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `2bbaaf9fa4d3c4ae45f572f24bfbd7f05407045a4cc957032b9f9183799a4f03` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `526b11176d6fe52743488955970bc75d3c767d246d85463e1994ba2370b4e909` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `1b0e8e1abefd8cc5d8de3d30097e7ecdb426ec55b80bc7ec7bb5b8d1fb2684fb` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
