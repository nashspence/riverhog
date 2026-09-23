# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `b54dad8f2f96c081905194d9ba3ceb40c1a4ea7af6a14ce4bfc954da2b979a0e` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `8bfa9c61d3803ed41b1d2fcee83af9cd9577762ed62df5975a8251bad4c1e32d` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `096bb777ea78e64b9dc86cf2fdd31076f87be90011d3c6d37f4df1b41c954ed7` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `68b073b10338124f5a649f37c259128dd392acf2d2136664637cfb6306c36ff7` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `796c43b29288807a41bb599bd22eac1fd6e2ecba7ec70d06c15ea7f973cffdd5` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
