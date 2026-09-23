# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `61019a0fad7fa7a8fbbbc4596183e915c7c5c121a1b3137e2ad6ab2a2977aac1` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `7b4daeffd7fff96d8b43cfc8736ebfa2f47aa27d062bb3937568aa4244fc7d51` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `10d6370181a911958e6a07bea3eb3de7257ae9ec363975081beba8f49fa5bbe0` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `04c5c0854058f9eb12c2ee43db69d01f1a82713db3b8a8a01071cdf95f607aa2` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `0ac1a74eab9a9b62736b55244500608149cbd7a4e4582a8f096b22461621a690` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
