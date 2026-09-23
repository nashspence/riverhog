# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `0396337919f08baf5393cf88ef247301f1b1ea3b7db3224c3eb3fded0e3b3cec` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `9654fe7b597143979bcee00c880f06b9b6523b0102f1e7c1618f121d627ec567` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `2cfee3edbf4a6776e30033b87961cbecfafa23e3623d04d614eb5a1cdc3e4b69` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `d4abcfb32929464626252ef9194cc3aa1d6db755356c6e8232c4f57504f136a1` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `67e538f4098576be749ad689b7bb96e2214b0cd7c210f21e68fade3b28375daa` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
