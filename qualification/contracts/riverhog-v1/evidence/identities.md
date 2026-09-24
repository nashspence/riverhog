# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `6b8b73de6614d65bf77d3f6fa67d911b1ad08729086ce03b2856473e1b129ca7` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `b7702169da95a5900c6e4815eedfe267cababb7f4eaed821d77d80a0379946de` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `a8014831b9417bc5e0e6d6384ddb42afcc1b9ab629385752bb74428c54518b31` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `412cafc4d2905375ff524fcc227ad682630acee07d04e6a2395f4347445eeb2a` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `4bd240123b5bc41c960673c14fac0fff5d9ed6bc57fde8902e5dd9422e90d641` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
