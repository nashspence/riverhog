# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `4e6c20a2b287f04d9a662724dfe96412e546b632217ca56d04741c81195e3896` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `6e847d588b2b1ab2259d196cabd817f7fc39bfde641fb3562424a1e4ed638aec` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `55dd597a92cd1aa7f29d2a4ba83eab8a965cb58e78cb06eb9ede6533f23d1d95` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `4aea2f5f59c7632452897bda5a4ba2e11f278d3cf4197ee56ee87036f81005fa` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `7e48d9fcb546fd138862f5573041d834b812e90f6ef446bf87ebedd5d7cb1ff3` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
