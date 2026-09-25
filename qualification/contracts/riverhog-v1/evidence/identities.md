# Snapshot identities

[Atlas](../index.md) · [Reference navigation](index.md)

These independent identities distinguish contract semantics, discovery coverage, source trace, and the generated human representation.

| Identity domain | SHA-256 |
|---|---|
| <a id="i-668a3da3b1"></a>`boundary_canonical_sha256` | `bec88b020598c09ae60e54a709ea00ea09c691f4ababbcfd09ebdc79ae40972c` |
| <a id="i-aef9e4a6d4"></a>`external_contract_sha256` | `04b420d1add15d83c2ef3de204c3931aea0b79d03437f6b725ba03b28be8f0d0` |
| <a id="i-95b76cebb4"></a>`semantic_contract_sha256` | `61aa1d2c6e607af3bb1df46714921b9e402d5ee9dd5fe43bb71d84acb879a631` |
| <a id="i-d374a59a6c"></a>`coverage_sha256` | `b9a3dc188af695d5e4cc666c688a511b11730b4ea4fc69ff0008c36d93851920` |
| <a id="i-b201ae62f3"></a>`trace_sha256` | `15963a0b5bd25dbc384f664bc898b4d28506d61562113137bb810c19f76c50f8` |

<a id="i-af23736723"></a>The byte-exact `atlas_representation_sha256` is recorded at `/identities/atlas_representation_sha256` in the [machine artifact (raw JSON)](../../riverhog-v1.json?raw=1). It cannot be embedded inside the document bytes that it identifies.
