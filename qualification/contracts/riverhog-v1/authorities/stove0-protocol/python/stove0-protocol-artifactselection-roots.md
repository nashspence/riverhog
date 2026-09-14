# stove0_protocol.ArtifactSelection.roots

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-artifactselection-roots:f57314954b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-48db7397d2"></a>
- <a id="s-4d6400bb26"></a>`distribution`: `stove0-protocol`
- <a id="s-34ad09c61a"></a>`module`: `stove0_protocol`
- <a id="s-03333e0438"></a>`name`: `roots`
- <a id="s-ce6540067c"></a>`owner`: `stove0_protocol.ArtifactSelection`
- <a id="s-7e2c65456f"></a>`unit`: `member`

### Declared structure

- <a id="s-4078b39add"></a>`kind`: `"method"`
- <a id="s-3c674eb0ab"></a>`signature`: `"\"(self) -> 'tuple[CollectionRootRef, ...]'\""`

## Maintained corroboration

### Related interface records

- [stove0_protocol.ArtifactSelection](stove0-protocol-artifactselection.md)

## Governing policies

- <a id="pa-159860805e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.ArtifactSelection.roots`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 436b825cf25e63bfa1d757e9f62d1bb5fdcf35598d2b9370cc2da07df1a91d7a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'tuple[CollectionRootRef, ...]'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "roots",
  "owner": "stove0_protocol.ArtifactSelection",
  "unit": "member"
}
```
