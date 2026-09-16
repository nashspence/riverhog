# stove0_protocol.update_artifact_selection_commitment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-protocol:stove0-protocol-update-artifact-selection-commitment:f9ef19bf1a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dd8fdea2e1"></a>
- <a id="s-2cf26fad0f"></a>`distribution`: `stove0-protocol`
- <a id="s-0e8ba10d45"></a>`module`: `stove0_protocol`
- <a id="s-7fabaa12fb"></a>`name`: `update_artifact_selection_commitment`
- <a id="s-60ef656baa"></a>`unit`: `export`

### Declared structure

- <a id="s-216c485c34"></a>`kind`: `"function"`
- <a id="s-a3600bc557"></a>`signature`: `"\"(digest: 'Any', *, ordinal: 'int', artifact: 'ArtifactSubject') -> 'None'\""`

## Governing policies

- <a id="pa-ac9fe5c653"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-protocol:stove0_protocol](../../../evidence/sources.md#src-084138045e) — `reference/stove0/packages/protocol/src/stove0_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_protocol.update_artifact_selection_commitment`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d4fb24cfbe06582b3a5c34cf37129e16e13d25c82339661e62bb33245c8aeecf -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(digest: 'Any', *, ordinal: 'int', artifact: 'ArtifactSubject') -> 'None'\""
  },
  "distribution": "stove0-protocol",
  "module": "stove0_protocol",
  "name": "update_artifact_selection_commitment",
  "unit": "export"
}
```

</details>
