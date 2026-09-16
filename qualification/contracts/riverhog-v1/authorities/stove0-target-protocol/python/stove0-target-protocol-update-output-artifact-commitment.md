# stove0_target_protocol.update_output_artifact_commitment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-update-output-arti-7c264538d3:8a11aae3f9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e1d556b352"></a>
- <a id="s-32b8b48d24"></a>`distribution`: `stove0-target-protocol`
- <a id="s-73107d5a67"></a>`module`: `stove0_target_protocol`
- <a id="s-f3dd701acb"></a>`name`: `update_output_artifact_commitment`
- <a id="s-7b13adc417"></a>`unit`: `export`

### Declared structure

- <a id="s-3a1dea9b18"></a>`kind`: `"function"`
- <a id="s-902006cba1"></a>`signature`: `"\"(digest: 'Any', *, ordinal: 'int', artifact: 'OutputArtifact') -> 'None'\""`

## Governing policies

- <a id="pa-77699505ba"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.update_output_artifact_commitment`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 40712c00de82f583faae6297bc0916ac70197db61994d0ac907b5d584b2563d4 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(digest: 'Any', *, ordinal: 'int', artifact: 'OutputArtifact') -> 'None'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "update_output_artifact_commitment",
  "unit": "export"
}
```

</details>
