# stove0_target_protocol.update_output_source_edge_commitment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-update-output-sour-bd1482176d:8d05d6163c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ff0e0a0055"></a>
- <a id="s-ba44894dd6"></a>`distribution`: `stove0-target-protocol`
- <a id="s-0712c53cd3"></a>`module`: `stove0_target_protocol`
- <a id="s-b31ad64292"></a>`name`: `update_output_source_edge_commitment`
- <a id="s-f36db3455c"></a>`unit`: `export`

### Declared structure

- <a id="s-cb419e2bc5"></a>`kind`: `"function"`
- <a id="s-53caf23749"></a>`signature`: `"\"(digest: 'Any', *, ordinal: 'int', edge: 'OutputSourceEdge') -> 'None'\""`

## Governing policies

- <a id="pa-433de0681b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.update_output_source_edge_commitment`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b35b7484f376cf95e6e11df6cea22f56656ab31f48e9d38399f2aa6bc173480 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(digest: 'Any', *, ordinal: 'int', edge: 'OutputSourceEdge') -> 'None'\""
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "update_output_source_edge_commitment",
  "unit": "export"
}
```
