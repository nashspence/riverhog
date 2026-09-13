# stove0_target_protocol.TRANSFORM_TARGET_PROTOCOL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-protocol:stove0-target-protocol-transform-target-protocol:0bec3de987 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e037e32458"></a>
| Field | Shape |
|---|---|
| <a id="s-8fc534e500"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-38f12b40db"></a>`distribution` | "stove0-target-protocol" |
| <a id="s-94ed225c72"></a>`module` | "stove0_target_protocol" |
| <a id="s-9738a93a31"></a>`name` | "TRANSFORM_TARGET_PROTOCOL" |
| <a id="s-9d61bca7fb"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a8bddaa7ae"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-target-protocol:stove0_target_protocol](../../../evidence/sources.md#src-f4f0b22026) — `reference/stove0/packages/target-protocol/src/stove0_target_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_target_protocol.TRANSFORM_TARGET_PROTOCOL`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 09acfcf4c0d8948f5f95e20cd333a5faa290d84721f9d9041ac93ff6c17f59e8 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-transform-target/v1"
  },
  "distribution": "stove0-target-protocol",
  "module": "stove0_target_protocol",
  "name": "TRANSFORM_TARGET_PROTOCOL",
  "unit": "export"
}
```
