# stove0_operator_contracts.stove0_event

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-operator-contracts:stove0-operator-contracts-stove0-event:09fb3c0f65 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-operator-contracts](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-949ed954c8"></a>
| Field | Shape |
|---|---|
| <a id="s-10e033a1a0"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-aa7529359f"></a>`distribution` | "stove0-operator-contracts" |
| <a id="s-c4167ddcbe"></a>`module` | "stove0_operator_contracts" |
| <a id="s-ac6fd6a2e0"></a>`name` | "stove0_event" |
| <a id="s-01a4d22fec"></a>`unit` | "export" |

## Governing policies

- <a id="pa-c639f0be7c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-operator-contracts:stove0_operator_contracts](../../../evidence/sources.md#src-51ad84528d) — `reference/stove0/packages/operator-contracts/src/stove0_operator_contracts/__init__.py`

### Machine authority

- `/external_contract/python/stove0_operator_contracts.stove0_event`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d2bdd480cb5bd9914490844c8f5c14364b00aa95deb14d10a99b3325fa3d6fe7 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, type: 'Stove0EventType', subject: 'str', data: 'Mapping[str, Any]') -> 'Stove0LifecycleEvent'\""
  },
  "distribution": "stove0-operator-contracts",
  "module": "stove0_operator_contracts",
  "name": "stove0_event",
  "unit": "export"
}
```
