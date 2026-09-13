# stove0_observer_client.SEMANTIC_VALIDATOR_ENTRY_POINT_GROUP

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-client:stove0-observer-client-semantic-validator-6e43d97c48:6a091a862e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3c7de7c7eb"></a>
| Field | Shape |
|---|---|
| <a id="s-082367d6f4"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-639c785c02"></a>`distribution` | "stove0-observer-client" |
| <a id="s-5a874ada50"></a>`module` | "stove0_observer_client" |
| <a id="s-e60c43ec74"></a>`name` | "SEMANTIC_VALIDATOR_ENTRY_POINT_GROUP" |
| <a id="s-e96f5314cc"></a>`unit` | "export" |

## Governing policies

- <a id="pa-84f3f02edf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-client:stove0_observer_client](../../../evidence/sources.md#src-67dbe161ba) — `reference/stove0/packages/observer-client/src/stove0_observer_client/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_client.SEMANTIC_VALIDATOR_ENTRY_POINT_GROUP`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8b50f7bdb0f7363f622ceeba262b3fc6940e1f4d7c46a57d22afee88b938e8cf -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.observer-semantic-validators"
  },
  "distribution": "stove0-observer-client",
  "module": "stove0_observer_client",
  "name": "SEMANTIC_VALIDATOR_ENTRY_POINT_GROUP",
  "unit": "export"
}
```
