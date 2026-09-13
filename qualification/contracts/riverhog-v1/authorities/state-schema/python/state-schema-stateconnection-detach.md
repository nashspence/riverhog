# state_schema.StateConnection.detach

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-detach:f946c97f9d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-63e7a25918"></a>
| Field | Shape |
|---|---|
| <a id="s-8539088231"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-b52b71f86e"></a>`distribution` | "state-schema" |
| <a id="s-6859e4d08f"></a>`module` | "state_schema" |
| <a id="s-58b787ffe0"></a>`name` | "detach" |
| <a id="s-79457e4d92"></a>`owner` | "state_schema.StateConnection" |
| <a id="s-4619b99563"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-0da3608d9a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.detach`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1438b25b0b4a9fa9ff5b3a8b0d6df3a37ee4fa89c6d96882d6163e640d44ca20 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "detach",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
