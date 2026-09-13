# state_schema.StateConnection.info

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-info:a1e9a76ea2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-94e7e2e503"></a>
| Field | Shape |
|---|---|
| <a id="s-5a234f7981"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-39266c5091"></a>`distribution` | "state-schema" |
| <a id="s-468898a647"></a>`module` | "state_schema" |
| <a id="s-9fb7d017ca"></a>`name` | "info" |
| <a id="s-e4de20a948"></a>`owner` | "state_schema.StateConnection" |
| <a id="s-3ba45ab52e"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-92dd8c0c34"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.info`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bd0c177f4c8c2cd0e51d938eeb5db2e0cc79bad456e645f60ddf11530ce836bb -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> '_InfoType'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "info",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
