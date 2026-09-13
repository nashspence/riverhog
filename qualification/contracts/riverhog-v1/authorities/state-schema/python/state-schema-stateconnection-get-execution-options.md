# state_schema.StateConnection.get_execution_options

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-get-execution-options:507b34e211 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-58e126b18b"></a>
| Field | Shape |
|---|---|
| <a id="s-d75473fdea"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-1734673723"></a>`distribution` | "state-schema" |
| <a id="s-6ea90b9175"></a>`module` | "state_schema" |
| <a id="s-2627bb8e43"></a>`name` | "get_execution_options" |
| <a id="s-0de58efd1e"></a>`owner` | "state_schema.StateConnection" |
| <a id="s-651ebce45d"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-fd47d6e8f0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.get_execution_options`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d51630267204ce7f0dd24bb1da90b297f18e28ca894c7f85f8f9f63d8c910de -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> '_ExecuteOptions'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "get_execution_options",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
