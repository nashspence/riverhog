# state_schema.StateEngine.raw_connection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateengine-raw-connection:ec91e8d7af -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e20489a00e"></a>
| Field | Shape |
|---|---|
| <a id="s-22424fd4d5"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-8b8446794a"></a>`distribution` | "state-schema" |
| <a id="s-852ea64808"></a>`module` | "state_schema" |
| <a id="s-6c667d7ad7"></a>`name` | "raw_connection" |
| <a id="s-513f459a34"></a>`owner` | "state_schema.StateEngine" |
| <a id="s-6a9050479e"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [state_schema.StateEngine](state-schema-stateengine.md)

## Governing policies

- <a id="pa-0fef352836"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateEngine.raw_connection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3191f796241111a5d92e4044a7ec9140bb0cef6a973fef7bc9bcd217abb4c896 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'PoolProxiedConnection'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "raw_connection",
  "owner": "state_schema.StateEngine",
  "unit": "member"
}
```
