# state_schema.StateConnection.get_nested_transaction

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-get-nested-transaction:11c7f41305 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7d43aa3561"></a>
| Field | Shape |
|---|---|
| <a id="s-f809e52bc6"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-e3b485fbe7"></a>`distribution` | "state-schema" |
| <a id="s-5b000db0f4"></a>`module` | "state_schema" |
| <a id="s-90e8733678"></a>`name` | "get_nested_transaction" |
| <a id="s-9dd5c7872a"></a>`owner` | "state_schema.StateConnection" |
| <a id="s-a26f936940"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-313e7a0da7"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.get_nested_transaction`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2e2c6ffb57dfd3724e8cc6ccc034b3d7f1cab898a340ada7287b964a7193411a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Optional[NestedTransaction]'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "get_nested_transaction",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
