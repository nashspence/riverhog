# state_schema.StateConnection.get_transaction

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-get-transaction:b87fd1578e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-56454b310d"></a>
| Field | Shape |
|---|---|
| <a id="s-1b34c08a4c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-3412af6905"></a>`distribution` | "state-schema" |
| <a id="s-950a9166d7"></a>`module` | "state_schema" |
| <a id="s-3a20125ab2"></a>`name` | "get_transaction" |
| <a id="s-e7ff999ac3"></a>`owner` | "state_schema.StateConnection" |
| <a id="s-eef543a1aa"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-f59e87e51f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.get_transaction`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: edb313ac2ffc1e884deae01265038f21348b1eca0d29683d95ef1829e871c0ac -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Optional[RootTransaction]'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "get_transaction",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
