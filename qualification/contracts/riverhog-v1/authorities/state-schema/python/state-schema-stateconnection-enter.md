# state_schema.StateConnection.__enter__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-enter:04f94f3bfe -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e2685a7fae"></a>
| Field | Shape |
|---|---|
| <a id="s-c433114f64"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-9a6b076dc2"></a>`distribution` | "state-schema" |
| <a id="s-1613a93517"></a>`module` | "state_schema" |
| <a id="s-580619a28d"></a>`name` | "__enter__" |
| <a id="s-840539b5eb"></a>`owner` | "state_schema.StateConnection" |
| <a id="s-1274df86f8"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-7a58b4e8b1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.__enter__`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bcaf73c947dd138016834067e5e19a082a27f0bed3a86206be171b2c35066324 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Connection'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "__enter__",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
