# state_schema.StateConnection.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-close:19b1ac10d4 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-457701a7cc"></a>
| Field | Shape |
|---|---|
| <a id="s-07067cee15"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-52d67cd8ba"></a>`distribution` | "state-schema" |
| <a id="s-be12ce1e3e"></a>`module` | "state_schema" |
| <a id="s-cc253067b0"></a>`name` | "close" |
| <a id="s-cdbb386d16"></a>`owner` | "state_schema.StateConnection" |
| <a id="s-1c48ffee33"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-06593c0cbc"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.close`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8035c80fc429223d741f4f20b28fbfe3f1169185db6ea21c945dec9f934431d9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "close",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
