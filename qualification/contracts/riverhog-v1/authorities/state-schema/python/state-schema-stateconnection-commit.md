# state_schema.StateConnection.commit

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-commit:9ff01e8326 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-60e2906fbe"></a>
| Field | Shape |
|---|---|
| <a id="s-5d25ee1521"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-8af14d32c5"></a>`distribution` | "state-schema" |
| <a id="s-35be6b5f1f"></a>`module` | "state_schema" |
| <a id="s-140c761b45"></a>`name` | "commit" |
| <a id="s-6751eb619e"></a>`owner` | "state_schema.StateConnection" |
| <a id="s-8b3f121d5d"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-3529b85883"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.commit`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f3f4cf63a64181c6ebe3584bfb9a4ef24888d2d6923ddd34c58b9fb6c8a307dd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "commit",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
