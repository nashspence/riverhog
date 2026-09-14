# state_schema.StateConnection.rollback

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-rollback:05f5f32f5e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d92c5bc45"></a>
- <a id="s-c7cc16d8cb"></a>`distribution`: `state-schema`
- <a id="s-0d0fc68111"></a>`module`: `state_schema`
- <a id="s-cc79d2962b"></a>`name`: `rollback`
- <a id="s-f41fd169da"></a>`owner`: `state_schema.StateConnection`
- <a id="s-f61e025057"></a>`unit`: `member`

### Declared structure

- <a id="s-e307bc3d1c"></a>`kind`: `"method"`
- <a id="s-38ba14a736"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-c02e584a45"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.rollback`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 96d609c481c72895e8d3c8799c608ee81bea86e99991f10a3d7277653f096dec -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "rollback",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
