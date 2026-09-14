# state_schema.StateConnection.begin

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-begin:08dbceb348 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-503bfc0405"></a>
- <a id="s-6144ba1215"></a>`distribution`: `state-schema`
- <a id="s-1355544396"></a>`module`: `state_schema`
- <a id="s-84c70c8a41"></a>`name`: `begin`
- <a id="s-552655f734"></a>`owner`: `state_schema.StateConnection`
- <a id="s-162eb3f617"></a>`unit`: `member`

### Declared structure

- <a id="s-3ddba20564"></a>`kind`: `"method"`
- <a id="s-ae438db0bd"></a>`signature`: `"\"(self) -> 'RootTransaction'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-6bb23dc13f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.begin`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a306591585a26547bf61933704d80bd1e9f5db8fa553e10750dd55210d2d6adf -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'RootTransaction'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "begin",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
