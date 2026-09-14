# state_schema.StateConnection.exec_driver_sql

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-exec-driver-sql:96bfa37f88 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc2562d591"></a>
- <a id="s-72c0d257ff"></a>`distribution`: `state-schema`
- <a id="s-3c207670e7"></a>`module`: `state_schema`
- <a id="s-b23bfc5f2b"></a>`name`: `exec_driver_sql`
- <a id="s-e2d8f218dd"></a>`owner`: `state_schema.StateConnection`
- <a id="s-03169c1aec"></a>`unit`: `member`

### Declared structure

- <a id="s-e55a5b1b50"></a>`kind`: `"method"`
- <a id="s-734f0339cb"></a>`signature`: `"\"(self, statement: 'str', parameters: 'Optional[_DBAPIAnyExecuteParams]' = None, execution_options: 'Optional[CoreExecuteOptionsParameter]' = None) -> 'CursorResult[Any]'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-4722cb6559"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.exec_driver_sql`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3464f3e3f839b44d0cc93473dbde3a511e3eace59d2fadcb82d731a4a1f81961 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, statement: 'str', parameters: 'Optional[_DBAPIAnyExecuteParams]' = None, execution_options: 'Optional[CoreExecuteOptionsParameter]' = None) -> 'CursorResult[Any]'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "exec_driver_sql",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
