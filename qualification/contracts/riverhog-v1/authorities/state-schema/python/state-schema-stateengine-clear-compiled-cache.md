# state_schema.StateEngine.clear_compiled_cache

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateengine-clear-compiled-cache:651e3e2d43 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b65ba2feea"></a>
- <a id="s-370c41dfda"></a>`distribution`: `state-schema`
- <a id="s-c64b4c0d2c"></a>`module`: `state_schema`
- <a id="s-7e69b0e8e3"></a>`name`: `clear_compiled_cache`
- <a id="s-e2b46bed0a"></a>`owner`: `state_schema.StateEngine`
- <a id="s-1ab4ce307e"></a>`unit`: `member`

### Declared structure

- <a id="s-99c1931524"></a>`kind`: `"method"`
- <a id="s-879662469a"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [state_schema.StateEngine](state-schema-stateengine.md)

## Governing policies

- <a id="pa-ac2e18a6ff"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateEngine.clear_compiled_cache`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f84d2b327ca986121132c3dfcf59b60c5f8b8540e088428a5bc0188d0871ea99 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "clear_compiled_cache",
  "owner": "state_schema.StateEngine",
  "unit": "member"
}
```
