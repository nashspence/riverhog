# state_schema.StateEngine.driver

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateengine-driver:b18b9ead2d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8e4df1beb8"></a>
- <a id="s-3c22d2c91b"></a>`distribution`: `state-schema`
- <a id="s-45cf967b52"></a>`module`: `state_schema`
- <a id="s-5052eac14c"></a>`name`: `driver`
- <a id="s-d167794fc6"></a>`owner`: `state_schema.StateEngine`
- <a id="s-96b6b1d4ee"></a>`unit`: `member`

### Declared structure

- <a id="s-4c11bb6be4"></a>`kind`: `"property"`
- <a id="s-9e8605c0d7"></a>`signature`: `"\"(self) -> 'str'\""`

## Maintained corroboration

### Related interface records

- [StateEngine](state-schema-stateengine.md)

## Governing policies

- <a id="pa-176bf3daf8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateEngine.driver`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 66ecc1749f93260cfd79813c6e1d2f4867418b2c8b343ecd5d62ba5bd1b782db -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'str'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "driver",
  "owner": "state_schema.StateEngine",
  "unit": "member"
}
```
