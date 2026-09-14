# state_schema.StateConnection.execution_options

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-execution-options:571460fae3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cc09006c3e"></a>
- <a id="s-f4abadc8c8"></a>`distribution`: `state-schema`
- <a id="s-eb3af14d4d"></a>`module`: `state_schema`
- <a id="s-3122f4faaa"></a>`name`: `execution_options`
- <a id="s-36d56bb18e"></a>`owner`: `state_schema.StateConnection`
- <a id="s-e311d48d6e"></a>`unit`: `member`

### Declared structure

- <a id="s-3632cc918f"></a>`kind`: `"method"`
- <a id="s-02aea1bb64"></a>`signature`: `"\"(self, **opt: 'Any') -> 'Connection'\""`

## Maintained corroboration

### Related interface records

- [state_schema.StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-79911e6095"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:state-schema:state_schema](../../../evidence/sources.md#src-57d87d192c) — `packages/state-schema/src/state_schema/__init__.py`

### Machine authority

- `/external_contract/python/state_schema.StateConnection.execution_options`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 767947452e76fbc1b48b28d981306af96bef7d57a2ac4622211e4d0ab1032f73 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, **opt: 'Any') -> 'Connection'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "execution_options",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```
