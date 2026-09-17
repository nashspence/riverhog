# state_schema.StateConnection.begin_nested

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-begin-nested:9d5e1abb17 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d0f5866097"></a>
- <a id="s-c2296a3fbe"></a>`distribution`: `state-schema`
- <a id="s-f12e6a236e"></a>`module`: `state_schema`
- <a id="s-349474d8b1"></a>`name`: `begin_nested`
- <a id="s-150d32a19c"></a>`owner`: `state_schema.StateConnection`
- <a id="s-49db5738db"></a>`unit`: `member`

### Declared structure

- <a id="s-ae4bf6622c"></a>`kind`: `"method"`
- <a id="s-213ad737f2"></a>`signature`: `"\"(self) -> 'NestedTransaction'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-00a214bc57"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources/authorities.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateConnection.begin_nested`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4de7d13ef1310de6cf2849e2c348a15c7eb57a6b5de4d3e9a740287bea0f6cc6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'NestedTransaction'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "begin_nested",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```

</details>
