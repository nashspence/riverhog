# state_schema.StateConnection.get_execution_options

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-get-execution-options:507b34e211 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-58e126b18b"></a>
- <a id="s-1734673723"></a>`distribution`: `state-schema`
- <a id="s-6ea90b9175"></a>`module`: `state_schema`
- <a id="s-2627bb8e43"></a>`name`: `get_execution_options`
- <a id="s-0de58efd1e"></a>`owner`: `state_schema.StateConnection`
- <a id="s-651ebce45d"></a>`unit`: `member`

### Declared structure

- <a id="s-241f33c8b0"></a>`kind`: `"method"`
- <a id="s-6f71f1e217"></a>`signature`: `"\"(self) -> '_ExecuteOptions'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-fd47d6e8f0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources/authorities.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateConnection.get_execution_options`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d51630267204ce7f0dd24bb1da90b297f18e28ca894c7f85f8f9f63d8c910de -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> '_ExecuteOptions'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "get_execution_options",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```

</details>
