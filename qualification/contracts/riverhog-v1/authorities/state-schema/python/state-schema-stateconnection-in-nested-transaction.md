# state_schema.StateConnection.in_nested_transaction

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-in-nested-transaction:c4b802a6f8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5cc6e14548"></a>
- <a id="s-efa5248c19"></a>`distribution`: `state-schema`
- <a id="s-48d914c93a"></a>`module`: `state_schema`
- <a id="s-55788e5b8d"></a>`name`: `in_nested_transaction`
- <a id="s-8591aa4856"></a>`owner`: `state_schema.StateConnection`
- <a id="s-426378ad8a"></a>`unit`: `member`

### Declared structure

- <a id="s-22c738cdf6"></a>`kind`: `"method"`
- <a id="s-51a9a288ef"></a>`signature`: `"\"(self) -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-171e088202"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources/authorities.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateConnection.in_nested_transaction`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e39e0b03dc2299552b2a5119e4f25b3fabe95de38b1eb89a2215a53cef4cd9b7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bool'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "in_nested_transaction",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```

</details>
