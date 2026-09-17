# state_schema.StateConnection.get_transaction

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:state-schema:state-schema-stateconnection-get-transaction:b87fd1578e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [state-schema](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-56454b310d"></a>
- <a id="s-3412af6905"></a>`distribution`: `state-schema`
- <a id="s-950a9166d7"></a>`module`: `state_schema`
- <a id="s-3a20125ab2"></a>`name`: `get_transaction`
- <a id="s-e7ff999ac3"></a>`owner`: `state_schema.StateConnection`
- <a id="s-eef543a1aa"></a>`unit`: `member`

### Declared structure

- <a id="s-4fa59c2f83"></a>`kind`: `"method"`
- <a id="s-15285067b7"></a>`signature`: `"\"(self) -> 'Optional[RootTransaction]'\""`

## Maintained corroboration

### Related interface records

- [StateConnection](state-schema-stateconnection.md)

## Governing policies

- <a id="pa-f59e87e51f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:state-schema:state_schema](../../../evidence/sources/authorities.md#src-57d87d192c) — [packages/state-schema/src/state\_schema/\_\_init\_\_.py](../../../../../../packages/state-schema/src/state_schema/__init__.py)

### Machine authority

- `/external_contract/python/state_schema.StateConnection.get_transaction`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: edb313ac2ffc1e884deae01265038f21348b1eca0d29683d95ef1829e871c0ac -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Optional[RootTransaction]'\""
  },
  "distribution": "state-schema",
  "module": "state_schema",
  "name": "get_transaction",
  "owner": "state_schema.StateConnection",
  "unit": "member"
}
```

</details>
