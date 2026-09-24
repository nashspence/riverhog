# riverhog_protocol.collection_id_for_event

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-id-for-event:6c975c1332 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cb014d3fb8"></a>
- <a id="s-984990ea21"></a>`distribution`: `riverhog-protocol`
- <a id="s-4b6c3bb021"></a>`module`: `riverhog_protocol`
- <a id="s-1d72cf6d9c"></a>`name`: `collection_id_for_event`
- <a id="s-c122b298df"></a>`unit`: `export`

### Declared structure

- <a id="s-e362c6097b"></a>`kind`: `"function"`
- <a id="s-31ae953f7b"></a>`signature`: `"\"(value: 'LifecycleEvent \| dict[str, Any]') -> 'int'\""`

## Governing policies

- <a id="pa-a18c60a670"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.collection_id_for_event`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 24fb571e43bc4ac242bd3cd4e27a0fbfcca197c1fd8366f512495a8a48861953 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(value: 'LifecycleEvent | dict[str, Any]') -> 'int'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "collection_id_for_event",
  "unit": "export"
}
```

</details>
