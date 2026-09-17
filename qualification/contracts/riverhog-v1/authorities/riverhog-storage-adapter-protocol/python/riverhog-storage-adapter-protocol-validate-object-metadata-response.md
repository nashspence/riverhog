# riverhog_storage_adapter_protocol.validate_object_metadata_response

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-706deda7d7:4e99977ae6 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-18e1483356"></a>
- <a id="s-d26dd2f029"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-f60cef52a4"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-2c9a3180d8"></a>`name`: `validate_object_metadata_response`
- <a id="s-98159bdbdc"></a>`unit`: `export`

### Declared structure

- <a id="s-32c3f1ddb7"></a>`kind`: `"function"`
- <a id="s-1d78d5a499"></a>`signature`: `"\"(request: 'ObjectHeadRequest', response: 'ObjectMetadataReceipt') -> 'None'\""`

## Governing policies

- <a id="pa-ae2a19493f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validate_object_metadata_response`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0e179d26a5142ca6bfbc7f82d2567f1368e1431521d8be4a41133c5aa71cdd0e -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'ObjectHeadRequest', response: 'ObjectMetadataReceipt') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_object_metadata_response",
  "unit": "export"
}
```

</details>
