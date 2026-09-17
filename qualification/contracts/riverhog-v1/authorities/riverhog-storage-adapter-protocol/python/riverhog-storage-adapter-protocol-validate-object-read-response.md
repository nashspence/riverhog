# riverhog_storage_adapter_protocol.validate_object_read_response

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-cb4bc9869b:0614a9d9ec -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a7a4b46bd0"></a>
- <a id="s-f94d92b557"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-35cf3e7dc5"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-6f25ad1980"></a>`name`: `validate_object_read_response`
- <a id="s-cf2c2b5194"></a>`unit`: `export`

### Declared structure

- <a id="s-db5abc11fd"></a>`kind`: `"function"`
- <a id="s-e2824357bf"></a>`signature`: `"\"(request: 'ObjectReadRequest', response: 'ObjectReadReceipt') -> 'None'\""`

## Governing policies

- <a id="pa-e14325d008"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validate_object_read_response`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 689807520ebf0075465165f0d4dde7c8a66c5c4877c1ce018d2d115183cc6a19 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'ObjectReadRequest', response: 'ObjectReadReceipt') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validate_object_read_response",
  "unit": "export"
}
```

</details>
