# riverhog_storage_adapter_protocol.validated_storage_adapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-validat-d0b27f1383:4a9fc31dfe -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3bfbde7af4"></a>
- <a id="s-85d5895918"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-6f227418f4"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-b3363cdf7e"></a>`name`: `validated_storage_adapter`
- <a id="s-bc53a5a3ac"></a>`unit`: `export`

### Declared structure

- <a id="s-eba7ae0264"></a>`kind`: `"function"`
- <a id="s-d9a0d3ed53"></a>`signature`: `"\"(adapter: 'StorageAdapterPort') -> 'ValidatedStorageAdapterPort'\""`

## Governing policies

- <a id="pa-6b4894f7ca"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.validated_storage_adapter`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ebf238714980f358fd0a60bda76bc9809b015f83da15986f59eb1c976b8451b6 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(adapter: 'StorageAdapterPort') -> 'ValidatedStorageAdapterPort'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "validated_storage_adapter",
  "unit": "export"
}
```

</details>
