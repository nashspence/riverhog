# riverhog_storage_adapter_protocol.STORAGE_ADAPTER_PROTOCOL

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-92f2486fca:8f188d5f5c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d0d0f459ed"></a>
- <a id="s-7767c59bdc"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-a635f6a5ca"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-62b06e9615"></a>`name`: `STORAGE_ADAPTER_PROTOCOL`
- <a id="s-287f7eaee5"></a>`unit`: `export`

### Declared structure

- <a id="s-4838237031"></a>`kind`: `"constant"`
- <a id="s-b5b91a9253"></a>`value`: `"riverhog-storage-adapter/v1"`

## Governing policies

- <a id="pa-df40859bda"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.STORAGE_ADAPTER_PROTOCOL`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c4ba1947543c8a8ed15d7bc9d849b42d3783af836798ce44c1943ad74f5b84fa -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "riverhog-storage-adapter/v1"
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "STORAGE_ADAPTER_PROTOCOL",
  "unit": "export"
}
```

</details>
