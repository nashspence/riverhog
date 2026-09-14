# riverhog_storage_adapter_protocol.StorageAdapterPort.head_object

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-f904b68bb7:1e83bc7085 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c158ff9ac9"></a>
- <a id="s-10760e6bbb"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-a09b190608"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-34627bc126"></a>`name`: `head_object`
- <a id="s-6323dbf082"></a>`owner`: `riverhog_storage_adapter_protocol.StorageAdapterPort`
- <a id="s-45c9cdd9a9"></a>`unit`: `member`

### Declared structure

- <a id="s-8ff85de574"></a>`kind`: `"method"`
- <a id="s-cbbd33e6c7"></a>`signature`: `"\"(self, request: 'ObjectHeadRequest') -> 'ObjectMetadataReceipt \| None'\""`

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.StorageAdapterPort](riverhog-storage-adapter-protocol-storageadapterport.md)

## Governing policies

- <a id="pa-7f07e8b199"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterPort.head_object`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6dbd6578428766f10ed4ebaedd39d3d70369138fda7a446c42852203f03743d5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ObjectHeadRequest') -> 'ObjectMetadataReceipt | None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "head_object",
  "owner": "riverhog_storage_adapter_protocol.StorageAdapterPort",
  "unit": "member"
}
```
