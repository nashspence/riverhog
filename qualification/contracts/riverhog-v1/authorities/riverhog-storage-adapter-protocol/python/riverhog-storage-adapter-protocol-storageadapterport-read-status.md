# riverhog_storage_adapter_protocol.StorageAdapterPort.read_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-0d5b2caec8:b3beccb783 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2519677cf5"></a>
- <a id="s-b9ca275265"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-b585bcb25c"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-1212935ce2"></a>`name`: `read_status`
- <a id="s-a019700e88"></a>`owner`: `riverhog_storage_adapter_protocol.StorageAdapterPort`
- <a id="s-c24317a695"></a>`unit`: `member`

### Declared structure

- <a id="s-9b5d0d73e4"></a>`kind`: `"method"`
- <a id="s-0c8ddb80f2"></a>`signature`: `"\"(self, request: 'ReadPreparationRequest') -> 'ReadStatus'\""`

## Maintained corroboration

### Related interface records

- [riverhog_storage_adapter_protocol.StorageAdapterPort](riverhog-storage-adapter-protocol-storageadapterport.md)

## Governing policies

- <a id="pa-8f66318096"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — `packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterPort.read_status`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 79d3669d1a016eb40dc077c4f2709fd9cf39ba4b222bff498ce9e9156adb7143 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ReadPreparationRequest') -> 'ReadStatus'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "read_status",
  "owner": "riverhog_storage_adapter_protocol.StorageAdapterPort",
  "unit": "member"
}
```
