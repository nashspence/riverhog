# riverhog_storage_adapter_protocol.StorageAdapterPort.write_segment

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-3c5e27fb06:cd803231db -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-05c33814bf"></a>
- <a id="s-50ec41544f"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-858e8009a0"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-37dc1bdec9"></a>`name`: `write_segment`
- <a id="s-28831519ea"></a>`owner`: `riverhog_storage_adapter_protocol.StorageAdapterPort`
- <a id="s-4bcb185550"></a>`unit`: `member`

### Declared structure

- <a id="s-378a3937cd"></a>`kind`: `"method"`
- <a id="s-9b2acfc6d7"></a>`signature`: `"\"(self, *, session: 'WriteSession', number: 'int', stored_bytes: 'int', content: 'BinaryContent') -> 'WriteSegmentReceipt'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterPort](riverhog-storage-adapter-protocol-storageadapterport.md)

## Governing policies

- <a id="pa-8095a0a65c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterPort.write_segment`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5a954d0722f0afc919de5f2c70f393a65d53f6c8c878515a1394503e92e7eefc -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, session: 'WriteSession', number: 'int', stored_bytes: 'int', content: 'BinaryContent') -> 'WriteSegmentReceipt'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "write_segment",
  "owner": "riverhog_storage_adapter_protocol.StorageAdapterPort",
  "unit": "member"
}
```

</details>
