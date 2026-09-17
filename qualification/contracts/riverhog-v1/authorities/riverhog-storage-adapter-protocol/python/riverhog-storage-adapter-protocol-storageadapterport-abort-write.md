# riverhog_storage_adapter_protocol.StorageAdapterPort.abort_write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-872588722b:18eb4c44bb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-01fefbdb27"></a>
- <a id="s-b722845896"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-510715ebd5"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-2edd441bb7"></a>`name`: `abort_write`
- <a id="s-74d854eaf3"></a>`owner`: `riverhog_storage_adapter_protocol.StorageAdapterPort`
- <a id="s-ab87626c83"></a>`unit`: `member`

### Declared structure

- <a id="s-c272888a6f"></a>`kind`: `"method"`
- <a id="s-c0ba3e26c0"></a>`signature`: `"\"(self, session: 'WriteSession') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterPort](riverhog-storage-adapter-protocol-storageadapterport.md)

## Governing policies

- <a id="pa-79a96268c7"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources/authorities.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterPort.abort_write`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 43ec776ce779ebc036c1fe96dd706fd4faf4bbbcd9e230b0862d6932caccd704 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, session: 'WriteSession') -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "abort_write",
  "owner": "riverhog_storage_adapter_protocol.StorageAdapterPort",
  "unit": "member"
}
```

</details>
