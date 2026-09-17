# riverhog_storage_adapter_protocol.StorageAdapterPort.list_segments

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-protocol:riverhog-storage-adapter-protocol-storage-a7ebd7b783:ad4af0fc3e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5b98b81abe"></a>
- <a id="s-d54d51de54"></a>`distribution`: `riverhog-storage-adapter-protocol`
- <a id="s-de26fb884b"></a>`module`: `riverhog_storage_adapter_protocol`
- <a id="s-a5d4c978ab"></a>`name`: `list_segments`
- <a id="s-941122ac48"></a>`owner`: `riverhog_storage_adapter_protocol.StorageAdapterPort`
- <a id="s-599b3a8b7a"></a>`unit`: `member`

### Declared structure

- <a id="s-87fe846fa5"></a>`kind`: `"method"`
- <a id="s-1b495cc290"></a>`signature`: `"\"(self, request: 'WriteSegmentListRequest') -> 'WriteSegmentPage'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterPort](riverhog-storage-adapter-protocol-storageadapterport.md)

## Governing policies

- <a id="pa-e8a90591e5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-protocol:riverhog_storage_adapter_protocol](../../../evidence/sources.md#src-2da8857a83) — [packages/riverhog-storage-adapter-protocol/src/riverhog\_storage\_adapter\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-protocol/src/riverhog_storage_adapter_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_protocol.StorageAdapterPort.list_segments`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0d7e5187dd3f1eb25d4bfa8d4b82485b9433bc409bc636ca3527d44f21b715bd -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'WriteSegmentListRequest') -> 'WriteSegmentPage'\""
  },
  "distribution": "riverhog-storage-adapter-protocol",
  "module": "riverhog_storage_adapter_protocol",
  "name": "list_segments",
  "owner": "riverhog_storage_adapter_protocol.StorageAdapterPort",
  "unit": "member"
}
```

</details>
