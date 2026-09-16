# riverhog_storage_adapter_support.StorageAdapterClient.read_status

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-e178f459b6:0361687fa0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7f26f6fc90"></a>
- <a id="s-a9a385da14"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-297686a363"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-d1cbb2b08e"></a>`name`: `read_status`
- <a id="s-c08e8627f4"></a>`owner`: `riverhog_storage_adapter_support.StorageAdapterClient`
- <a id="s-fb017b193a"></a>`unit`: `member`

### Declared structure

- <a id="s-338ae3f0c6"></a>`kind`: `"method"`
- <a id="s-d233bc9717"></a>`signature`: `"\"(self, request: 'ReadPreparationRequest') -> 'ReadStatus'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-02e5bed84b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.read_status`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 504e1d7f0482c6dd253ed0a33517f6d5828fd406b3116c7a9d1f9e0487770ea4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'ReadPreparationRequest') -> 'ReadStatus'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "read_status",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```

</details>
