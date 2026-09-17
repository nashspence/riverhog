# riverhog_storage_adapter_support.StorageAdapterClient.from_token_file

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-2e4ae648cf:79fe85bfe0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fe28e1a91e"></a>
- <a id="s-d11ef85f32"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-94ed5676aa"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-116cc0211f"></a>`name`: `from_token_file`
- <a id="s-92885c5b03"></a>`owner`: `riverhog_storage_adapter_support.StorageAdapterClient`
- <a id="s-c1eabf1b97"></a>`unit`: `member`

### Declared structure

- <a id="s-1e595238f0"></a>`kind`: `"classmethod"`
- <a id="s-cf2491e7bc"></a>`signature`: `"\"(cls, base_url: 'str', *, token_file: 'Path', allow_insecure_http: 'bool' = False, timeout: 'float \| httpx.Timeout \| None' = 300.0, maximum_connections: 'int' = 32, client: 'httpx.Client \| None' = None) -> 'StorageAdapterClient'\""`

## Maintained corroboration

### Related interface records

- [StorageAdapterClient](riverhog-storage-adapter-support-storageadapterclient.md)

## Governing policies

- <a id="pa-934d38c737"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient.from_token_file`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9971e725f251033bd40fc49de58a5e2cd12b8dc7b389fd0bcf6fa84e9bd1ce0f -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, base_url: 'str', *, token_file: 'Path', allow_insecure_http: 'bool' = False, timeout: 'float | httpx.Timeout | None' = 300.0, maximum_connections: 'int' = 32, client: 'httpx.Client | None' = None) -> 'StorageAdapterClient'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "from_token_file",
  "owner": "riverhog_storage_adapter_support.StorageAdapterClient",
  "unit": "member"
}
```

</details>
