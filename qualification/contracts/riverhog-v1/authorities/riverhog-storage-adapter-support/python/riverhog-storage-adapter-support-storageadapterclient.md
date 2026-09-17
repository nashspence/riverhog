# riverhog_storage_adapter_support.StorageAdapterClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-storage-adapter-support:riverhog-storage-adapter-support-storagea-39187ac4d5:14fc4450ce -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9f54e035ce"></a>
- <a id="s-bd8661b7e4"></a>`distribution`: `riverhog-storage-adapter-support`
- <a id="s-862cbcbcc7"></a>`module`: `riverhog_storage_adapter_support`
- <a id="s-f256fdcc14"></a>`name`: `StorageAdapterClient`
- <a id="s-1fde2f7ee9"></a>`unit`: `export`

### Declared structure

- <a id="s-70b52cff7f"></a>`kind`: `"class"`
- <a id="s-2d25332ed2"></a>`signature`: `"\"(base_url: 'str', *, token: 'str', allow_insecure_http: 'bool' = False, timeout: 'float \| httpx.Timeout \| None' = 300.0, maximum_connections: 'int' = 32, client: 'httpx.Client \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [descriptor](riverhog-storage-adapter-support-storageadapterclient-descriptor.md)
- [write_segment](riverhog-storage-adapter-support-storageadapterclient-write-segment.md)
- [put_small_object](riverhog-storage-adapter-support-storageadapterclient-put-small-object.md)
- [from_token_file](riverhog-storage-adapter-support-storageadapterclient-from-token-file.md)
- [delete_object](riverhog-storage-adapter-support-storageadapterclient-delete-object.md)
- [close](riverhog-storage-adapter-support-storageadapterclient-close.md)
- [prepare_read](riverhog-storage-adapter-support-storageadapterclient-prepare-read.md)
- [abort_write](riverhog-storage-adapter-support-storageadapterclient-abort-write.md)
- [find_completed_write](riverhog-storage-adapter-support-storageadapterclient-find-completed-write.md)
- [read_object](riverhog-storage-adapter-support-storageadapterclient-read-object.md)
- [check_readiness](riverhog-storage-adapter-support-storageadapterclient-check-readiness.md)
- [begin_write](riverhog-storage-adapter-support-storageadapterclient-begin-write.md)
- [complete_write](riverhog-storage-adapter-support-storageadapterclient-complete-write.md)
- [cleanup_read](riverhog-storage-adapter-support-storageadapterclient-cleanup-read.md)
- [head_object](riverhog-storage-adapter-support-storageadapterclient-head-object.md)
- [read_status](riverhog-storage-adapter-support-storageadapterclient-read-status.md)
- [list_segments](riverhog-storage-adapter-support-storageadapterclient-list-segments.md)
- [delete_prefix](riverhog-storage-adapter-support-storageadapterclient-delete-prefix.md)

## Governing policies

- <a id="pa-2d3aeb5a99"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-storage-adapter-support:riverhog_storage_adapter_support](../../../evidence/sources/authorities.md#src-284271cd54) — [packages/riverhog-storage-adapter-support/src/riverhog\_storage\_adapter\_support/\_\_init\_\_.py](../../../../../../packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_storage_adapter_support.StorageAdapterClient`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a5ff841c487131cda6201b8224b0746011e8d17f084a17ec2b1bfe94beaf0ae5 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(base_url: 'str', *, token: 'str', allow_insecure_http: 'bool' = False, timeout: 'float | httpx.Timeout | None' = 300.0, maximum_connections: 'int' = 32, client: 'httpx.Client | None' = None) -> 'None'\""
  },
  "distribution": "riverhog-storage-adapter-support",
  "module": "riverhog_storage_adapter_support",
  "name": "StorageAdapterClient",
  "unit": "export"
}
```

</details>
