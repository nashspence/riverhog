# riverhog_client.processing.ClaimedCollectionApi

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollectionapi:2d79c722e8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-46e7dea9ef"></a>
- <a id="s-10faafac62"></a>`distribution`: `riverhog-client`
- <a id="s-4cd7331ba0"></a>`module`: `riverhog_client.processing`
- <a id="s-9f90d94905"></a>`name`: `ClaimedCollectionApi`
- <a id="s-923847b0c5"></a>`unit`: `export`

### Declared structure

- <a id="s-5fe627ba34"></a>`kind`: `"class"`
- <a id="s-15040c199a"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [list_retrieval_plan_files](riverhog-client-processing-claimedcollectionapi-list-retrieval-plan-files.md)
- [get_portable_collection_inventory](riverhog-client-processing-claimedcollectionapi-get-portable-collection-inventory.md)
- [renew_retrieval_job](riverhog-client-processing-claimedcollectionapi-renew-retrieval-job.md)
- [get_collection](riverhog-client-processing-claimedcollectionapi-get-collection.md)
- [acknowledge_retrieval_job](riverhog-client-processing-claimedcollectionapi-acknowledge-retrieval-job.md)
- [plan_retrieval](riverhog-client-processing-claimedcollectionapi-plan-retrieval.md)
- [get_retrieval_job](riverhog-client-processing-claimedcollectionapi-get-retrieval-job.md)
- [stream_retrieval_file](riverhog-client-processing-claimedcollectionapi-stream-retrieval-file.md)
- [cancel_retrieval_job](riverhog-client-processing-claimedcollectionapi-cancel-retrieval-job.md)
- [download_retrieval_file](riverhog-client-processing-claimedcollectionapi-download-retrieval-file.md)
- [create_retrieval_job](riverhog-client-processing-claimedcollectionapi-create-retrieval-job.md)

## Governing policies

- <a id="pa-a1660e9b41"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionApi`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 73dc77496524759175aef146edbc7eeaf0baa9239fa2c5874537d4742bc89e4b -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "ClaimedCollectionApi",
  "unit": "export"
}
```

</details>
