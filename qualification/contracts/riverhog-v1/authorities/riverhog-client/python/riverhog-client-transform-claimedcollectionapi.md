# riverhog_client.transform.ClaimedCollectionApi

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollectionapi:c751e26dc5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c5e29dbedb"></a>
- <a id="s-67dd9cc75a"></a>`distribution`: `riverhog-client`
- <a id="s-97a2eda9f0"></a>`module`: `riverhog_client.transform`
- <a id="s-13714ef2e0"></a>`name`: `ClaimedCollectionApi`
- <a id="s-8014c2be0d"></a>`unit`: `export`

### Declared structure

- <a id="s-3bb8ed4fbf"></a>`kind`: `"class"`
- <a id="s-6a8facf183"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [renew_retrieval_job](riverhog-client-transform-claimedcollectionapi-renew-retrieval-job.md)
- [acknowledge_retrieval_job](riverhog-client-transform-claimedcollectionapi-acknowledge-retrieval-job.md)
- [get_retrieval_job](riverhog-client-transform-claimedcollectionapi-get-retrieval-job.md)
- [get_collection](riverhog-client-transform-claimedcollectionapi-get-collection.md)
- [download_retrieval_file](riverhog-client-transform-claimedcollectionapi-download-retrieval-file.md)
- [get_portable_collection_inventory](riverhog-client-transform-claimedcollectionapi-get-portable-collection-inventory.md)
- [list_retrieval_plan_files](riverhog-client-transform-claimedcollectionapi-list-retrieval-plan-files.md)
- [plan_retrieval](riverhog-client-transform-claimedcollectionapi-plan-retrieval.md)
- [create_retrieval_job](riverhog-client-transform-claimedcollectionapi-create-retrieval-job.md)
- [cancel_retrieval_job](riverhog-client-transform-claimedcollectionapi-cancel-retrieval-job.md)
- [stream_retrieval_file](riverhog-client-transform-claimedcollectionapi-stream-retrieval-file.md)

## Governing policies

- <a id="pa-90aaadf562"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — [packages/riverhog-client/src/riverhog\_client/transform/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/transform/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionApi`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 78b0c34b5e2160d4d49343b20f3eb3952ba806af490335bb290201e29d1d4c6a -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "ClaimedCollectionApi",
  "unit": "export"
}
```

</details>
