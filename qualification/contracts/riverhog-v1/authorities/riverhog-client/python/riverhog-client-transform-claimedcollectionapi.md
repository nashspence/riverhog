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
| Field | Shape |
|---|---|
| <a id="s-4c98f83cbf"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-67dd9cc75a"></a>`distribution` | "riverhog-client" |
| <a id="s-97a2eda9f0"></a>`module` | "riverhog_client.transform" |
| <a id="s-13714ef2e0"></a>`name` | "ClaimedCollectionApi" |
| <a id="s-8014c2be0d"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_client.transform.ClaimedCollectionApi.renew_retrieval_job](riverhog-client-transform-claimedcollectionapi-renew-retrieval-job.md)
- [riverhog_client.transform.ClaimedCollectionApi.acknowledge_retrieval_job](riverhog-client-transform-claimedcollectionapi-acknowledge-retrieval-job.md)
- [riverhog_client.transform.ClaimedCollectionApi.get_retrieval_job](riverhog-client-transform-claimedcollectionapi-get-retrieval-job.md)
- [riverhog_client.transform.ClaimedCollectionApi.get_collection](riverhog-client-transform-claimedcollectionapi-get-collection.md)
- [riverhog_client.transform.ClaimedCollectionApi.download_retrieval_file](riverhog-client-transform-claimedcollectionapi-download-retrieval-file.md)
- [riverhog_client.transform.ClaimedCollectionApi.get_portable_collection_inventory](riverhog-client-transform-claimedcollectionapi-get-portable-collection-inventory.md)
- [riverhog_client.transform.ClaimedCollectionApi.list_retrieval_plan_files](riverhog-client-transform-claimedcollectionapi-list-retrieval-plan-files.md)
- [riverhog_client.transform.ClaimedCollectionApi.plan_retrieval](riverhog-client-transform-claimedcollectionapi-plan-retrieval.md)
- [riverhog_client.transform.ClaimedCollectionApi.create_retrieval_job](riverhog-client-transform-claimedcollectionapi-create-retrieval-job.md)
- [riverhog_client.transform.ClaimedCollectionApi.cancel_retrieval_job](riverhog-client-transform-claimedcollectionapi-cancel-retrieval-job.md)
- [riverhog_client.transform.ClaimedCollectionApi.stream_retrieval_file](riverhog-client-transform-claimedcollectionapi-stream-retrieval-file.md)

## Governing policies

- <a id="pa-90aaadf562"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionApi`

### Exact owned JSON

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
