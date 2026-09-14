# riverhog_client.ApiClient.list_retrieval_cache_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-retrieval-b626e434fc:5127530cd5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-164b1757e3"></a>
- <a id="s-dda78e7985"></a>`distribution`: `riverhog-client`
- <a id="s-d3c1e9cf77"></a>`module`: `riverhog_client`
- <a id="s-180ddaa7e9"></a>`name`: `list_retrieval_cache_objects`
- <a id="s-6f4f8258fa"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-107b57ec79"></a>`unit`: `member`

### Declared structure

- <a id="s-e736bc6e8a"></a>`kind`: `"method"`
- <a id="s-e72fe3f39f"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None, q: 'str \| None' = None, collection_id: 'CollectionId \| None' = None, source_store: 'ArchiveStoreName \| None' = None, cache_store: 'RetrievalCacheStoreName \| None' = None, state: 'RetrievalCacheState \| None' = None, protection: 'RetrievalCacheProtection \| None' = None, expires_before: 'str \| None' = None, expires_after: 'str \| None' = None, sort: 'RetrievalCacheSort' = 'cached_at', order: 'SortOrder' = 'desc') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-ca9061b9a8"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_retrieval_cache_objects`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e1177bfb1e55c99498109c883f8eab4cc2bd4f7078ac4b76318360f14d456a3a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, collection_id: 'CollectionId | None' = None, source_store: 'ArchiveStoreName | None' = None, cache_store: 'RetrievalCacheStoreName | None' = None, state: 'RetrievalCacheState | None' = None, protection: 'RetrievalCacheProtection | None' = None, expires_before: 'str | None' = None, expires_after: 'str | None' = None, sort: 'RetrievalCacheSort' = 'cached_at', order: 'SortOrder' = 'desc') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_retrieval_cache_objects",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
