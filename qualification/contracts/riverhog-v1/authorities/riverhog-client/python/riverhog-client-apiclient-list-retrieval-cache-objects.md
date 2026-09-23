# riverhog_client.ApiClient.list_retrieval_cache_objects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-retrieval-b626e434fc:5127530cd5 -->

Exact externally visible contract owned by this contract element.

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

- [piggity retrieval cache list](../../piggity/cli/piggity-retrieval-cache-list.md)
- [GET /v1/retrieval-cache/objects](../../riverhog/http-operations/get-v1-retrieval-cache-objects.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-ca9061b9a8"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.list\_retrieval\_cache\_objects](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L910)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_retrieval_cache_objects`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
