# riverhog_client.ApiClient.list_collection_provenance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-collection-provenance:acb712ceb3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-aa6068ec56"></a>
- <a id="s-7f9ca4f02a"></a>`distribution`: `riverhog-client`
- <a id="s-64a884326b"></a>`module`: `riverhog_client`
- <a id="s-06ea0b1b2a"></a>`name`: `list_collection_provenance`
- <a id="s-5d21f47071"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-d672d22f4c"></a>`unit`: `member`

### Declared structure

- <a id="s-9e0c40c108"></a>`kind`: `"method"`
- <a id="s-8b859ac0ab"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, page_size: 'int' = 25, page_token: 'str \| None' = None, q: 'str \| None' = None, status: 'ProvenanceStatus \| None' = None, sort: 'ProvenanceSort' = 'path', order: 'SortOrder' = 'asc') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity collection provenance list](../../piggity/cli/piggity-collection-provenance-list.md)
- [GET /v1/collections/{collection_id}/provenance/files](../../riverhog/http-operations/get-v1-collections-collection-id-provenance-files.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-2667b148b5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.list\_collection\_provenance](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1629)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_collection_provenance`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 917f0ac816ee8172f77409068286a99e58fc1806b07fc8013fc98f267058d898 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, status: 'ProvenanceStatus | None' = None, sort: 'ProvenanceSort' = 'path', order: 'SortOrder' = 'asc') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_collection_provenance",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
