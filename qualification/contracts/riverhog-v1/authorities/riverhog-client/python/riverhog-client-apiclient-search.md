# riverhog_client.ApiClient.search

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-search:d85ca73de9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ab763349ac"></a>
- <a id="s-ba37f0d411"></a>`distribution`: `riverhog-client`
- <a id="s-3d9e439043"></a>`module`: `riverhog_client`
- <a id="s-98e437423c"></a>`name`: `search`
- <a id="s-c5cc5c18ab"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-fcb30a7182"></a>`unit`: `member`

### Declared structure

- <a id="s-6344cdd10a"></a>`kind`: `"method"`
- <a id="s-dfa9fd3e8e"></a>`signature`: `"\"(self, query: 'str \| None' = None, *, page_size: 'int' = 25, page_token: 'str \| None' = None, sort: 'SearchSort' = 'file_ref', order: 'SortOrder' = 'asc', collection: 'CollectionId \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity find](../../piggity/cli/piggity-find.md)
- [GET /v1/search](../../riverhog/http-operations/get-v1-search.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-d9b8aee2a0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.search](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L1545)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.search`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5edad3b43944799f8d8cbb460a6955e53137166db6b7dc88e14ef1c91ae47dc3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, query: 'str | None' = None, *, page_size: 'int' = 25, page_token: 'str | None' = None, sort: 'SearchSort' = 'file_ref', order: 'SortOrder' = 'asc', collection: 'CollectionId | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "search",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
