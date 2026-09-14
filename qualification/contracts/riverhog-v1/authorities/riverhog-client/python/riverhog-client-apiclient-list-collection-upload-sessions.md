# riverhog_client.ApiClient.list_collection_upload_sessions

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-collection-057a011129:5b3fdd488b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bbfd68900a"></a>
- <a id="s-ca6c7bca14"></a>`distribution`: `riverhog-client`
- <a id="s-b1d6b6f5bb"></a>`module`: `riverhog_client`
- <a id="s-306e76b3e5"></a>`name`: `list_collection_upload_sessions`
- <a id="s-37cbdeca0a"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-5408872b97"></a>`unit`: `member`

### Declared structure

- <a id="s-689b51146c"></a>`kind`: `"method"`
- <a id="s-c126bbe923"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None, q: 'str \| None' = None, state: 'CollectionUploadState \| None' = None, sort: 'CollectionUploadSort' = 'created_at', order: 'SortOrder' = 'desc') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-46e3b83497"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_collection_upload_sessions`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: af8d1f4748c1d5ac48e0c3910fe128fde14451031833e9ebf78b5d96bf0492ae -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, state: 'CollectionUploadState | None' = None, sort: 'CollectionUploadSort' = 'created_at', order: 'SortOrder' = 'desc') -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_collection_upload_sessions",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
