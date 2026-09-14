# riverhog_client.ApiClient.list_apps

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-apps:5befa28a94 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bd8d067282"></a>
- <a id="s-43ec4a6597"></a>`distribution`: `riverhog-client`
- <a id="s-a047755848"></a>`module`: `riverhog_client`
- <a id="s-1591de9eb2"></a>`name`: `list_apps`
- <a id="s-c0d63be24d"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-66662b9168"></a>`unit`: `member`

### Declared structure

- <a id="s-4f99b36580"></a>`kind`: `"method"`
- <a id="s-35e6fd0e58"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None, q: 'str \| None' = None, sort: 'ApplicationSort' = 'name', order: 'SortOrder' = 'asc', active: 'bool \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-6b917485c6"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_apps`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5fd1f13b903cfd4f3a6a4b4c6733eca11ddaa8a3db83ca28ae1e834e52278f6c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, sort: 'ApplicationSort' = 'name', order: 'SortOrder' = 'asc', active: 'bool | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_apps",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
