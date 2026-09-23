# riverhog_client.ApiClient.list_apps

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-apps:5befa28a94 -->

Exact externally visible contract owned by this contract element.

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

- [piggity app list](../../piggity/cli/piggity-app-list.md)
- [GET /v1/apps](../../riverhog/http-operations/get-v1-apps.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-6b917485c6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.list\_apps](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2063)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_apps`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
