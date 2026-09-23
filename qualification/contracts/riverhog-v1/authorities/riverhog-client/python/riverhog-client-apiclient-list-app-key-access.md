# riverhog_client.ApiClient.list_app_key_access

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-app-key-access:c67313f6e4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-71553a4d06"></a>
- <a id="s-f75faac3a0"></a>`distribution`: `riverhog-client`
- <a id="s-2746365972"></a>`module`: `riverhog_client`
- <a id="s-43dbef4642"></a>`name`: `list_app_key_access`
- <a id="s-9de3cb1fc1"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-fafb5e66e2"></a>`unit`: `member`

### Declared structure

- <a id="s-00c5138d80"></a>`kind`: `"method"`
- <a id="s-f3359efb48"></a>`signature`: `"\"(self, *, page_size: 'int' = 25, page_token: 'str \| None' = None, q: 'str \| None' = None, sort: 'ApplicationAccessSort' = 'permission', order: 'SortOrder' = 'asc', app: 'ApplicationName \| None' = None, key_id: 'ApplicationKeyId \| None' = None, permission: 'ApplicationPermission \| None' = None, resource: 'ApplicationResource \| None' = None, active: 'bool \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [piggity app key access list](../../piggity/cli/piggity-app-key-access-list.md)
- [GET /v1/app-key-access](../../riverhog/http-operations/get-v1-app-key-access.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-eed38506db"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.list\_app\_key\_access](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L2163)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_app_key_access`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82af5a73ada1473a20156b0cbf14b59964e70326e36ced46d5898cf21249cfe5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, sort: 'ApplicationAccessSort' = 'permission', order: 'SortOrder' = 'asc', app: 'ApplicationName | None' = None, key_id: 'ApplicationKeyId | None' = None, permission: 'ApplicationPermission | None' = None, resource: 'ApplicationResource | None' = None, active: 'bool | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_app_key_access",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
