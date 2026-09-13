# riverhog_client.ApiClient.list_app_keys

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-app-keys:d01c0ea0a7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d2bb3b497b"></a>
| Field | Shape |
|---|---|
| <a id="s-67569839e6"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-e31d0413fb"></a>`distribution` | "riverhog-client" |
| <a id="s-550376d77e"></a>`module` | "riverhog_client" |
| <a id="s-da68833f3c"></a>`name` | "list_app_keys" |
| <a id="s-63c10dcf76"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-4ed7ea3e07"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-8b987febcf"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_app_keys`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08338fa6139d03050bb7674284dceb308326c461174ea9a694352c08a01c4f7e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, app: 'ApplicationName', *, page_size: 'int' = 25, page_token: 'str | None' = None, q: 'str | None' = None, sort: 'ApplicationKeySort' = 'created_at', order: 'SortOrder' = 'desc', active: 'bool | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_app_keys",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
