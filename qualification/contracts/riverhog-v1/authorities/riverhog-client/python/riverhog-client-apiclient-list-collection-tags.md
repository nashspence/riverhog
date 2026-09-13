# riverhog_client.ApiClient.list_collection_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-collection-tags:d1b1b78527 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-080f3aa4cb"></a>
| Field | Shape |
|---|---|
| <a id="s-6ce18424f7"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-e78e516e8d"></a>`distribution` | "riverhog-client" |
| <a id="s-cd8b756ec1"></a>`module` | "riverhog_client" |
| <a id="s-1ba58764cb"></a>`name` | "list_collection_tags" |
| <a id="s-47ffc1c454"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-5f58c96a8f"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-89d148c8ae"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_collection_tags`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 749811bf92d5bde8374b2bf823d3f3582c45a045555cceae34a9fd4569ccec7e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, revision: 'int', tag_set_identity: 'str', page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_collection_tags",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
