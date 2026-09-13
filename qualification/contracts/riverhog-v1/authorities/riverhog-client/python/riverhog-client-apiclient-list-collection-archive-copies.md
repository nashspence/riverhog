# riverhog_client.ApiClient.list_collection_archive_copies

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-collection-db295c0400:5e3cf3974f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0d61b006bf"></a>
| Field | Shape |
|---|---|
| <a id="s-8f63259646"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-108427f19a"></a>`distribution` | "riverhog-client" |
| <a id="s-f05cbfa645"></a>`module` | "riverhog_client" |
| <a id="s-09dd9ec1a8"></a>`name` | "list_collection_archive_copies" |
| <a id="s-cfc6f8c5d6"></a>`owner` | "riverhog_client.ApiClient" |
| <a id="s-d1c99e6602"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-18cca84f5e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_collection_archive_copies`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 49c3b7ab8d370078be75828cb7bc5bf80ef95a708b6ae6e54f43b903fa5bb053 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_collection_archive_copies",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
