# riverhog_client.CatalogSyncApi

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogsyncapi:674a28aeba -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-92619df247"></a>
| Field | Shape |
|---|---|
| <a id="s-8db08170ca"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-414e92ac37"></a>`distribution` | "riverhog-client" |
| <a id="s-6ba3c6647d"></a>`module` | "riverhog_client" |
| <a id="s-963facea37"></a>`name` | "CatalogSyncApi" |
| <a id="s-8cfcf60499"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [riverhog_client.CatalogSyncApi.create_catalog_sync_checkpoint](riverhog-client-catalogsyncapi-create-catalog-sync-checkpoint.md)
- [riverhog_client.CatalogSyncApi.list_catalog_sync_collections](riverhog-client-catalogsyncapi-list-catalog-sync-collections.md)
- [riverhog_client.CatalogSyncApi.list_catalog_sync_changes](riverhog-client-catalogsyncapi-list-catalog-sync-changes.md)
- [riverhog_client.CatalogSyncApi.list_collection_tags](riverhog-client-catalogsyncapi-list-collection-tags.md)

## Governing policies

- <a id="pa-7a1782bce0"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.CatalogSyncApi`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f33602f773d38967b03759b92634c16423a9026b44775881093fff5117d1964 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "CatalogSyncApi",
  "unit": "export"
}
```
