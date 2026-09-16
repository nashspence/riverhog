# riverhog_client.ApiClient.list_catalog_sync_collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-catalog-sy-4971582ac5:4c0ea9f162 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f6c9560723"></a>
- <a id="s-929363c3ef"></a>`distribution`: `riverhog-client`
- <a id="s-73ac65e8e1"></a>`module`: `riverhog_client`
- <a id="s-e5b6bbbe17"></a>`name`: `list_catalog_sync_collections`
- <a id="s-06e0662762"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-4cea204666"></a>`unit`: `member`

### Declared structure

- <a id="s-b7364f4c2a"></a>`kind`: `"method"`
- <a id="s-1f6f7011b3"></a>`signature`: `"\"(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncCollectionPage'\""`

## Maintained corroboration

### Related interface records

- [piggity catalog-sync collections](../../piggity/cli/piggity-catalog-sync-collections.md)
- [GET /v1/catalog-sync/collections](../../riverhog/http-operations/get-v1-catalog-sync-collections.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-b7532bb2db"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`
- **Client method:** [packages/riverhog-client/src/riverhog_client/client.py::ApiClient.list_catalog_sync_collections](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L739)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_catalog_sync_collections`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1d291d00bab4f33cbe637b0c22fc658351fc2f87094651c97da2fb10c592dc0c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncCollectionPage'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_catalog_sync_collections",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
