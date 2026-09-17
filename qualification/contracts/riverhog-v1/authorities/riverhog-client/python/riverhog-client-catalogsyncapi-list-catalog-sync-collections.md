# riverhog_client.CatalogSyncApi.list_catalog_sync_collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogsyncapi-list-catal-31a8618401:7eeccb82b3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2e0a07fb4a"></a>
- <a id="s-b2f0892bb9"></a>`distribution`: `riverhog-client`
- <a id="s-797781321b"></a>`module`: `riverhog_client`
- <a id="s-c27a60e84d"></a>`name`: `list_catalog_sync_collections`
- <a id="s-ec985390e4"></a>`owner`: `riverhog_client.CatalogSyncApi`
- <a id="s-b74542acb8"></a>`unit`: `member`

### Declared structure

- <a id="s-bbbed5969f"></a>`kind`: `"method"`
- <a id="s-5c708f12e8"></a>`signature`: `"\"(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncCollectionPage'\""`

## Maintained corroboration

### Related interface records

- [CatalogSyncApi](riverhog-client-catalogsyncapi.md)

## Governing policies

- <a id="pa-91a408f9d9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogSyncApi.list_catalog_sync_collections`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ac78054906db6d347732b9cb3f45ed4fcb01a27d6798b67faeac487a65271dc2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncCollectionPage'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_catalog_sync_collections",
  "owner": "riverhog_client.CatalogSyncApi",
  "unit": "member"
}
```

</details>
