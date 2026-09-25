# riverhog_client.CatalogFollowApi.list_catalog_sync_collections

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogfollowapi-list-cat-46a5f99b45:9ef305c010 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-cfb9583eb5"></a>
- <a id="s-c6d6e3c7af"></a>`distribution`: `riverhog-client`
- <a id="s-7e33ac6e2e"></a>`module`: `riverhog_client`
- <a id="s-fbdc313655"></a>`name`: `list_catalog_sync_collections`
- <a id="s-5c1abce9ab"></a>`owner`: `riverhog_client.CatalogFollowApi`
- <a id="s-e10fb3e4a8"></a>`unit`: `member`

### Declared structure

- <a id="s-d60482e665"></a>`kind`: `"method"`
- <a id="s-8e309db1d4"></a>`signature`: `"\"(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncCollectionPage'\""`

## Maintained corroboration

### Related interface records

- [CatalogFollowApi](riverhog-client-catalogfollowapi.md)

## Governing policies

- <a id="pa-da5400328f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogFollowApi.list_catalog_sync_collections`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c93e9cdc980ec20894cbbbe9c3dbf4010dfe2c28dfb53a1cd9f520e42bb040b9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncCollectionPage'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_catalog_sync_collections",
  "owner": "riverhog_client.CatalogFollowApi",
  "unit": "member"
}
```

</details>
