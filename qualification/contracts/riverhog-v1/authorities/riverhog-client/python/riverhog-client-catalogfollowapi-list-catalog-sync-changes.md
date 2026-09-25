# riverhog_client.CatalogFollowApi.list_catalog_sync_changes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogfollowapi-list-cat-eb6dfba458:d83e877ee8 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3329ee00db"></a>
- <a id="s-97887d3604"></a>`distribution`: `riverhog-client`
- <a id="s-da8e7ac001"></a>`module`: `riverhog_client`
- <a id="s-0b1ce62684"></a>`name`: `list_catalog_sync_changes`
- <a id="s-1a98df2521"></a>`owner`: `riverhog_client.CatalogFollowApi`
- <a id="s-0d5e9f8ad7"></a>`unit`: `member`

### Declared structure

- <a id="s-9597a9ed1c"></a>`kind`: `"method"`
- <a id="s-6ee4b064af"></a>`signature`: `"\"(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncChangePage'\""`

## Maintained corroboration

### Related interface records

- [CatalogFollowApi](riverhog-client-catalogfollowapi.md)

## Governing policies

- <a id="pa-8236c4c4b1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogFollowApi.list_catalog_sync_changes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 11943273b4c191e9c61d84f11674227a7868f4aba9bebd1a87018a62e0b90a53 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncChangePage'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_catalog_sync_changes",
  "owner": "riverhog_client.CatalogFollowApi",
  "unit": "member"
}
```

</details>
