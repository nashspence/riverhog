# riverhog_client.processing.ClaimedCollectionApi.get_portable_collection_inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-processing-claimedcollect-2acd505811:1e32aefc00 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-af4c511c3e"></a>
- <a id="s-70fe983a39"></a>`distribution`: `riverhog-client`
- <a id="s-9d7f6cd3e2"></a>`module`: `riverhog_client.processing`
- <a id="s-82e60c687b"></a>`name`: `get_portable_collection_inventory`
- <a id="s-c34bbdb68b"></a>`owner`: `riverhog_client.processing.ClaimedCollectionApi`
- <a id="s-49fb5cecff"></a>`unit`: `member`

### Declared structure

- <a id="s-637e86c3bd"></a>`kind`: `"method"`
- <a id="s-c134480a48"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, cursor: 'str \| None' = None, limit: 'int' = 100, inventory_identity: 'str \| None' = None) -> 'PortableCollectionInventoryPage'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-processing-claimedcollectionapi.md)

## Governing policies

- <a id="pa-b417bdc9b9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client.processing](../../../evidence/sources/authorities.md#src-89057c8bbf) — [packages/riverhog-client/src/riverhog\_client/processing/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/processing/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.processing.ClaimedCollectionApi.get_portable_collection_inventory`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4ff3cc2bd7693409c7593f195f5dfe15bf48aa595a10cd9c3014347f02e7e5b7 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, cursor: 'str | None' = None, limit: 'int' = 100, inventory_identity: 'str | None' = None) -> 'PortableCollectionInventoryPage'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.processing",
  "name": "get_portable_collection_inventory",
  "owner": "riverhog_client.processing.ClaimedCollectionApi",
  "unit": "member"
}
```

</details>
