# riverhog_client.CatalogSyncApi.list_catalog_sync_changes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogsyncapi-list-catal-a1de088ac3:f512409d11 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-dc54a13366"></a>
- <a id="s-9b483ff07f"></a>`distribution`: `riverhog-client`
- <a id="s-f72716b504"></a>`module`: `riverhog_client`
- <a id="s-3f21da637d"></a>`name`: `list_catalog_sync_changes`
- <a id="s-5083e9a91f"></a>`owner`: `riverhog_client.CatalogSyncApi`
- <a id="s-3d7b103b58"></a>`unit`: `member`

### Declared structure

- <a id="s-752eb7b016"></a>`kind`: `"method"`
- <a id="s-f63809443e"></a>`signature`: `"\"(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncChangePage'\""`

## Maintained corroboration

### Related interface records

- [CatalogSyncApi](riverhog-client-catalogsyncapi.md)

## Governing policies

- <a id="pa-977a04f178"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogSyncApi.list_catalog_sync_changes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 745d9f581cb4461afafd8a2a731384b852b1be9555856c5fc074b2c4885f52ff -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncChangePage'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_catalog_sync_changes",
  "owner": "riverhog_client.CatalogSyncApi",
  "unit": "member"
}
```

</details>
