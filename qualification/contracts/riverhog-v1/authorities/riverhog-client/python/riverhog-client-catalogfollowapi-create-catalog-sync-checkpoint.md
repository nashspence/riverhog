# riverhog_client.CatalogFollowApi.create_catalog_sync_checkpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogfollowapi-create-c-1eb4b032d5:ec551536e4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e8fffe0871"></a>
- <a id="s-7c740d68ce"></a>`distribution`: `riverhog-client`
- <a id="s-05741a8074"></a>`module`: `riverhog_client`
- <a id="s-096c2d483f"></a>`name`: `create_catalog_sync_checkpoint`
- <a id="s-ef48b81d52"></a>`owner`: `riverhog_client.CatalogFollowApi`
- <a id="s-cd66d9de36"></a>`unit`: `member`

### Declared structure

- <a id="s-40fe83466f"></a>`kind`: `"method"`
- <a id="s-d88e49cca6"></a>`signature`: `"\"(self) -> 'CatalogSyncCheckpoint'\""`

## Maintained corroboration

### Related interface records

- [CatalogFollowApi](riverhog-client-catalogfollowapi.md)

## Governing policies

- <a id="pa-b67359390e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogFollowApi.create_catalog_sync_checkpoint`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c82b6d97ffc93b45d0efd1c1ec9cda8c8a78059f75854407e383ab8dd098a04d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'CatalogSyncCheckpoint'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_catalog_sync_checkpoint",
  "owner": "riverhog_client.CatalogFollowApi",
  "unit": "member"
}
```

</details>
