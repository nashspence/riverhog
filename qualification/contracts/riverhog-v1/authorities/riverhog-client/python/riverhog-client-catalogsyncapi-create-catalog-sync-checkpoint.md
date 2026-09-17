# riverhog_client.CatalogSyncApi.create_catalog_sync_checkpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogsyncapi-create-cat-66f3bb72b4:67f6fc1116 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-091f5e6a9a"></a>
- <a id="s-2ed37c783c"></a>`distribution`: `riverhog-client`
- <a id="s-11fd346636"></a>`module`: `riverhog_client`
- <a id="s-f44d6f8099"></a>`name`: `create_catalog_sync_checkpoint`
- <a id="s-1c479a6621"></a>`owner`: `riverhog_client.CatalogSyncApi`
- <a id="s-acf7ea9ba6"></a>`unit`: `member`

### Declared structure

- <a id="s-bb284cdf21"></a>`kind`: `"method"`
- <a id="s-60405b0557"></a>`signature`: `"\"(self) -> 'CatalogSyncCheckpoint'\""`

## Maintained corroboration

### Related interface records

- [CatalogSyncApi](riverhog-client-catalogsyncapi.md)

## Governing policies

- <a id="pa-284c5b5f00"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_client.CatalogSyncApi.create_catalog_sync_checkpoint`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 82289c7432ad730d21ef0fd704ab9f2a31f28377ecec3deea0e5299bd8eb4dc5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'CatalogSyncCheckpoint'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_catalog_sync_checkpoint",
  "owner": "riverhog_client.CatalogSyncApi",
  "unit": "member"
}
```

</details>
