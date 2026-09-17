# riverhog_client.ApiClient.create_catalog_sync_checkpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-create-catalog-5e423d65e8:b01201c6d4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0083175495"></a>
- <a id="s-29c658f14f"></a>`distribution`: `riverhog-client`
- <a id="s-5c2761b5e7"></a>`module`: `riverhog_client`
- <a id="s-beb8aadeaf"></a>`name`: `create_catalog_sync_checkpoint`
- <a id="s-248a5a0531"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-dd386eb500"></a>`unit`: `member`

### Declared structure

- <a id="s-d7a25294a8"></a>`kind`: `"method"`
- <a id="s-aa52a94c0c"></a>`signature`: `"\"(self) -> 'CatalogSyncCheckpoint'\""`

## Maintained corroboration

### Related interface records

- [piggity catalog-sync checkpoint](../../piggity/cli/piggity-catalog-sync-checkpoint.md)
- [GET /v1/catalog-sync/checkpoint](../../riverhog/http-operations/get-v1-catalog-sync-checkpoint.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-90caa438c9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources/authorities.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.create\_catalog\_sync\_checkpoint](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L734)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.create_catalog_sync_checkpoint`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9ece9c43951161ffa947b590d49ce72f709d94c176c51fc6065dba33fe64ea6d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'CatalogSyncCheckpoint'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "create_catalog_sync_checkpoint",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
