# riverhog_client.ApiClient.create_catalog_sync_checkpoint

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-create-catalog-5e423d65e8:b01201c6d4 -->

Exact externally visible contract owned by this semantic dossier.

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

- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-90caa438c9"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.create_catalog_sync_checkpoint`

### Exact owned JSON

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
