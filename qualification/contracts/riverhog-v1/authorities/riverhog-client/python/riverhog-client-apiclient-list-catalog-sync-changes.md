# riverhog_client.ApiClient.list_catalog_sync_changes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-list-catalog-sync-changes:b2933b0a57 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9e0348d27e"></a>
- <a id="s-2ada865424"></a>`distribution`: `riverhog-client`
- <a id="s-1b0e090a5d"></a>`module`: `riverhog_client`
- <a id="s-e88906b493"></a>`name`: `list_catalog_sync_changes`
- <a id="s-f948bfe764"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-fcd28662bb"></a>`unit`: `member`

### Declared structure

- <a id="s-0805c4156f"></a>`kind`: `"method"`
- <a id="s-49f7d8ab3e"></a>`signature`: `"\"(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncChangePage'\""`

## Maintained corroboration

### Related interface records

- [piggity catalog-sync changes](../../piggity/cli/piggity-catalog-sync-changes.md)
- [GET /v1/catalog-sync/changes](../../riverhog/http-operations/get-v1-catalog-sync-changes.md)
- [ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-70a37cb6db"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — [packages/riverhog-client/src/riverhog\_client/\_\_init\_\_.py](../../../../../../packages/riverhog-client/src/riverhog_client/__init__.py)
- **Client method:** [packages/riverhog-client/src/riverhog\_client/client.py::ApiClient.list\_catalog\_sync\_changes](../../../../../../packages/riverhog-client/src/riverhog_client/client.py#L754)

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.list_catalog_sync_changes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 38bc2592f3410e4798e93791d495bf772d4550ca095b5115bf386f14e88c805d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, cursor: 'str', *, limit: 'int' = 100) -> 'CatalogSyncChangePage'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_catalog_sync_changes",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```

</details>
