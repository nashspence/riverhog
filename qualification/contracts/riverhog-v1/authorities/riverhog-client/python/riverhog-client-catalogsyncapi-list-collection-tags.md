# riverhog_client.CatalogSyncApi.list_collection_tags

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-catalogsyncapi-list-collection-tags:d3ec8315f1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e7c3cf9838"></a>
- <a id="s-fc36704604"></a>`distribution`: `riverhog-client`
- <a id="s-535b34135b"></a>`module`: `riverhog_client`
- <a id="s-d8531b79fc"></a>`name`: `list_collection_tags`
- <a id="s-16bd13fa51"></a>`owner`: `riverhog_client.CatalogSyncApi`
- <a id="s-ada093eb6a"></a>`unit`: `member`

### Declared structure

- <a id="s-6a0fc88d73"></a>`kind`: `"method"`
- <a id="s-f9b63859d0"></a>`signature`: `"\"(self, collection_id: 'int', *, revision: 'int', tag_set_identity: 'str', page_size: 'int' = 25, page_token: 'str \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [CatalogSyncApi](riverhog-client-catalogsyncapi.md)

## Governing policies

- <a id="pa-42162bdefd"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.CatalogSyncApi.list_collection_tags`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4f7d1ff0eb27c5cd70adfb1ff6e7508e6b91aa909a31c7595505f174241d258a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'int', *, revision: 'int', tag_set_identity: 'str', page_size: 'int' = 25, page_token: 'str | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "list_collection_tags",
  "owner": "riverhog_client.CatalogSyncApi",
  "unit": "member"
}
```
