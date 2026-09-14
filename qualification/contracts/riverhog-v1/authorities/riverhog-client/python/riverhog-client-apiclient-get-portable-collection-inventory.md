# riverhog_client.ApiClient.get_portable_collection_inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-apiclient-get-portable-co-c6f02e0a33:767e4986e5 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d982e05480"></a>
- <a id="s-86bf00265f"></a>`distribution`: `riverhog-client`
- <a id="s-c416f361e7"></a>`module`: `riverhog_client`
- <a id="s-1e736bac95"></a>`name`: `get_portable_collection_inventory`
- <a id="s-eb36257f75"></a>`owner`: `riverhog_client.ApiClient`
- <a id="s-7593d8e0a7"></a>`unit`: `member`

### Declared structure

- <a id="s-d621d783a1"></a>`kind`: `"method"`
- <a id="s-238ba6ae80"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, cursor: 'str \| None' = None, limit: 'int' = 100, inventory_identity: 'str \| None' = None) -> 'PortableCollectionInventoryPage'\""`

## Maintained corroboration

### Related interface records

- [riverhog_client.ApiClient](riverhog-client-apiclient.md)

## Governing policies

- <a id="pa-d0b0847b05"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client](../../../evidence/sources.md#src-c149020c71) — `packages/riverhog-client/src/riverhog_client/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.ApiClient.get_portable_collection_inventory`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5b2783cae7d5201de052942369ddaecd9d4bf50d734184d56d5799d1c5de44aa -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, cursor: 'str | None' = None, limit: 'int' = 100, inventory_identity: 'str | None' = None) -> 'PortableCollectionInventoryPage'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client",
  "name": "get_portable_collection_inventory",
  "owner": "riverhog_client.ApiClient",
  "unit": "member"
}
```
