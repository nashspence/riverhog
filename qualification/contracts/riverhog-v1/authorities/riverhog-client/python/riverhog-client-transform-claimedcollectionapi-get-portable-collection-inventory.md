# riverhog_client.transform.ClaimedCollectionApi.get_portable_collection_inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-client:riverhog-client-transform-claimedcollecti-a26ee51058:5dc26007e0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-37c3559cf1"></a>
- <a id="s-1a5b23cec9"></a>`distribution`: `riverhog-client`
- <a id="s-0623cb4c0f"></a>`module`: `riverhog_client.transform`
- <a id="s-49f76e4498"></a>`name`: `get_portable_collection_inventory`
- <a id="s-f9c21ae2d0"></a>`owner`: `riverhog_client.transform.ClaimedCollectionApi`
- <a id="s-cc147c32bb"></a>`unit`: `member`

### Declared structure

- <a id="s-c8081d6bdc"></a>`kind`: `"method"`
- <a id="s-a25bb7c417"></a>`signature`: `"\"(self, collection_id: 'CollectionId', *, cursor: 'str \| None' = None, limit: 'int' = 100, inventory_identity: 'str \| None' = None) -> 'PortableCollectionInventoryPage'\""`

## Maintained corroboration

### Related interface records

- [ClaimedCollectionApi](riverhog-client-transform-claimedcollectionapi.md)

## Governing policies

- <a id="pa-beeb168ab5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-client:riverhog_client.transform](../../../evidence/sources.md#src-7a247bb534) — `packages/riverhog-client/src/riverhog_client/transform/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_client.transform.ClaimedCollectionApi.get_portable_collection_inventory`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0b77a8e4971e3e5287efed146652ed028dbcc8a57de911abddab6dbce3899785 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'CollectionId', *, cursor: 'str | None' = None, limit: 'int' = 100, inventory_identity: 'str | None' = None) -> 'PortableCollectionInventoryPage'\""
  },
  "distribution": "riverhog-client",
  "module": "riverhog_client.transform",
  "name": "get_portable_collection_inventory",
  "owner": "riverhog_client.transform.ClaimedCollectionApi",
  "unit": "member"
}
```
