# stove0_core.RiverhogApi.get_portable_collection_inventory

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-get-portable-coll-af507b1efa:6c849b3b4d -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7cdc737bae"></a>
- <a id="s-a5501fc5ec"></a>`distribution`: `stove0-server`
- <a id="s-db566b5ae6"></a>`module`: `stove0_core`
- <a id="s-3516af0496"></a>`name`: `get_portable_collection_inventory`
- <a id="s-7b286d0653"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-075b20213f"></a>`unit`: `member`

### Declared structure

- <a id="s-4e8d478fff"></a>`kind`: `"method"`
- <a id="s-6746bfe578"></a>`signature`: `"\"(self, collection_id: 'int', *, cursor: 'str \| None' = None, limit: 'int' = 100, inventory_identity: 'str \| None' = None) -> 'PortableCollectionInventoryPage'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-b93838bde2"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.get_portable_collection_inventory`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 504db837a01de5758d1ad1ad245b5d7e7914dc55787a3d1023973db8a65df3e1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'int', *, cursor: 'str | None' = None, limit: 'int' = 100, inventory_identity: 'str | None' = None) -> 'PortableCollectionInventoryPage'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "get_portable_collection_inventory",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
