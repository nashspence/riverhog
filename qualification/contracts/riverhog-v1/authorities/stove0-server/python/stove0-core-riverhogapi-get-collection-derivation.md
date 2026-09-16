# stove0_core.RiverhogApi.get_collection_derivation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-get-collection-derivation:c40ccaa82a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fee7cf3780"></a>
- <a id="s-aafe365f43"></a>`distribution`: `stove0-server`
- <a id="s-ef5a938175"></a>`module`: `stove0_core`
- <a id="s-503c064197"></a>`name`: `get_collection_derivation`
- <a id="s-969cf5fcb8"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-efee3a640f"></a>`unit`: `member`

### Declared structure

- <a id="s-7a405965c5"></a>`kind`: `"method"`
- <a id="s-79388ed787"></a>`signature`: `"\"(self, collection_id: 'int') -> 'CollectionDerivationResponseDocument'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-015c9ba9a5"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-server:stove0_core](../../../evidence/sources.md#src-7558b08e7f) — `reference/stove0/application/server/src/stove0_core/__init__.py`

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.get_collection_derivation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 803f852f27caca30dff3ce7616581071927276ddc804fc5a0897e3670e9a1dba -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'int') -> 'CollectionDerivationResponseDocument'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "get_collection_derivation",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
