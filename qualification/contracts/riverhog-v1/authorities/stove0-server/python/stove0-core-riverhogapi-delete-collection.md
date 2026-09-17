# stove0_core.RiverhogApi.delete_collection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-delete-collection:7b8e433065 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7be61dfd4e"></a>
- <a id="s-9a4384f54c"></a>`distribution`: `stove0-server`
- <a id="s-8bd97719f0"></a>`module`: `stove0_core`
- <a id="s-12cc622224"></a>`name`: `delete_collection`
- <a id="s-acc8a5379a"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-fed00c7994"></a>`unit`: `member`

### Declared structure

- <a id="s-1416ac4874"></a>`kind`: `"method"`
- <a id="s-b78a7510da"></a>`signature`: `"\"(self, collection_id: 'int', *, challenge: 'str', retirement_claim_id: 'str \| None' = None, event_context: 'Mapping[str, Any] \| None' = None) -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-21e0cf2f2f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [reference/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../reference/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.delete_collection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 1ab837e8b3f8ed76d7fde9c0c54bb4a9ffb9fa5d692819fe906c2c94a1697ec6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'int', *, challenge: 'str', retirement_claim_id: 'str | None' = None, event_context: 'Mapping[str, Any] | None' = None) -> 'dict[str, Any]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "delete_collection",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
