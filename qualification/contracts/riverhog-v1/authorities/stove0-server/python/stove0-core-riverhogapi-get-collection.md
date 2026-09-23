# stove0_core.RiverhogApi.get_collection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogapi-get-collection:36774f786e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7956c40d6b"></a>
- <a id="s-0ef91e80af"></a>`distribution`: `stove0-server`
- <a id="s-ddfedbcb1d"></a>`module`: `stove0_core`
- <a id="s-a5da606474"></a>`name`: `get_collection`
- <a id="s-92e76b0bd9"></a>`owner`: `stove0_core.RiverhogApi`
- <a id="s-f8bf1436ad"></a>`unit`: `member`

### Declared structure

- <a id="s-aeca85f994"></a>`kind`: `"method"`
- <a id="s-c305f8aceb"></a>`signature`: `"\"(self, collection_id: 'int') -> 'dict[str, Any]'\""`

## Maintained corroboration

### Related interface records

- [RiverhogApi](stove0-core-riverhogapi.md)

## Governing policies

- <a id="pa-7989be8744"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogApi.get_collection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f1897b3915608312eaa98ccedc6af2b92e15e4dae7577e9c5678cc24c041253e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, collection_id: 'int') -> 'dict[str, Any]'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "get_collection",
  "owner": "stove0_core.RiverhogApi",
  "unit": "member"
}
```

</details>
