# stove0_core.Stove0RiverhogClient.retire_source_collection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0riverhogclient-retire-s-cf38eb0345:521375b407 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1e5fb7bb65"></a>
- <a id="s-29965efc99"></a>`distribution`: `stove0-server`
- <a id="s-905400f22b"></a>`module`: `stove0_core`
- <a id="s-4923e35523"></a>`name`: `retire_source_collection`
- <a id="s-88537a81a1"></a>`owner`: `stove0_core.Stove0RiverhogClient`
- <a id="s-ace4235818"></a>`unit`: `member`

### Declared structure

- <a id="s-853c965e64"></a>`kind`: `"method"`
- <a id="s-3b7c50ccfb"></a>`signature`: `"\"(self, record: 'WorkRecord', collection_id: 'int') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [Stove0RiverhogClient](stove0-core-stove0riverhogclient.md)

## Governing policies

- <a id="pa-75069e2d7d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0RiverhogClient.retire_source_collection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: daa5ec11767facb91ea6222e4d363ed431ad235a7ab5b11c7333a12f6af9de15 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord', collection_id: 'int') -> 'bool'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "retire_source_collection",
  "owner": "stove0_core.Stove0RiverhogClient",
  "unit": "member"
}
```

</details>
