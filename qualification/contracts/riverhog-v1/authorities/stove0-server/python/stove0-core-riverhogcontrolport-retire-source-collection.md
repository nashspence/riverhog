# stove0_core.RiverhogControlPort.retire_source_collection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-retire-so-5fc5cd75a3:2f7ad7fb47 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9d536c76e2"></a>
- <a id="s-93a895ee0a"></a>`distribution`: `stove0-server`
- <a id="s-431422c2a0"></a>`module`: `stove0_core`
- <a id="s-86517ea500"></a>`name`: `retire_source_collection`
- <a id="s-c9e6023652"></a>`owner`: `stove0_core.RiverhogControlPort`
- <a id="s-621cfbe3bc"></a>`unit`: `member`

### Declared structure

- <a id="s-c9002a83dd"></a>`kind`: `"method"`
- <a id="s-ba5de548ec"></a>`signature`: `"\"(self, record: 'WorkRecord', collection_id: 'int') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-2d411c683a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.retire_source_collection`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 34909d558bfd4cd354cc41063dba0a5219b46ede9328ed2fd933ffd7939add12 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord', collection_id: 'int') -> 'bool'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "retire_source_collection",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```

</details>
