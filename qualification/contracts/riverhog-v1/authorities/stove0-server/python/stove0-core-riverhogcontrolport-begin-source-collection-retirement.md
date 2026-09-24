# stove0_core.RiverhogControlPort.begin_source_collection_retirement

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-riverhogcontrolport-begin-sou-9f08e8b6f8:1564d99893 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8a570a4cfb"></a>
- <a id="s-1eefbafd70"></a>`distribution`: `stove0-server`
- <a id="s-1fb330745d"></a>`module`: `stove0_core`
- <a id="s-f08de02576"></a>`name`: `begin_source_collection_retirement`
- <a id="s-26634fb689"></a>`owner`: `stove0_core.RiverhogControlPort`
- <a id="s-87e05ac3bd"></a>`unit`: `member`

### Declared structure

- <a id="s-9458bf4666"></a>`kind`: `"method"`
- <a id="s-1c7796760c"></a>`signature`: `"\"(self, record: 'WorkRecord') -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [RiverhogControlPort](stove0-core-riverhogcontrolport.md)

## Governing policies

- <a id="pa-d6a235c67e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.RiverhogControlPort.begin_source_collection_retirement`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 285f0f1f27ba4ab3e0d240432b4d717fad0f9aaa1661aa2a4cfeb66cc282440b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, record: 'WorkRecord') -> 'bool'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "begin_source_collection_retirement",
  "owner": "stove0_core.RiverhogControlPort",
  "unit": "member"
}
```

</details>
