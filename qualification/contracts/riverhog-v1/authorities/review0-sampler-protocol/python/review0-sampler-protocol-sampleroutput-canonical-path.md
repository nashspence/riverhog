# review0_sampler_protocol.SamplerOutput.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-sampleroutput-ca-a116d1b3ba:0b26857f1c -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-df73e014db"></a>
- <a id="s-abdbf63e49"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-432128f21f"></a>`module`: `review0_sampler_protocol`
- <a id="s-b6c0eca9c7"></a>`name`: `canonical_path`
- <a id="s-d4de439ab4"></a>`owner`: `review0_sampler_protocol.SamplerOutput`
- <a id="s-4256857368"></a>`unit`: `member`

### Declared structure

- <a id="s-1dce5706bd"></a>`kind`: `"classmethod"`
- <a id="s-e806e16ebb"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [SamplerOutput](review0-sampler-protocol-sampleroutput.md)

## Governing policies

- <a id="pa-cb36eca733"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerOutput.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 86d63d82ec4870ad8e9b5aa664d3dceebee8e04ff862025a6cd161b892326336 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "canonical_path",
  "owner": "review0_sampler_protocol.SamplerOutput",
  "unit": "member"
}
```

</details>
