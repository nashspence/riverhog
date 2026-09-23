# review0_sampler_protocol.SamplerOutput.canonical_sources

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-sampleroutput-ca-11623dd0a7:6968b20391 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c04fd047c7"></a>
- <a id="s-81fc8b517b"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-6aeb08b075"></a>`module`: `review0_sampler_protocol`
- <a id="s-80e630ffca"></a>`name`: `canonical_sources`
- <a id="s-f686857c61"></a>`owner`: `review0_sampler_protocol.SamplerOutput`
- <a id="s-fc7e1a09a7"></a>`unit`: `member`

### Declared structure

- <a id="s-569e7d5a7a"></a>`kind`: `"classmethod"`
- <a id="s-8a3ac2d2f8"></a>`signature`: `"\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""`

## Maintained corroboration

### Related interface records

- [SamplerOutput](review0-sampler-protocol-sampleroutput.md)

## Governing policies

- <a id="pa-3a7b127803"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerOutput.canonical_sources`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ae9cf7be85f5948dc8e079067fb83420a0fd9f4a826b23a567d3c68bbfb8b138 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[str, ...]') -> 'tuple[str, ...]'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "canonical_sources",
  "owner": "review0_sampler_protocol.SamplerOutput",
  "unit": "member"
}
```

</details>
