# review0_sampler_protocol.SamplerRequestPayload.canonical_inputs

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerrequestpa-91f3859655:1c5b82fa69 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1a0e45b5f9"></a>
- <a id="s-c316cc2699"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-39581bea7c"></a>`module`: `review0_sampler_protocol`
- <a id="s-f5dc2d8cd5"></a>`name`: `canonical_inputs`
- <a id="s-f4c791a7e2"></a>`owner`: `review0_sampler_protocol.SamplerRequestPayload`
- <a id="s-c26e1b83ef"></a>`unit`: `member`

### Declared structure

- <a id="s-074f9b89d8"></a>`kind`: `"classmethod"`
- <a id="s-d921b02e88"></a>`signature`: `"\"(cls, value: 'tuple[SamplerInput, ...]') -> 'tuple[SamplerInput, ...]'\""`

## Maintained corroboration

### Related interface records

- [SamplerRequestPayload](review0-sampler-protocol-samplerrequestpayload.md)

## Governing policies

- <a id="pa-9a822844d0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerRequestPayload.canonical_inputs`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bee407745337c257c32a305093830f2895b30ca1ebfb50dc2cda9497e64e4902 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[SamplerInput, ...]') -> 'tuple[SamplerInput, ...]'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "canonical_inputs",
  "owner": "review0_sampler_protocol.SamplerRequestPayload",
  "unit": "member"
}
```

</details>
