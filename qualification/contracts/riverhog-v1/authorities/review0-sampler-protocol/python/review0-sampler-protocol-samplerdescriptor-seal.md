# review0_sampler_protocol.SamplerDescriptor.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerdescriptor-seal:2b286279c3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5e9368af0c"></a>
- <a id="s-e170f488dd"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-2bbc2ad828"></a>`module`: `review0_sampler_protocol`
- <a id="s-c98a652006"></a>`name`: `seal`
- <a id="s-c6e0f4a774"></a>`owner`: `review0_sampler_protocol.SamplerDescriptor`
- <a id="s-0993be8a73"></a>`unit`: `member`

### Declared structure

- <a id="s-2b13847262"></a>`kind`: `"classmethod"`
- <a id="s-1af4feda05"></a>`signature`: `"\"(cls, payload: 'SamplerDescriptorPayload') -> 'SamplerDescriptor'\""`

## Maintained corroboration

### Related interface records

- [SamplerDescriptor](review0-sampler-protocol-samplerdescriptor.md)

## Governing policies

- <a id="pa-321d2a6bda"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerDescriptor.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4541ff187e27603b11f0c9f57dee656bdd6061f4888a9118e7063707699a0229 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'SamplerDescriptorPayload') -> 'SamplerDescriptor'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "seal",
  "owner": "review0_sampler_protocol.SamplerDescriptor",
  "unit": "member"
}
```

</details>
