# review0_sampler_protocol.SamplerDescriptor.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerdescripto-483a794372:8a762a73df -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c1d83cac9f"></a>
- <a id="s-caec2018be"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-9c26f2bb38"></a>`module`: `review0_sampler_protocol`
- <a id="s-02cafedae8"></a>`name`: `verify_digest`
- <a id="s-e855486368"></a>`owner`: `review0_sampler_protocol.SamplerDescriptor`
- <a id="s-fed9b44674"></a>`unit`: `member`

### Declared structure

- <a id="s-ffa48ae6fc"></a>`kind`: `"method"`
- <a id="s-c34ab6e5ef"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [SamplerDescriptor](review0-sampler-protocol-samplerdescriptor.md)

## Governing policies

- <a id="pa-2f3471dc3c"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerDescriptor.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 11ff72cab3f78cbb5916510f4d728d14d1d6cbe868a461b532f5948aa626df3a -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "verify_digest",
  "owner": "review0_sampler_protocol.SamplerDescriptor",
  "unit": "member"
}
```

</details>
