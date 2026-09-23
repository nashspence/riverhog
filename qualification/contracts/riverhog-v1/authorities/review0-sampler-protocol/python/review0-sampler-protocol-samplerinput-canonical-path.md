# review0_sampler_protocol.SamplerInput.canonical_path

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerinput-canonical-path:20af62930d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6871956039"></a>
- <a id="s-e317839693"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-32847cdd7e"></a>`module`: `review0_sampler_protocol`
- <a id="s-c1f7e2bf4c"></a>`name`: `canonical_path`
- <a id="s-54433f08ae"></a>`owner`: `review0_sampler_protocol.SamplerInput`
- <a id="s-aee75e1e9d"></a>`unit`: `member`

### Declared structure

- <a id="s-27db5010b6"></a>`kind`: `"classmethod"`
- <a id="s-4b9b722475"></a>`signature`: `"\"(cls, value: 'str') -> 'str'\""`

## Maintained corroboration

### Related interface records

- [SamplerInput](review0-sampler-protocol-samplerinput.md)

## Governing policies

- <a id="pa-77bb0964d1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerInput.canonical_path`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f9a29bae8dfcc06287e0a33a2ac4c33dd4643ef58b14615b1de6c0e48331dbd0 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'str') -> 'str'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "canonical_path",
  "owner": "review0_sampler_protocol.SamplerInput",
  "unit": "member"
}
```

</details>
