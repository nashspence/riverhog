# review0_sampler_protocol.SamplerResult.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerresult-seal:a9afa47b84 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5b955e7ce2"></a>
- <a id="s-edffab6ebd"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-33e6ed8768"></a>`module`: `review0_sampler_protocol`
- <a id="s-08cfa2e5e9"></a>`name`: `seal`
- <a id="s-0da2f904dc"></a>`owner`: `review0_sampler_protocol.SamplerResult`
- <a id="s-8e91de0cfe"></a>`unit`: `member`

### Declared structure

- <a id="s-f9e097ae4e"></a>`kind`: `"classmethod"`
- <a id="s-1d415b40c0"></a>`signature`: `"\"(cls, payload: 'SamplerResultPayload') -> 'SamplerResult'\""`

## Maintained corroboration

### Related interface records

- [SamplerResult](review0-sampler-protocol-samplerresult.md)

## Governing policies

- <a id="pa-07063d25a4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerResult.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff0d2d9adedda8c736178c1cfa5068d89ceb1b5b3e2d99c71739fbd090ea39fa -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'SamplerResultPayload') -> 'SamplerResult'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "seal",
  "owner": "review0_sampler_protocol.SamplerResult",
  "unit": "member"
}
```

</details>
