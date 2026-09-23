# review0_sampler_protocol.SamplerResult.state_shape

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerresult-state-shape:4efdcfb087 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b0040a6d1f"></a>
- <a id="s-231acf1e81"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-e068769e3e"></a>`module`: `review0_sampler_protocol`
- <a id="s-5a68f85a7c"></a>`name`: `state_shape`
- <a id="s-ee96e5439d"></a>`owner`: `review0_sampler_protocol.SamplerResult`
- <a id="s-e157df26b7"></a>`unit`: `member`

### Declared structure

- <a id="s-7bffc28ed0"></a>`kind`: `"method"`
- <a id="s-25f6d7f22c"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [SamplerResult](review0-sampler-protocol-samplerresult.md)

## Governing policies

- <a id="pa-71934d2e7e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerResult.state_shape`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d0d3466f9b1c7ad11d2edb22086d21093f3096e91d92d37ae005bca8f2996186 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "state_shape",
  "owner": "review0_sampler_protocol.SamplerResult",
  "unit": "member"
}
```

</details>
