# stove0_review_sampler_protocol.validate_result

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-review-sampler-protocol:stove0-review-sampler-protocol-validate-result:b1beecddca -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-review-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6b0aca7989"></a>
- <a id="s-8436317451"></a>`distribution`: `stove0-review-sampler-protocol`
- <a id="s-49ba9d1bf6"></a>`module`: `stove0_review_sampler_protocol`
- <a id="s-7b92f0a74b"></a>`name`: `validate_result`
- <a id="s-f6ddf6cd73"></a>`unit`: `export`

### Declared structure

- <a id="s-3338b3bdc7"></a>`kind`: `"function"`
- <a id="s-4871adf0fd"></a>`signature`: `"\"(result: 'SamplerResult', request: 'SamplerRequest', descriptor: 'SamplerDescriptor') -> 'None'\""`

## Governing policies

- <a id="pa-06f275b7e1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-review-sampler-protocol:stove0_review_sampler_protocol](../../../evidence/sources.md#src-25c43eb779) — [reference/stove0/targets/review/sampler/protocol/src/stove0\_review\_sampler\_protocol/\_\_init\_\_.py](../../../../../../reference/stove0/targets/review/sampler/protocol/src/stove0_review_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_review_sampler_protocol.validate_result`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2d2d1e15d1c550924610fb7093df3767716447585ecaa5f29461177d572fb88c -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(result: 'SamplerResult', request: 'SamplerRequest', descriptor: 'SamplerDescriptor') -> 'None'\""
  },
  "distribution": "stove0-review-sampler-protocol",
  "module": "stove0_review_sampler_protocol",
  "name": "validate_result",
  "unit": "export"
}
```

</details>
