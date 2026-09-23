# review0_sampler_lib.ReviewSampler.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-reviewsampler-descriptor:8096d91911 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-41efd5f3d0"></a>
- <a id="s-7730a2035c"></a>`distribution`: `review0-sampler-lib`
- <a id="s-8181e727d9"></a>`module`: `review0_sampler_lib`
- <a id="s-59f8abf676"></a>`name`: `descriptor`
- <a id="s-de7f20ba2e"></a>`owner`: `review0_sampler_lib.ReviewSampler`
- <a id="s-366c2f0c68"></a>`unit`: `member`

### Declared structure

- <a id="s-571cacf7fe"></a>`kind`: `"method"`
- <a id="s-75484077b3"></a>`signature`: `"\"(self) -> 'SamplerDescriptor'\""`

## Maintained corroboration

### Related interface records

- [ReviewSampler](review0-sampler-lib-reviewsampler.md)

## Governing policies

- <a id="pa-7c1f6afc53"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.ReviewSampler.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: efd54405a1eadf488fe53f41f6bef5f771b1c7a719fff4b95a16da05ef10be5c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'SamplerDescriptor'\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "descriptor",
  "owner": "review0_sampler_lib.ReviewSampler",
  "unit": "member"
}
```

</details>
