# review0_sampler_lib.SamplerConformanceResult.validate_result

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-samplerconformanceres-97024b94f7:c3e9e2dedb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fb0ec6046d"></a>
- <a id="s-c43161bb61"></a>`distribution`: `review0-sampler-lib`
- <a id="s-0fd0f51f03"></a>`module`: `review0_sampler_lib`
- <a id="s-4e951d7b2b"></a>`name`: `validate_result`
- <a id="s-da746ccde5"></a>`owner`: `review0_sampler_lib.SamplerConformanceResult`
- <a id="s-15efe06963"></a>`unit`: `member`

### Declared structure

- <a id="s-43933e2859"></a>`kind`: `"method"`
- <a id="s-f18494c7c0"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [SamplerConformanceResult](review0-sampler-lib-samplerconformanceresult.md)

## Governing policies

- <a id="pa-90bf604358"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SamplerConformanceResult.validate_result`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 16800426781c12a614fbb5b74cb10ac369e772ede1210beb6ec4e913ea8580d9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "validate_result",
  "owner": "review0_sampler_lib.SamplerConformanceResult",
  "unit": "member"
}
```

</details>
