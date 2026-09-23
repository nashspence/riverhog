# review0_sampler_lib.SamplerClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-samplerclient:6faf6fbd75 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5b99c39d29"></a>
- <a id="s-385138bfc5"></a>`distribution`: `review0-sampler-lib`
- <a id="s-f472138418"></a>`module`: `review0_sampler_lib`
- <a id="s-7987a24ba2"></a>`name`: `SamplerClient`
- <a id="s-2a17b39ce1"></a>`unit`: `export`

### Declared structure

- <a id="s-2ff3fe1e2e"></a>`kind`: `"class"`
- <a id="s-d32d515827"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [descriptor](review0-sampler-lib-samplerclient-descriptor.md)
- [sample](review0-sampler-lib-samplerclient-sample.md)

## Governing policies

- <a id="pa-a4d1efd206"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SamplerClient`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c13f981473ef37d2eb629d102c5b9e021257a0148d5cc764b758fdb65c5bbc12 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "SamplerClient",
  "unit": "export"
}
```

</details>
