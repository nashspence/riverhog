# review0_sampler_lib.SamplerHttpBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-samplerhttpbinding:fb85fc09a3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a19ff8e133"></a>
- <a id="s-7705e3842a"></a>`distribution`: `review0-sampler-lib`
- <a id="s-a4e79ba3b5"></a>`module`: `review0_sampler_lib`
- <a id="s-4da51ba3c4"></a>`name`: `SamplerHttpBinding`
- <a id="s-05a1c50dbf"></a>`unit`: `export`

### Declared structure

- <a id="s-351e6b26a0"></a>`kind`: `"class"`
- <a id="s-4c2a58b147"></a>`signature`: `"\"(sampler: 'ReviewSampler', *, maximum_request_bytes: 'int' = 4194304, maximum_concurrency: 'int' = 1) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [handle](review0-sampler-lib-samplerhttpbinding-handle.md)

## Governing policies

- <a id="pa-49d89d808b"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SamplerHttpBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cab8a0464bb3e30f81f59943d674e00630aa47727fa8fd5da5a4af756f4a5b88 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(sampler: 'ReviewSampler', *, maximum_request_bytes: 'int' = 4194304, maximum_concurrency: 'int' = 1) -> 'None'\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "SamplerHttpBinding",
  "unit": "export"
}
```

</details>
