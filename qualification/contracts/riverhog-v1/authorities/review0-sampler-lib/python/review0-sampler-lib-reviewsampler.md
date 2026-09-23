# review0_sampler_lib.ReviewSampler

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-reviewsampler:4bf5de2a81 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b84d9d5c25"></a>
- <a id="s-f117c5d635"></a>`distribution`: `review0-sampler-lib`
- <a id="s-a5c12076d8"></a>`module`: `review0_sampler_lib`
- <a id="s-5522e5b386"></a>`name`: `ReviewSampler`
- <a id="s-91b1ae571f"></a>`unit`: `export`

### Declared structure

- <a id="s-c43540ecb1"></a>`kind`: `"class"`
- <a id="s-251acc4285"></a>`signature`: `"'(*args, **kwargs)'"`

## Maintained corroboration

### Related interface records

- [descriptor](review0-sampler-lib-reviewsampler-descriptor.md)
- [sample](review0-sampler-lib-reviewsampler-sample.md)

## Governing policies

- <a id="pa-1e0617bea6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.ReviewSampler`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ff6344a3b79f5ffe304922380850f1eea8d8a0aac33d55348d590907bcba9557 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "ReviewSampler",
  "unit": "export"
}
```

</details>
