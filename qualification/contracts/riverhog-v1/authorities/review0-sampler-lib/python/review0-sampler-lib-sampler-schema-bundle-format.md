# review0_sampler_lib.SAMPLER_SCHEMA_BUNDLE_FORMAT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-sampler-schema-bundle-format:3894e5db51 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-29db89addd"></a>
- <a id="s-a9a5dd9791"></a>`distribution`: `review0-sampler-lib`
- <a id="s-961a96c210"></a>`module`: `review0_sampler_lib`
- <a id="s-e39833228b"></a>`name`: `SAMPLER_SCHEMA_BUNDLE_FORMAT`
- <a id="s-1a0c67addd"></a>`unit`: `export`

### Declared structure

- <a id="s-98ed04b06a"></a>`kind`: `"constant"`
- <a id="s-9cd71c881d"></a>`value`: `"review0-sampler-schema-bundle/v1"`

## Governing policies

- <a id="pa-3d19dc66e9"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SAMPLER_SCHEMA_BUNDLE_FORMAT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: dd1b1d97e13fc7329ea7ba7027b003b150d1077c2a8d6b1765b6885f5f4ae791 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "review0-sampler-schema-bundle/v1"
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "SAMPLER_SCHEMA_BUNDLE_FORMAT",
  "unit": "export"
}
```

</details>
