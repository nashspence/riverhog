# review0_sampler_lib.SamplerWorkspace.canceled

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-samplerworkspace-canceled:e7611365ec -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b579b5979d"></a>
- <a id="s-3f0c41c6fa"></a>`distribution`: `review0-sampler-lib`
- <a id="s-72152cfa32"></a>`module`: `review0_sampler_lib`
- <a id="s-af6d40e636"></a>`name`: `canceled`
- <a id="s-ca6a7dc063"></a>`owner`: `review0_sampler_lib.SamplerWorkspace`
- <a id="s-004e46c945"></a>`unit`: `member`

### Declared structure

- <a id="s-830d32ed44"></a>`kind`: `"method"`
- <a id="s-4e2cd02c2a"></a>`signature`: `"\"(self) -> 'bool'\""`

## Maintained corroboration

### Related interface records

- [SamplerWorkspace](review0-sampler-lib-samplerworkspace.md)

## Governing policies

- <a id="pa-cadc893ca0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SamplerWorkspace.canceled`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4b5672e04a11e3035214253e9fb22dc3f181c53eec4e99829f92483ce1f74444 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bool'\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "canceled",
  "owner": "review0_sampler_lib.SamplerWorkspace",
  "unit": "member"
}
```

</details>
