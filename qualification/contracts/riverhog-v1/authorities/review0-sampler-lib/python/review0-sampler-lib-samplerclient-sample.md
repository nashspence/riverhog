# review0_sampler_lib.SamplerClient.sample

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-samplerclient-sample:0b7ed8fa15 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f92fed1495"></a>
- <a id="s-55806e3467"></a>`distribution`: `review0-sampler-lib`
- <a id="s-77977bdfac"></a>`module`: `review0_sampler_lib`
- <a id="s-20f9fffb84"></a>`name`: `sample`
- <a id="s-7b2619daa8"></a>`owner`: `review0_sampler_lib.SamplerClient`
- <a id="s-e12b048f07"></a>`unit`: `member`

### Declared structure

- <a id="s-f876342955"></a>`kind`: `"method"`
- <a id="s-ff0e63b5a8"></a>`signature`: `"\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""`

## Maintained corroboration

### Related interface records

- [SamplerClient](review0-sampler-lib-samplerclient.md)

## Governing policies

- <a id="pa-bb97ce18a3"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SamplerClient.sample`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 97e57157f0b67df261475d6ad1bd9de560afe469e1d28031725653e8a2640e13 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "sample",
  "owner": "review0_sampler_lib.SamplerClient",
  "unit": "member"
}
```

</details>
