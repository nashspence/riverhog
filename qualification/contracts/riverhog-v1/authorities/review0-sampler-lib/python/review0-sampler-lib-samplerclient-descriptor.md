# review0_sampler_lib.SamplerClient.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-lib:review0-sampler-lib-samplerclient-descriptor:f7efb73396 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b09869e739"></a>
- <a id="s-9709288b41"></a>`distribution`: `review0-sampler-lib`
- <a id="s-0ec6fc276a"></a>`module`: `review0_sampler_lib`
- <a id="s-64bcac8911"></a>`name`: `descriptor`
- <a id="s-6b58b5e455"></a>`owner`: `review0_sampler_lib.SamplerClient`
- <a id="s-4b1ed7e7c9"></a>`unit`: `member`

### Declared structure

- <a id="s-8237fc1abb"></a>`kind`: `"method"`
- <a id="s-65b11f5f7b"></a>`signature`: `"\"(self, *, refresh: 'bool' = False) -> 'SamplerDescriptor'\""`

## Maintained corroboration

### Related interface records

- [SamplerClient](review0-sampler-lib-samplerclient.md)

## Governing policies

- <a id="pa-7d0c174c86"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-lib:review0_sampler_lib](../../../evidence/sources/authorities.md#src-d1d6c03a07) — [some-implementations/stove0/review0/sampler/support/src/review0\_sampler\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/support/src/review0_sampler_lib/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_lib.SamplerClient.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 001eaba60ab60ea49703e1732a1aa2e4aeeaac1c8a59519e2b11d87cb2f92a14 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, refresh: 'bool' = False) -> 'SamplerDescriptor'\""
  },
  "distribution": "review0-sampler-lib",
  "module": "review0_sampler_lib",
  "name": "descriptor",
  "owner": "review0_sampler_lib.SamplerClient",
  "unit": "member"
}
```

</details>
