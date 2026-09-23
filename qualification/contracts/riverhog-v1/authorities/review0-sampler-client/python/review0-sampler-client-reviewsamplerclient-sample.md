# review0_sampler_client.ReviewSamplerClient.sample

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-client:review0-sampler-client-reviewsamplerclient-sample:0eb7fbda47 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b2ee6cd607"></a>
- <a id="s-9a07d2e8e5"></a>`distribution`: `review0-sampler-client`
- <a id="s-05be9eda85"></a>`module`: `review0_sampler_client`
- <a id="s-af2b2e2d13"></a>`name`: `sample`
- <a id="s-2de81513a9"></a>`owner`: `review0_sampler_client.ReviewSamplerClient`
- <a id="s-50198bf55b"></a>`unit`: `member`

### Declared structure

- <a id="s-49c189f0ca"></a>`kind`: `"method"`
- <a id="s-c633129d7d"></a>`signature`: `"\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplerClient](review0-sampler-client-reviewsamplerclient.md)

## Governing policies

- <a id="pa-46a4ce0ba1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-client:review0_sampler_client](../../../evidence/sources/authorities.md#src-eb22a957aa) — [some-implementations/stove0/review0/sampler/client/src/review0\_sampler\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/client/src/review0_sampler_client/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_client.ReviewSamplerClient.sample`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a4d4c52dad06c76bc4f6da655cfadd89082750cc0885e4018d2db39d59539fbe -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, request: 'SamplerRequest') -> 'SamplerResult'\""
  },
  "distribution": "review0-sampler-client",
  "module": "review0_sampler_client",
  "name": "sample",
  "owner": "review0_sampler_client.ReviewSamplerClient",
  "unit": "member"
}
```

</details>
