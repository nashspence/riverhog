# review0_sampler_client.ReviewSamplerClient.descriptor

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-client:review0-sampler-client-reviewsamplerclien-e4a869ce88:8794a69f17 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-99ac7d62da"></a>
- <a id="s-0e23de3dd5"></a>`distribution`: `review0-sampler-client`
- <a id="s-a5384b6201"></a>`module`: `review0_sampler_client`
- <a id="s-23649015aa"></a>`name`: `descriptor`
- <a id="s-76445d1de8"></a>`owner`: `review0_sampler_client.ReviewSamplerClient`
- <a id="s-3e217ec9be"></a>`unit`: `member`

### Declared structure

- <a id="s-ccd60cd535"></a>`kind`: `"method"`
- <a id="s-2cc50e5f7e"></a>`signature`: `"\"(self, *, refresh: 'bool' = False) -> 'SamplerDescriptor'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplerClient](review0-sampler-client-reviewsamplerclient.md)

## Governing policies

- <a id="pa-bc3ffefce1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-client:review0_sampler_client](../../../evidence/sources/authorities.md#src-eb22a957aa) — [some-implementations/stove0/review0/sampler/client/src/review0\_sampler\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/client/src/review0_sampler_client/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_client.ReviewSamplerClient.descriptor`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2cd8e27b8519c1bfd3409e7761d2a9e0e4e5b61b75bc2e7c4d23f5bcdc946f70 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, refresh: 'bool' = False) -> 'SamplerDescriptor'\""
  },
  "distribution": "review0-sampler-client",
  "module": "review0_sampler_client",
  "name": "descriptor",
  "owner": "review0_sampler_client.ReviewSamplerClient",
  "unit": "member"
}
```

</details>
