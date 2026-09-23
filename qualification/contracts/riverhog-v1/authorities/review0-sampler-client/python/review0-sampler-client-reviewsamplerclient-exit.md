# review0_sampler_client.ReviewSamplerClient.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-client:review0-sampler-client-reviewsamplerclient-exit:2240a86dc5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d5118a4386"></a>
- <a id="s-2d65c8e366"></a>`distribution`: `review0-sampler-client`
- <a id="s-944aaf1aa3"></a>`module`: `review0_sampler_client`
- <a id="s-bcc6461a39"></a>`name`: `__exit__`
- <a id="s-6a4ce3f471"></a>`owner`: `review0_sampler_client.ReviewSamplerClient`
- <a id="s-b77bd41961"></a>`unit`: `member`

### Declared structure

- <a id="s-f7e9d400a6"></a>`kind`: `"method"`
- <a id="s-535ea704e6"></a>`signature`: `"\"(self, *_args: 'object') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplerClient](review0-sampler-client-reviewsamplerclient.md)

## Governing policies

- <a id="pa-3ba55bcfb1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-client:review0_sampler_client](../../../evidence/sources/authorities.md#src-eb22a957aa) — [some-implementations/stove0/review0/sampler/client/src/review0\_sampler\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/client/src/review0_sampler_client/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_client.ReviewSamplerClient.__exit__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f07905e3b09e4fdf6ac4a755d3b1272791e09ba76694b67a532ed23d0f6aec0d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *_args: 'object') -> 'None'\""
  },
  "distribution": "review0-sampler-client",
  "module": "review0_sampler_client",
  "name": "__exit__",
  "owner": "review0_sampler_client.ReviewSamplerClient",
  "unit": "member"
}
```

</details>
