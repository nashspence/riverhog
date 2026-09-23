# review0_sampler_client.ReviewSamplerClient.__enter__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-client:review0-sampler-client-reviewsamplerclient-enter:9aab963987 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a816d463d4"></a>
- <a id="s-f317503373"></a>`distribution`: `review0-sampler-client`
- <a id="s-9c92cd3334"></a>`module`: `review0_sampler_client`
- <a id="s-0bc941caa7"></a>`name`: `__enter__`
- <a id="s-bf26c29185"></a>`owner`: `review0_sampler_client.ReviewSamplerClient`
- <a id="s-7750c818b3"></a>`unit`: `member`

### Declared structure

- <a id="s-d0ecad287c"></a>`kind`: `"method"`
- <a id="s-3e865b26fc"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplerClient](review0-sampler-client-reviewsamplerclient.md)

## Governing policies

- <a id="pa-2d06561394"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-client:review0_sampler_client](../../../evidence/sources/authorities.md#src-eb22a957aa) — [some-implementations/stove0/review0/sampler/client/src/review0\_sampler\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/client/src/review0_sampler_client/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_client.ReviewSamplerClient.__enter__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 362bfaf5781274bac08520df2a54b32f23f4d012521762452464c9a18a69d073 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "review0-sampler-client",
  "module": "review0_sampler_client",
  "name": "__enter__",
  "owner": "review0_sampler_client.ReviewSamplerClient",
  "unit": "member"
}
```

</details>
