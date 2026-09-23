# review0_sampler_client.ReviewSamplerClient.close

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-client:review0-sampler-client-reviewsamplerclient-close:a7e4e7edef -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f024cb3545"></a>
- <a id="s-99ff23b4a1"></a>`distribution`: `review0-sampler-client`
- <a id="s-bf2e841e6e"></a>`module`: `review0_sampler_client`
- <a id="s-82339e8c13"></a>`name`: `close`
- <a id="s-a45ab7939c"></a>`owner`: `review0_sampler_client.ReviewSamplerClient`
- <a id="s-1d2f79727d"></a>`unit`: `member`

### Declared structure

- <a id="s-309a68261f"></a>`kind`: `"method"`
- <a id="s-ae9fad956a"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ReviewSamplerClient](review0-sampler-client-reviewsamplerclient.md)

## Governing policies

- <a id="pa-5df3851264"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-client:review0_sampler_client](../../../evidence/sources/authorities.md#src-eb22a957aa) — [some-implementations/stove0/review0/sampler/client/src/review0\_sampler\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/client/src/review0_sampler_client/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_client.ReviewSamplerClient.close`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8f12d5c19c54b17afe517c4bfcade1699d8d0793003b2ddcfc363e742f2cb5a6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "review0-sampler-client",
  "module": "review0_sampler_client",
  "name": "close",
  "owner": "review0_sampler_client.ReviewSamplerClient",
  "unit": "member"
}
```

</details>
