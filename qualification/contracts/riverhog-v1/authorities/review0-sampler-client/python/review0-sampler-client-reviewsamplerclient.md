# review0_sampler_client.ReviewSamplerClient

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-client:review0-sampler-client-reviewsamplerclient:9c051cea32 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-client](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bd338b6a69"></a>
- <a id="s-d390cb85a0"></a>`distribution`: `review0-sampler-client`
- <a id="s-5f9114af1d"></a>`module`: `review0_sampler_client`
- <a id="s-ae4ba3e428"></a>`name`: `ReviewSamplerClient`
- <a id="s-ce6b5b18a9"></a>`unit`: `export`

### Declared structure

- <a id="s-b9e7c15b45"></a>`kind`: `"class"`
- <a id="s-722b6dee3e"></a>`signature`: `"\"(base_url: 'str', token: 'str', *, allow_insecure_http: 'bool' = False, timeout_seconds: 'float' = 86400, transport: 'httpx.BaseTransport \| None' = None) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [descriptor](review0-sampler-client-reviewsamplerclient-descriptor.md)
- [close](review0-sampler-client-reviewsamplerclient-close.md)
- [__enter__](review0-sampler-client-reviewsamplerclient-enter.md)
- [__exit__](review0-sampler-client-reviewsamplerclient-exit.md)
- [sample](review0-sampler-client-reviewsamplerclient-sample.md)

## Governing policies

- <a id="pa-47a1aa3818"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-client:review0_sampler_client](../../../evidence/sources/authorities.md#src-eb22a957aa) — [some-implementations/stove0/review0/sampler/client/src/review0\_sampler\_client/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/client/src/review0_sampler_client/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_client.ReviewSamplerClient`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5901f4f5dbc2acfeaf5dddc7b06a1b3397227cbc016be848aa9b1d52057eb42a -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(base_url: 'str', token: 'str', *, allow_insecure_http: 'bool' = False, timeout_seconds: 'float' = 86400, transport: 'httpx.BaseTransport | None' = None) -> 'None'\""
  },
  "distribution": "review0-sampler-client",
  "module": "review0_sampler_client",
  "name": "ReviewSamplerClient",
  "unit": "export"
}
```

</details>
