# review0_sampler_protocol.SamplerResultPayload.state_shape

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:review0-sampler-protocol:review0-sampler-protocol-samplerresultpay-a778d42498:cc57c4a87a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [review0-sampler-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-acafc091e6"></a>
- <a id="s-fcab49b217"></a>`distribution`: `review0-sampler-protocol`
- <a id="s-5e31028de4"></a>`module`: `review0_sampler_protocol`
- <a id="s-1624baa0d4"></a>`name`: `state_shape`
- <a id="s-08355ae15f"></a>`owner`: `review0_sampler_protocol.SamplerResultPayload`
- <a id="s-2bf4bc86e5"></a>`unit`: `member`

### Declared structure

- <a id="s-01e30df9b1"></a>`kind`: `"method"`
- <a id="s-1a05ce5d85"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [SamplerResultPayload](review0-sampler-protocol-samplerresultpayload.md)

## Governing policies

- <a id="pa-82dff88e03"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:review0-sampler-protocol:review0_sampler_protocol](../../../evidence/sources/authorities.md#src-317a05ab5e) — [some-implementations/stove0/review0/sampler/protocol/src/review0\_sampler\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/review0/sampler/protocol/src/review0_sampler_protocol/__init__.py)

### Machine authority

- `/external_contract/python/review0_sampler_protocol.SamplerResultPayload.state_shape`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c0683b688bc53cd21a63583e4b2dfeb9fed9ea1016e5c9743e60dacdeadbf7b4 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "review0-sampler-protocol",
  "module": "review0_sampler_protocol",
  "name": "state_shape",
  "owner": "review0_sampler_protocol.SamplerResultPayload",
  "unit": "member"
}
```

</details>
